pub mod auth;
pub mod chat_completions;
pub mod client;
pub mod compaction;
pub mod continuation;
pub mod count_tokens;
pub(crate) mod events;
pub mod images;
pub mod inline_compaction;
pub mod native;
pub mod request_summary;
pub mod search;
pub mod transcription;
pub mod translate;
mod upstream_capacity;
pub mod websocket;

use async_trait::async_trait;
use axum::Json;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http::StatusCode;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{OwnedMutexGuard, OwnedSemaphorePermit};

use crate::anthropic::error::json_error;
use crate::anthropic::schema::{CountTokensResponse, MessagesRequest};
use crate::anthropic::sse::parse_sse_events;
use crate::config;
use crate::logging::create_logger;
use crate::monitor::usage_from_anthropic_sse;
use crate::provider::{CliHandlers, Provider, RequestContext};
use crate::registry;
use crate::request_identity::ConversationIdentity;
use crate::retry::{compute_backoff_delay, sleep};

use self::auth::browser_login::run_browser_login;
use self::auth::device::DeviceAuthClient;
use self::auth::manager::CodexAuthManager;
use self::auth::token_store::file_store;
use self::client::CodexHttpClient;
use self::compaction::{
    CompactionAttempt, abort_compaction_attempt, activate_compaction, apply_compaction_replay,
    begin_compaction, request_compaction, store_compaction,
};
use self::continuation::{
    ContinuationReservation, abort_continuation_for_owner, continuation_candidate_for_owner,
    record_continuation_for_owner,
};
use self::count_tokens::count_translated_tokens;
use self::translate::accumulate::accumulate_response_with_traffic;
use self::translate::live_stream::LiveStreamTranslator;
use self::translate::model_allowlist::{
    CODEX_AUTO_REVIEW_MODEL, assert_allowed_model, full_lane_web_search_model,
    resolve_model_request_with_config_override, uses_responses_lite,
};
use self::translate::reducer::finish_metadata_from_upstream;
use self::translate::request::{
    TranslateOptions, has_hosted_web_search, is_compact_messages_request, translate_request,
};

const MAX_RETRYABLE_LIVE_STREAM_RETRIES: u32 = 10;
const MAX_EMPTY_COMPLETION_RETRIES: u32 = 10;
const EMPTY_CODEX_COMPLETION_DETAIL: &str = "empty_codex_completion";
const LIVE_STREAM_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const STATE_DURABILITY_INDETERMINATE_CODE: &str = "CLAUDEX_STATE_DURABILITY_INDETERMINATE";
const STATE_DURABILITY_INDETERMINATE_MESSAGE: &str =
    "The sidecar namespace was replaced, but parent-directory durability could not be confirmed.";
use self::translate::stream::translate_stream_bytes_with_traffic;
use self::upstream_capacity::{
    UpstreamCapacityError, UpstreamCapacityGates, UpstreamCapacityPermit, UpstreamClass,
};

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

pub(crate) fn clear_session_compaction(session_id: &str) {
    compaction::clear_compaction(session_id);
}

pub struct CodexProvider {
    client: Arc<CodexHttpClient>,
    inline_locks: inline_compaction::LaneLocks,
    session_gates: inline_compaction::SessionConcurrencyGates,
    inline_recovery: Arc<inline_compaction::StateRecoveryRegistry>,
    upstream_capacity: Option<UpstreamCapacityGates>,
    upstream_capacity_error: Option<UpstreamCapacityError>,
}

impl Default for CodexProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexProvider {
    pub fn new() -> Self {
        let (upstream_capacity, upstream_capacity_error) =
            match UpstreamCapacityGates::from_environment() {
                Ok(gates) => (gates, None),
                Err(error) => (None, Some(error)),
            };
        Self {
            client: Arc::new(CodexHttpClient::new()),
            inline_locks: inline_compaction::LaneLocks::default(),
            session_gates: inline_compaction::SessionConcurrencyGates::default(),
            inline_recovery: Arc::new(inline_compaction::StateRecoveryRegistry::default()),
            upstream_capacity,
            upstream_capacity_error,
        }
    }

    async fn acquire_upstream_capacity(
        &self,
        class: UpstreamClass,
    ) -> Result<Option<UpstreamCapacityPermit>, UpstreamCapacityError> {
        if let Some(error) = self.upstream_capacity_error.as_ref() {
            return Err(error.clone());
        }
        match self.upstream_capacity.as_ref() {
            Some(gates) => gates.acquire(class).await.map(Some),
            None => Ok(None),
        }
    }
}

impl CodexProvider {
    async fn handle_messages_inner(
        &self,
        body: MessagesRequest,
        ctx: RequestContext,
        conversation_identity: Option<ConversationIdentity>,
    ) -> Response {
        let message_id = format!("msg_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
        let want_stream = body.stream;
        let model = body.model.as_deref().unwrap_or("gpt-5.6-sol");
        let request_session_hash = ctx
            .session_id
            .as_deref()
            .map(|session_id| encode_sha256(session_id.as_bytes()));

        let mut resolved =
            resolve_model_request_with_config_override(model, !body.bypass_provider_model_override);
        if let Err(e) = assert_allowed_model(&resolved.model) {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Model \"{model}\" resolves to unsupported model \"{}\"",
                    e.model
                ),
            );
        }
        if search::is_standalone_search_request(&body) {
            if let Some(monitor) = ctx.monitor.as_ref() {
                monitor.model_resolved(&ctx.req_id, &resolved.model);
            }
            let (search_request, query) = match search::build_search_request(
                &body,
                &resolved.model,
                ctx.session_id.as_deref(),
            ) {
                Ok(request) => request,
                Err(error) => {
                    return json_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_request_error",
                        error.to_string(),
                    );
                }
            };
            let _upstream_capacity_permit =
                match self.acquire_upstream_capacity(UpstreamClass::Data).await {
                    Ok(permit) => permit,
                    Err(error) => {
                        log_upstream_capacity_error(&ctx.req_id, UpstreamClass::Data, &error);
                        return map_upstream_capacity_error_to_response(&error);
                    }
                };
            let log = create_logger("codex");
            let started_at = Instant::now();
            log.info(
                "codex_standalone_search_started",
                Some(serde_json::Map::from_iter([
                    ("reqId".to_string(), serde_json::json!(&ctx.req_id)),
                    ("model".to_string(), serde_json::json!(&resolved.model)),
                    ("stream".to_string(), serde_json::json!(want_stream)),
                ])),
            );
            if let Some(monitor) = ctx.monitor.as_ref() {
                monitor.upstream_started(&ctx.req_id);
            }
            let search_response = match self.client.post_search(&search_request, &ctx).await {
                Ok(response) => response,
                Err(error) => {
                    log.warn(
                        "codex_standalone_search_failed",
                        Some(serde_json::Map::from_iter([
                            ("reqId".to_string(), serde_json::json!(&ctx.req_id)),
                            ("model".to_string(), serde_json::json!(&resolved.model)),
                            ("status".to_string(), serde_json::json!(error.status)),
                            (
                                "ms".to_string(),
                                serde_json::json!(started_at.elapsed().as_millis()),
                            ),
                        ])),
                    );
                    return map_codex_error_to_response(&error);
                }
            };
            log.info(
                "codex_standalone_search_completed",
                Some(serde_json::Map::from_iter([
                    ("reqId".to_string(), serde_json::json!(&ctx.req_id)),
                    ("model".to_string(), serde_json::json!(&resolved.model)),
                    (
                        "resultCount".to_string(),
                        serde_json::json!(search_response.results.as_ref().map(Vec::len)),
                    ),
                    (
                        "ms".to_string(),
                        serde_json::json!(started_at.elapsed().as_millis()),
                    ),
                ])),
            );
            let input_tokens = search::search_request_input_tokens(&search_request);
            let output_tokens = search::search_response_output_tokens(&search_response);
            if let Some(monitor) = ctx.monitor.as_ref() {
                monitor.usage_updated(&ctx.req_id, Some(input_tokens), Some(output_tokens));
            }
            return search::anthropic_search_response(
                &search_response,
                &query,
                &message_id,
                model,
                want_stream,
                input_tokens,
                ctx.traffic.as_deref(),
            );
        }
        let inline_config = if resolved.model == "codex-auto-review" {
            None
        } else {
            match inline_compaction::InlineCompactionConfig::from_environment() {
                Ok(config) => config,
                Err(error) => {
                    log_inline_compaction_error(
                        Some(&ctx.req_id),
                        request_session_hash.as_deref(),
                        None,
                        &error,
                        self.inline_recovery.backend_degraded(),
                    );
                    return map_inline_compaction_error_to_response(&error);
                }
            }
        };
        let session_concurrency_config = if resolved.model == "codex-auto-review" {
            None
        } else {
            match inline_compaction::SessionConcurrencyConfig::from_environment() {
                Ok(config) => config,
                Err(error) => {
                    log_inline_compaction_error(
                        Some(&ctx.req_id),
                        request_session_hash.as_deref(),
                        None,
                        &error,
                        self.inline_recovery.backend_degraded(),
                    );
                    return map_inline_compaction_error_to_response(&error);
                }
            }
        };
        // Native `context_management` is a full Responses capability. The
        // Responses Lite lane rejects the otherwise valid request when inline
        // compaction is present. Keep the resolved model unchanged and move
        // only inline-enabled requests onto the full lane.
        let use_responses_lite = if inline_config.is_some() {
            false
        } else {
            apply_model_lane_for_request(&mut resolved.model, &body)
        };
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.model_resolved(&ctx.req_id, &resolved.model);
        }

        let mut translated = match translate_request(
            &body,
            TranslateOptions {
                session_id: ctx.session_id.clone(),
                service_tier: resolved.service_tier.clone(),
                model: resolved.model.clone(),
                use_responses_lite,
            },
        ) {
            Ok(t) => t,
            Err(e) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    e.to_string(),
                );
            }
        };

        let compact_boundary = is_compact_messages_request(&body);
        let server_compaction_enabled = config::codex_server_compaction();
        let mut compaction_attempt = None;
        let mut _inline_lane_guard = None;
        let mut _session_concurrency_permit = None;
        let mut inline_prepared = None;
        if inline_config.is_some() || session_concurrency_config.is_some() {
            let Some(lane_id) =
                inline_compaction::state_lane_id_from_identity(conversation_identity.as_ref())
            else {
                let error = inline_compaction::InlineCompactionError::Config(
                    "a stable Claude Code session lane is required",
                );
                log_inline_compaction_error(
                    Some(&ctx.req_id),
                    request_session_hash.as_deref(),
                    None,
                    &error,
                    self.inline_recovery.backend_degraded(),
                );
                return map_inline_compaction_error_to_response(&error);
            };
            let lane_hash = inline_compaction::telemetry_lane_hash(&lane_id);
            if inline_config.is_some() {
                let lane_guard = match self.inline_locks.lock(&lane_id).await {
                    Ok(guard) => guard,
                    Err(error) => {
                        log_inline_compaction_error(
                            Some(&ctx.req_id),
                            request_session_hash.as_deref(),
                            Some(&lane_hash),
                            &error,
                            self.inline_recovery.backend_degraded(),
                        );
                        return map_inline_compaction_error_to_response(&error);
                    }
                };
                _inline_lane_guard = Some(lane_guard);
            }
            if let Some(session_concurrency_config) = session_concurrency_config.as_ref() {
                let session_id = match conversation_identity.as_ref() {
                    Some(ConversationIdentity::Main(session_id))
                    | Some(ConversationIdentity::Agent(session_id, _)) => session_id,
                    None => unreachable!("stable identity was required above"),
                };
                let permit = match self
                    .session_gates
                    .acquire(session_id, session_concurrency_config)
                    .await
                {
                    Ok(permit) => permit,
                    Err(error) => {
                        log_inline_compaction_error(
                            Some(&ctx.req_id),
                            request_session_hash.as_deref(),
                            Some(&lane_hash),
                            &error,
                            self.inline_recovery.backend_degraded(),
                        );
                        return map_inline_compaction_error_to_response(&error);
                    }
                };
                _session_concurrency_permit = Some(permit);
            }
            if let Some(inline_config) = inline_config.as_ref() {
                let recovery_required = self.inline_recovery.recovery_required(&lane_hash);
                match inline_compaction::prepare_request_with_recovery(
                    inline_config,
                    &lane_id,
                    &mut translated,
                    compact_boundary,
                    recovery_required,
                ) {
                    Ok(prepared) => inline_prepared = Some(prepared),
                    Err(error) => {
                        self.inline_recovery.mark_from_error(&lane_hash, &error);
                        log_inline_compaction_error(
                            Some(&ctx.req_id),
                            request_session_hash.as_deref(),
                            Some(&lane_hash),
                            &error,
                            self.inline_recovery.backend_degraded(),
                        );
                        return map_inline_compaction_error_to_response(&error);
                    }
                }
            }
        }
        let upstream_class = if resolved.model == CODEX_AUTO_REVIEW_MODEL {
            UpstreamClass::Control
        } else {
            UpstreamClass::Data
        };
        let mut _upstream_capacity_permit =
            match self.acquire_upstream_capacity(upstream_class).await {
                Ok(permit) => permit,
                Err(error) => {
                    log_upstream_capacity_error(&ctx.req_id, upstream_class, &error);
                    return map_upstream_capacity_error_to_response(&error);
                }
            };
        if !server_compaction_enabled && let Some(session_id) = ctx.session_id.as_deref() {
            compaction::clear_compaction(session_id);
        }
        if server_compaction_enabled
            && compact_boundary
            && let Some(session_id) = ctx.session_id.as_deref()
        {
            let attempt = begin_compaction(session_id, &translated.model);
            compaction_attempt = Some(attempt);
            log_compaction_event(
                "server_compaction_triggered",
                &ctx,
                translated.input.len(),
                None,
            );
            if let Some(monitor) = ctx.monitor.as_ref() {
                monitor.compaction_started(&ctx.req_id);
            }
            let mut compaction_ctx = ctx.clone();
            compaction_ctx.monitor = None;
            match request_compaction(self.client.as_ref(), &translated, &compaction_ctx).await {
                Ok(native_history) => {
                    if store_compaction(session_id, attempt, native_history) {
                        log_compaction_event(
                            "server_compaction_completed",
                            &ctx,
                            translated.input.len(),
                            None,
                        );
                    } else {
                        log_compaction_event(
                            "server_compaction_failed",
                            &ctx,
                            translated.input.len(),
                            Some("compaction state was superseded or exceeded the in-memory limit"),
                        );
                    }
                }
                Err(error) => {
                    abort_compaction_attempt(Some(session_id), Some(attempt));
                    log_compaction_event(
                        "server_compaction_failed",
                        &ctx,
                        translated.input.len(),
                        Some(&error.to_string()),
                    );
                }
            }
        } else if server_compaction_enabled
            && !compact_boundary
            && let Some(replay) = apply_compaction_replay(ctx.session_id.as_deref(), &translated)
        {
            translated = replay.request;
            compaction_attempt = Some(replay.attempt);
        }

        // Check continuation
        let previous_response_id_enabled = config::codex_previous_response_id();
        let continuation = continuation_candidate_for_owner(
            conversation_identity.as_ref(),
            &translated,
            previous_response_id_enabled,
        );
        let turn_id = continuation.turn_id();
        let configured_transport = config::codex_transport();
        let transport = configured_transport.as_str();
        let upstream_started_at = Instant::now();
        let log = create_logger("codex");
        let req_id = ctx.req_id.clone();
        log.info(
            "codex_upstream_request_started",
            Some(serde_json::Map::from_iter([
                ("reqId".to_string(), serde_json::json!(&req_id)),
                ("transport".to_string(), serde_json::json!(transport)),
                ("model".to_string(), serde_json::json!(&resolved.model)),
                ("stream".to_string(), serde_json::json!(want_stream)),
                (
                    "responsesLite".to_string(),
                    serde_json::json!(use_responses_lite),
                ),
                (
                    "previousResponseIdEnabled".to_string(),
                    serde_json::json!(previous_response_id_enabled),
                ),
                (
                    "hasPreviousResponseId".to_string(),
                    serde_json::json!(continuation.candidate().previous_response_id.is_some()),
                ),
                (
                    "inputDeltaCount".to_string(),
                    serde_json::json!(continuation.candidate().input_delta.as_ref().map(Vec::len)),
                ),
                ("turnId".to_string(), serde_json::json!(turn_id)),
            ])),
        );

        // Post to upstream with continuation
        let client = self.client.clone();
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.upstream_started(&ctx.req_id);
        }
        if want_stream {
            let stream_request = translated.clone();
            let resources = LiveStreamResources {
                lane_guard: _inline_lane_guard.take(),
                session_permit: _session_concurrency_permit.take(),
                upstream_capacity_permit: _upstream_capacity_permit.take(),
                inline_prepared: inline_prepared.take(),
                inline_recovery: self.inline_recovery.clone(),
            };
            let response = live_stream_response(
                client,
                message_id,
                model,
                ctx,
                stream_request,
                continuation,
                LiveStreamCompaction {
                    compact_boundary,
                    attempt: compaction_attempt,
                },
                resources,
                configured_transport,
            )
            .await;
            log.info(
                "codex_upstream_response_ready",
                Some(serde_json::Map::from_iter([
                    ("reqId".to_string(), serde_json::json!(&req_id)),
                    ("transport".to_string(), serde_json::json!(transport)),
                    (
                        "status".to_string(),
                        serde_json::json!(response.status().as_u16()),
                    ),
                    (
                        "ms".to_string(),
                        serde_json::json!(upstream_started_at.elapsed().as_millis()),
                    ),
                ])),
            );
            return response;
        }

        let request_continuation = continuation.clone();
        let mut continuation = Some(continuation);
        let mut attempt = 0_u32;
        let upstream = loop {
            let response = match client
                .post_codex_for_owner(&translated, &ctx, continuation.as_ref())
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    log.warn(
                        "codex_upstream_request_failed",
                        Some(serde_json::Map::from_iter([
                            ("reqId".to_string(), serde_json::json!(&req_id)),
                            ("transport".to_string(), serde_json::json!(transport)),
                            ("status".to_string(), serde_json::json!(e.status)),
                            (
                                "origin".to_string(),
                                serde_json::json!(format!("{:?}", e.origin)),
                            ),
                            ("error".to_string(), serde_json::json!(&e.message)),
                            (
                                "ms".to_string(),
                                serde_json::json!(upstream_started_at.elapsed().as_millis()),
                            ),
                        ])),
                    );
                    abort_compaction_attempt(ctx.session_id.as_deref(), compaction_attempt);
                    abort_continuation_for_owner(&request_continuation);
                    return map_codex_error_to_response(&e);
                }
            };
            if !is_empty_codex_success_completion(&response.body) {
                break response;
            }
            // A successful terminal event with no output would translate into
            // an empty end_turn; retry with full context instead.
            let error = empty_buffered_completion_error();
            drop_live_continuation_for_retry(&mut continuation);
            if attempt >= MAX_EMPTY_COMPLETION_RETRIES {
                abort_compaction_attempt(ctx.session_id.as_deref(), compaction_attempt);
                abort_continuation_for_owner(&request_continuation);
                return map_codex_error_to_response(&error);
            }
            let delay = compute_backoff_delay(attempt, None);
            if delay.exceeds_budget {
                abort_compaction_attempt(ctx.session_id.as_deref(), compaction_attempt);
                abort_continuation_for_owner(&request_continuation);
                return map_codex_error_to_response(&error);
            }
            attempt += 1;
            sleep(delay.wait_ms).await;
        };
        log.info(
            "codex_upstream_response_received",
            Some(serde_json::Map::from_iter([
                ("reqId".to_string(), serde_json::json!(&req_id)),
                ("transport".to_string(), serde_json::json!(transport)),
                ("status".to_string(), serde_json::json!(upstream.status)),
                (
                    "bodyBytes".to_string(),
                    serde_json::json!(upstream.body.len()),
                ),
                (
                    "ms".to_string(),
                    serde_json::json!(upstream_started_at.elapsed().as_millis()),
                ),
            ])),
        );

        if want_stream {
            let estimated_input_tokens = count_translated_tokens(&translated);
            let sse_bytes = match translate_stream_bytes_with_traffic(
                &upstream.body,
                &message_id,
                model,
                estimated_input_tokens,
                ctx.traffic.as_deref(),
            ) {
                Ok(b) => b,
                Err(e) => {
                    abort_compaction_attempt(ctx.session_id.as_deref(), compaction_attempt);
                    abort_continuation_for_owner(&request_continuation);
                    return map_codex_failure_to_response(&format!(
                        "Stream translation error: {e}"
                    ));
                }
            };
            if let Some(monitor) = ctx.monitor.as_ref() {
                let (input_tokens, output_tokens) = usage_from_anthropic_sse(&sse_bytes);
                monitor.stream_progress(
                    &ctx.req_id,
                    sse_bytes.len() as u64,
                    count_sse_events(&sse_bytes),
                    input_tokens,
                    output_tokens,
                );
            }
            if let Some(prepared) = inline_prepared.as_ref() {
                match commit_inline_compaction(prepared, &upstream.body) {
                    Ok(report) => self.inline_recovery.note_durable_commit(&report),
                    Err(error) => {
                        self.inline_recovery
                            .mark_from_error(prepared.lane_hash(), &error);
                        log_inline_compaction_error(
                            Some(&ctx.req_id),
                            request_session_hash.as_deref(),
                            Some(prepared.lane_hash()),
                            &error,
                            self.inline_recovery.backend_degraded(),
                        );
                        abort_compaction_attempt(ctx.session_id.as_deref(), compaction_attempt);
                        abort_continuation_for_owner(&request_continuation);
                        return map_inline_compaction_error_to_response(&error);
                    }
                }
            }
            update_continuation_from_upstream(
                ctx.session_id.as_deref(),
                &request_continuation,
                compaction_attempt,
                &translated,
                &upstream.body,
                upstream.socket_id,
                compact_boundary,
            );

            let headers = [
                (http::header::CONTENT_TYPE, "text/event-stream"),
                (http::header::CACHE_CONTROL, "no-cache"),
                (http::header::CONNECTION, "keep-alive"),
            ];
            (headers, sse_bytes).into_response()
        } else {
            match accumulate_response_with_traffic(
                &upstream.body,
                &message_id,
                model,
                ctx.traffic.as_deref(),
            ) {
                Ok(json) => {
                    if let Some(prepared) = inline_prepared.as_ref() {
                        match commit_inline_compaction(prepared, &upstream.body) {
                            Ok(report) => self.inline_recovery.note_durable_commit(&report),
                            Err(error) => {
                                self.inline_recovery
                                    .mark_from_error(prepared.lane_hash(), &error);
                                log_inline_compaction_error(
                                    Some(&ctx.req_id),
                                    request_session_hash.as_deref(),
                                    Some(prepared.lane_hash()),
                                    &error,
                                    self.inline_recovery.backend_degraded(),
                                );
                                abort_compaction_attempt(
                                    ctx.session_id.as_deref(),
                                    compaction_attempt,
                                );
                                abort_continuation_for_owner(&request_continuation);
                                return map_inline_compaction_error_to_response(&error);
                            }
                        }
                    }
                    if let Some(monitor) = ctx.monitor.as_ref() {
                        monitor.usage_updated(
                            &ctx.req_id,
                            json.pointer("/usage/input_tokens").and_then(|v| v.as_u64()),
                            json.pointer("/usage/output_tokens")
                                .and_then(|v| v.as_u64()),
                        );
                    }
                    update_continuation_from_upstream(
                        ctx.session_id.as_deref(),
                        &request_continuation,
                        compaction_attempt,
                        &translated,
                        &upstream.body,
                        upstream.socket_id,
                        compact_boundary,
                    );
                    (StatusCode::OK, Json(json)).into_response()
                }
                Err(e) => {
                    abort_compaction_attempt(ctx.session_id.as_deref(), compaction_attempt);
                    abort_continuation_for_owner(&request_continuation);
                    map_codex_failure_to_response(&format!("Accumulation error: {e}"))
                }
            }
        }
    }
}

#[async_trait]
impl Provider for CodexProvider {
    fn name(&self) -> &'static str {
        "codex"
    }

    fn supported_models(&self) -> Vec<String> {
        let mut models: Vec<String> = registry::CODEX_MODELS
            .iter()
            .map(|m| m.to_string())
            .collect();
        for m in registry::CODEX_MODELS {
            models.push(format!("{m}-fast"));
        }
        models.sort_unstable();
        models.dedup();
        models
    }

    fn cli(&self) -> &'static dyn CliHandlers {
        &CODEX_CLI
    }

    async fn handle_messages(&self, body: MessagesRequest, ctx: RequestContext) -> Response {
        self.handle_messages_inner(body, ctx, None).await
    }

    async fn handle_messages_with_conversation_identity(
        &self,
        body: MessagesRequest,
        ctx: RequestContext,
        conversation_identity: Option<ConversationIdentity>,
    ) -> Response {
        self.handle_messages_inner(body, ctx, conversation_identity)
            .await
    }

    async fn handle_count_tokens(&self, body: MessagesRequest, ctx: RequestContext) -> Response {
        let model = body.model.as_deref().unwrap_or("gpt-5.6-sol");
        let mut resolved =
            resolve_model_request_with_config_override(model, !body.bypass_provider_model_override);
        if let Err(e) = assert_allowed_model(&resolved.model) {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Model \"{model}\" resolves to unsupported model \"{}\"",
                    e.model
                ),
            );
        }
        let use_responses_lite = apply_model_lane_for_request(&mut resolved.model, &body);
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.model_resolved(&ctx.req_id, &resolved.model);
        }

        let translated = match translate_request(
            &body,
            TranslateOptions {
                session_id: None,
                service_tier: resolved.service_tier.clone(),
                model: resolved.model.clone(),
                use_responses_lite,
            },
        ) {
            Ok(t) => t,
            Err(e) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    e.to_string(),
                );
            }
        };

        let tokens = count_translated_tokens(&translated);
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.usage_updated(&ctx.req_id, Some(tokens), None);
        }
        (
            StatusCode::OK,
            Json(CountTokensResponse {
                input_tokens: tokens,
            }),
        )
            .into_response()
    }
}

/// Picks the upstream model and lane for a request. Hosted web_search must
/// run on the full Responses API (the lite lane rejects hosted tools), and
/// lite-only models like gpt-5.6-luna don't exist there, so such requests
/// are upgraded to a full-lane model. Returns whether to use the lite lane.
fn apply_model_lane_for_request(model: &mut String, body: &MessagesRequest) -> bool {
    if has_hosted_web_search(body) {
        *model = full_lane_web_search_model(model).to_string();
        return false;
    }
    uses_responses_lite(model)
}

fn count_sse_events(bytes: &[u8]) -> u64 {
    String::from_utf8_lossy(bytes).matches("event:").count() as u64
}

fn log_compaction_event(
    event: &str,
    ctx: &RequestContext,
    input_items: usize,
    error: Option<&str>,
) {
    let mut fields = serde_json::Map::new();
    fields.insert("reqId".into(), serde_json::json!(ctx.req_id));
    fields.insert("inputItems".into(), serde_json::json!(input_items));
    if let Some(error) = error {
        fields.insert("error".into(), serde_json::json!(error));
        create_logger("codex").warn(event, Some(fields));
    } else {
        create_logger("codex").info(event, Some(fields));
    }
}

fn abort_request_state(
    session_id: Option<&str>,
    continuation: &ContinuationReservation,
    compaction_attempt: Option<CompactionAttempt>,
) {
    abort_compaction_attempt(session_id, compaction_attempt);
    abort_continuation_for_owner(continuation);
}

fn encode_sha256(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[derive(Default)]
struct LiveStreamResources {
    lane_guard: Option<OwnedMutexGuard<()>>,
    session_permit: Option<OwnedSemaphorePermit>,
    upstream_capacity_permit: Option<UpstreamCapacityPermit>,
    inline_prepared: Option<inline_compaction::PreparedInlineCompaction>,
    inline_recovery: Arc<inline_compaction::StateRecoveryRegistry>,
}

struct LiveRequestLease {
    // These guards deliberately have no early-release path. They leave scope
    // only after the producer has finalized or aborted the logical request.
    _lane_guard: Option<OwnedMutexGuard<()>>,
    _session_permit: Option<OwnedSemaphorePermit>,
    _upstream_capacity_permit: Option<UpstreamCapacityPermit>,
    inline_prepared: Option<inline_compaction::PreparedInlineCompaction>,
    inline_recovery: Arc<inline_compaction::StateRecoveryRegistry>,
    continuation: ContinuationReservation,
    session_id: Option<String>,
    compaction: LiveStreamCompaction,
    logical_request_id: String,
    request_fingerprint: String,
    semantic_output_started: bool,
    terminal_seen: bool,
    terminal_delivered: bool,
    finalized: bool,
    committed: bool,
}

impl LiveRequestLease {
    fn new(
        resources: LiveStreamResources,
        continuation: ContinuationReservation,
        session_id: Option<String>,
        compaction: LiveStreamCompaction,
        logical_request_id: String,
        request_body: &translate::request::ResponsesRequest,
    ) -> Self {
        debug_assert!(
            resources.inline_prepared.is_none()
                || continuation.candidate().previous_response_id.is_none(),
            "inline compaction and previous_response_id must be mutually exclusive"
        );
        let request_fingerprint = serde_json::to_vec(request_body)
            .map(|body| encode_sha256(&body))
            .unwrap_or_else(|_| "unavailable".to_string());
        Self {
            _lane_guard: resources.lane_guard,
            _session_permit: resources.session_permit,
            _upstream_capacity_permit: resources.upstream_capacity_permit,
            inline_prepared: resources.inline_prepared,
            inline_recovery: resources.inline_recovery,
            continuation,
            session_id,
            compaction,
            logical_request_id,
            request_fingerprint,
            semantic_output_started: false,
            terminal_seen: false,
            terminal_delivered: false,
            finalized: false,
            committed: false,
        }
    }

    fn abort(&mut self) {
        if self.finalized {
            return;
        }
        abort_request_state(
            self.session_id.as_deref(),
            &self.continuation,
            self.compaction.attempt,
        );
        self.finalized = true;
    }

    fn log_lifecycle(&self, event: &str, warn: bool) {
        let fields = serde_json::Map::from_iter([
            (
                "logicalRequestId".to_string(),
                serde_json::json!(&self.logical_request_id),
            ),
            (
                "bodyFingerprint".to_string(),
                serde_json::json!(&self.request_fingerprint),
            ),
            (
                "semanticStarted".to_string(),
                serde_json::json!(self.semantic_output_started),
            ),
            (
                "terminalSeen".to_string(),
                serde_json::json!(self.terminal_seen),
            ),
            (
                "terminalDelivered".to_string(),
                serde_json::json!(self.terminal_delivered),
            ),
            ("committed".to_string(), serde_json::json!(self.committed)),
        ]);
        let log = create_logger("codex_live_transaction");
        if warn {
            log.warn(event, Some(fields));
        } else {
            log.info(event, Some(fields));
        }
    }

    fn note_retry(&self, attempt_id: u32, retry_owner: &str, started_at: Instant) {
        let mut fields = serde_json::Map::from_iter([
            (
                "logicalRequestId".to_string(),
                serde_json::json!(&self.logical_request_id),
            ),
            ("attemptId".to_string(), serde_json::json!(attempt_id)),
            (
                "bodyFingerprint".to_string(),
                serde_json::json!(&self.request_fingerprint),
            ),
            (
                "semanticStarted".to_string(),
                serde_json::json!(self.semantic_output_started),
            ),
            ("retryOwner".to_string(), serde_json::json!(retry_owner)),
        ]);
        fields.insert(
            "totalStartupMs".to_string(),
            serde_json::json!(started_at.elapsed().as_millis()),
        );
        create_logger("codex_live_transaction").info("retry", Some(fields));
    }
}

impl Drop for LiveRequestLease {
    fn drop(&mut self) {
        if !self.finalized {
            self.abort();
            self.log_lifecycle("lease_aborted", true);
        } else if self.committed && self.terminal_seen && !self.terminal_delivered {
            self.log_lifecycle("committed_but_terminal_undelivered", true);
        }
        self.log_lifecycle("lease_released", false);
    }
}

struct LiveRequestStateCleanup {
    lease: LiveRequestLease,
}

impl LiveRequestStateCleanup {
    fn new(lease: LiveRequestLease) -> Self {
        Self { lease }
    }

    fn abort(&mut self) {
        self.lease.abort();
    }

    fn mark_semantic_output_started(&mut self) {
        self.lease.semantic_output_started = true;
    }

    fn note_retry(&self, attempt_id: u32, retry_owner: &str, started_at: Instant) {
        self.lease.note_retry(attempt_id, retry_owner, started_at);
    }

    fn finalize(
        &mut self,
        request_body: &translate::request::ResponsesRequest,
        upstream_body: &[u8],
        socket_id: Option<u64>,
    ) -> Result<(), inline_compaction::InlineCompactionError> {
        self.lease.terminal_seen = true;
        self.lease.log_lifecycle("upstream_terminal_seen", false);
        let sidecar_committed = if let Some(prepared) = self.lease.inline_prepared.as_ref() {
            let lane_hash = prepared.lane_hash().to_string();
            let report = match commit_inline_compaction(prepared, upstream_body) {
                Ok(report) => {
                    self.lease.inline_recovery.note_durable_commit(&report);
                    report
                }
                Err(error) => {
                    self.lease
                        .inline_recovery
                        .mark_from_error(&lane_hash, &error);
                    let session_hash = self
                        .lease
                        .session_id
                        .as_deref()
                        .map(|session_id| encode_sha256(session_id.as_bytes()));
                    log_inline_compaction_error(
                        Some(&self.lease.logical_request_id),
                        session_hash.as_deref(),
                        Some(&lane_hash),
                        &error,
                        self.lease.inline_recovery.backend_degraded(),
                    );
                    self.lease.log_lifecycle("state_commit_failed", true);
                    return Err(error);
                }
            };
            matches!(
                report.outcome,
                inline_compaction::CommitOutcome::Created
                    | inline_compaction::CommitOutcome::Advanced
                    | inline_compaction::CommitOutcome::Replaced
            )
        } else {
            false
        };
        if sidecar_committed {
            self.lease.log_lifecycle("sidecar_committed", false);
        }
        update_continuation_from_upstream(
            self.lease.session_id.as_deref(),
            &self.lease.continuation,
            self.lease.compaction.attempt,
            request_body,
            upstream_body,
            socket_id,
            self.lease.compaction.compact_boundary,
        );
        self.lease
            .log_lifecycle("state_continuity_finalized", false);
        self.lease.committed = true;
        self.lease.finalized = true;
        self.lease.log_lifecycle("state_finalized", false);
        Ok(())
    }

    fn note_terminal_delivered(&mut self) {
        self.lease.terminal_delivered = true;
        self.lease.log_lifecycle("terminal_delivered", false);
    }
}

enum LiveStreamStart {
    Response(Response),
    Retry {
        cleanup: Box<LiveRequestStateCleanup>,
        error: client::CodexError,
        full_context_retry_attempted: bool,
    },
}

#[derive(Clone, Copy)]
struct LiveStreamCompaction {
    compact_boundary: bool,
    attempt: Option<CompactionAttempt>,
}

#[allow(clippy::too_many_arguments)]
async fn live_stream_response(
    client: Arc<CodexHttpClient>,
    message_id: String,
    model: &str,
    ctx: RequestContext,
    request_body: translate::request::ResponsesRequest,
    continuation: ContinuationReservation,
    compaction: LiveStreamCompaction,
    resources: LiveStreamResources,
    transport: config::CodexTransport,
) -> Response {
    let model = model.to_string();
    let request_continuation = continuation.clone();
    let lease = LiveRequestLease::new(
        resources,
        request_continuation.clone(),
        ctx.session_id.clone(),
        compaction,
        ctx.req_id.clone(),
        &request_body,
    );
    let mut cleanup = LiveRequestStateCleanup::new(lease);
    let startup_started_at = Instant::now();
    let mut attempt = 0_u32;
    let mut continuation = Some(continuation);

    loop {
        let upstream_events = match transport {
            config::CodexTransport::Http => {
                client
                    .stream_codex_http_events_for_owner(&request_body, &ctx)
                    .await
            }
            config::CodexTransport::WebSocket => {
                client
                    .stream_codex_websocket_events_for_owner(
                        &request_body,
                        &ctx,
                        continuation.as_ref(),
                    )
                    .await
            }
            config::CodexTransport::Auto => {
                client
                    .stream_codex_auto_events_for_owner(&request_body, &ctx, continuation.as_ref())
                    .await
            }
        };
        let upstream_events = match upstream_events {
            Ok(events) => events,
            Err(err) if err.origin == client::CodexErrorOrigin::Http => {
                cleanup.abort();
                return map_codex_error_to_response(&err);
            }
            Err(err) if retryable_live_start_codex_error(&err) => {
                let dropped = drop_live_continuation_for_retry(&mut continuation);
                if dropped && is_missing_previous_response_error(&err) {
                    attempt += 1;
                    cleanup.note_retry(attempt, "provider", startup_started_at);
                    continue;
                }
                if attempt >= MAX_RETRYABLE_LIVE_STREAM_RETRIES {
                    cleanup.abort();
                    return map_codex_error_to_response(&err);
                }
                let delay = compute_backoff_delay(attempt, err.retry_after.as_deref());
                if delay.exceeds_budget {
                    cleanup.abort();
                    return map_codex_error_to_response(&err);
                }
                attempt += 1;
                cleanup.note_retry(attempt, "provider", startup_started_at);
                sleep(delay.wait_ms).await;
                continue;
            }
            Err(err) => {
                cleanup.abort();
                return map_codex_error_to_response(&err);
            }
        };

        match live_stream_response_once(
            upstream_events,
            message_id.clone(),
            &model,
            ctx.clone(),
            request_body.clone(),
            cleanup,
        )
        .await
        {
            LiveStreamStart::Response(response) => return response,
            LiveStreamStart::Retry {
                cleanup: returned_cleanup,
                error,
                full_context_retry_attempted,
            } => {
                cleanup = *returned_cleanup;
                // The incremental HTTP reader performs its own bounded
                // pre-semantic retries so it can stop immediately when the
                // consumer disappears. Do not multiply that exhausted retry
                // loop by the provider-level WebSocket retry policy.
                if error.origin == client::CodexErrorOrigin::Http {
                    cleanup.abort();
                    return map_codex_error_to_response(&error);
                }
                let dropped = drop_live_continuation_for_retry(&mut continuation);
                if full_context_retry_attempted && client::is_continuation_retry_error(&error) {
                    cleanup.abort();
                    return map_codex_error_to_response(&error);
                }
                if dropped && is_missing_previous_response_error(&error) {
                    attempt += 1;
                    cleanup.note_retry(attempt, "provider", startup_started_at);
                    continue;
                }
                if attempt >= MAX_RETRYABLE_LIVE_STREAM_RETRIES {
                    cleanup.abort();
                    return map_codex_error_to_response(&error);
                }
                let delay = compute_backoff_delay(attempt, error.retry_after.as_deref());
                if delay.exceeds_budget {
                    cleanup.abort();
                    return map_codex_error_to_response(&error);
                }
                attempt += 1;
                cleanup.note_retry(attempt, "provider", startup_started_at);
                sleep(delay.wait_ms).await;
            }
        }
    }
}

fn provider_retry(
    upstream_events: &websocket::CodexWebSocketEventStream,
    error: client::CodexError,
    cleanup: LiveRequestStateCleanup,
) -> LiveStreamStart {
    let full_context_retry_attempted = upstream_events.used_full_context_retry();
    upstream_events.mark_provider_retry_handoff();
    LiveStreamStart::Retry {
        cleanup: Box::new(cleanup),
        error,
        full_context_retry_attempted,
    }
}

#[allow(clippy::too_many_arguments)]
async fn live_stream_response_once(
    mut upstream_events: websocket::CodexWebSocketEventStream,
    message_id: String,
    model: &str,
    ctx: RequestContext,
    request_body: translate::request::ResponsesRequest,
    mut cleanup: LiveRequestStateCleanup,
) -> LiveStreamStart {
    let estimated_input_tokens = count_translated_tokens(&request_body);
    let mut translator = LiveStreamTranslator::with_estimated_input_tokens(
        message_id,
        model.to_string(),
        estimated_input_tokens,
    );
    let mut upstream_sse_body = Vec::new();
    // Keep protocol framing private until real output makes a transparent retry unsafe.
    // Every branch that consumes pending_chunk returns, so it is never flushed twice.
    let mut pending_chunk = Vec::new();
    let mut deferred_terminal_chunk = Vec::new();
    let mut generation_started = false;

    while let Some(item) = upstream_events.recv().await {
        let payload = match item {
            Ok(payload) => payload,
            Err(err) => {
                if retryable_live_start_codex_error(&err) {
                    return provider_retry(&upstream_events, err, cleanup);
                }
                cleanup.abort();
                return LiveStreamStart::Response(map_codex_error_to_response(&err));
            }
        };
        if !generation_started && codex_generation_event(&payload) {
            if let Some(monitor) = ctx.monitor.as_ref() {
                monitor.generation_started(&ctx.req_id);
            }
            generation_started = true;
        }
        append_upstream_sse_payload(&mut upstream_sse_body, &payload);
        let (chunk, terminal, downstream_finished) =
            match translate_live_stream_payload(&mut translator, &payload, None) {
                Ok(result) => result,
                Err(message) => {
                    if retryable_live_start_payload(&payload, &message) {
                        let lower_message = message.to_ascii_lowercase();
                        let status = websocket::event_error_status(&payload).unwrap_or_else(|| {
                            let error = payload.get("error").or_else(|| {
                                payload.get("response").and_then(|value| value.get("error"))
                            });
                            let overloaded = error.is_some_and(|error| {
                                error.get("code").and_then(|value| value.as_str())
                                    == Some("overloaded_error")
                                    || error.get("type").and_then(|value| value.as_str())
                                        == Some("overloaded_error")
                            });
                            if payload.get("type").and_then(|value| value.as_str())
                                == Some("codex.rate_limits")
                                || lower_message.contains("rate limit")
                            {
                                429
                            } else if overloaded || lower_message.contains("overloaded") {
                                529
                            } else {
                                503
                            }
                        });
                        return provider_retry(
                            &upstream_events,
                            client::CodexError {
                                status,
                                message: message.clone(),
                                detail: Some(message),
                                retry_after: retry_after_from_live_payload(&payload),
                                origin: client::CodexErrorOrigin::WebSocket,
                            },
                            cleanup,
                        );
                    }
                    cleanup.abort();
                    return LiveStreamStart::Response(map_codex_failure_to_response(&message));
                }
            };
        if translator.has_semantic_output() {
            cleanup.mark_semantic_output_started();
        }
        if terminal
            && is_codex_success_terminal_event(&payload)
            && !translator.has_semantic_output()
        {
            return provider_retry(&upstream_events, empty_live_completion_error(), cleanup);
        }
        if terminal {
            deferred_terminal_chunk.extend_from_slice(&chunk);
            match cleanup.finalize(
                &request_body,
                &upstream_sse_body,
                upstream_events.socket_id(),
            ) {
                Ok(()) => pending_chunk.extend_from_slice(&deferred_terminal_chunk),
                Err(error) => {
                    cleanup.abort();
                    pending_chunk.extend_from_slice(&live_stream_commit_failure_chunk(&error));
                }
            }
            record_live_stream_downstream_capture(&ctx, &pending_chunk);
            record_live_stream_progress(&ctx, &pending_chunk);
            return LiveStreamStart::Response(single_live_stream_response(pending_chunk, cleanup));
        }
        if downstream_finished {
            // A compatibility repair can finish the Anthropic shape before
            // Codex emits response.completed. Hold that success terminator
            // until the upstream terminal has been validated and committed.
            deferred_terminal_chunk.extend_from_slice(&chunk);
        } else {
            pending_chunk.extend_from_slice(&chunk);
        }
        if translator.has_semantic_output() && !pending_chunk.is_empty() {
            record_live_stream_downstream_capture(&ctx, &pending_chunk);
            record_live_stream_progress(&ctx, &pending_chunk);
            return LiveStreamStart::Response(remaining_live_stream_response(
                upstream_events,
                translator,
                pending_chunk,
                deferred_terminal_chunk,
                ctx,
                request_body,
                upstream_sse_body,
                cleanup,
            ));
        }
    }

    provider_retry(
        &upstream_events,
        client::CodexError {
            status: 0,
            message: "WebSocket connection closed before terminal Codex response event".to_string(),
            detail: Some(websocket::WEBSOCKET_MISSING_TERMINAL_DETAIL.to_string()),
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocket,
        },
        cleanup,
    )
}

fn empty_live_completion_error() -> client::CodexError {
    client::CodexError {
        status: 503,
        message: "Codex completed without producing output".to_string(),
        detail: Some(EMPTY_CODEX_COMPLETION_DETAIL.to_string()),
        retry_after: None,
        origin: client::CodexErrorOrigin::WebSocket,
    }
}

fn codex_generation_event(payload: &serde_json::Value) -> bool {
    !matches!(
        payload.get("type").and_then(|value| value.as_str()),
        Some("codex.rate_limits" | "keepalive") | None
    )
}

fn translate_live_stream_payload(
    translator: &mut LiveStreamTranslator,
    payload: &serde_json::Value,
    traffic: Option<&crate::traffic::TrafficCapture>,
) -> Result<(Vec<u8>, bool, bool), String> {
    let chunk = translator.accept(payload, traffic)?;
    Ok((
        chunk,
        is_codex_terminal_event(payload),
        translator.is_finished(),
    ))
}

fn live_stream_failure_chunk(message: &str, error_type: &str) -> Vec<u8> {
    crate::anthropic::sse::encode_sse_event(
        Some("error"),
        &serde_json::json!({
            "type": "error",
            "error": {"type": error_type, "message": message}
        })
        .to_string(),
    )
}

fn live_stream_commit_failure_chunk(error: &inline_compaction::InlineCompactionError) -> Vec<u8> {
    if error.durability_details().is_some() {
        return crate::anthropic::sse::encode_sse_event(
            Some("error"),
            &serde_json::json!({
                "type": "error",
                "error": {
                    "type": "api_error",
                    "code": STATE_DURABILITY_INDETERMINATE_CODE,
                    "message": STATE_DURABILITY_INDETERMINATE_MESSAGE,
                    "retryable": false,
                    "resume_required": true
                }
            })
            .to_string(),
        );
    }
    live_stream_failure_chunk(
        "Bridge failed to commit response continuity state",
        "api_error",
    )
}

fn record_live_stream_downstream_capture(ctx: &RequestContext, chunk: &[u8]) {
    let Some(traffic) = ctx.traffic.as_ref() else {
        return;
    };
    for event in parse_sse_events(chunk) {
        let Ok(data) = serde_json::from_str::<serde_json::Value>(&event.data) else {
            continue;
        };
        traffic.write_json_event(
            "050-downstream-event",
            &serde_json::json!({
                "event": event.event.as_deref().unwrap_or("message"),
                "data": data,
            }),
        );
    }
}

fn record_live_stream_progress(ctx: &RequestContext, chunk: &[u8]) {
    if let Some(monitor) = ctx.monitor.as_ref() {
        let (input_tokens, output_tokens) = usage_from_anthropic_sse(chunk);
        monitor.stream_progress(
            &ctx.req_id,
            chunk.len() as u64,
            count_sse_events(chunk),
            input_tokens,
            output_tokens,
        );
    }
}

enum SingleLiveStreamState {
    Chunk {
        chunk: Vec<u8>,
        cleanup: LiveRequestStateCleanup,
    },
    Hold(LiveRequestStateCleanup),
}

fn single_live_stream_response(chunk: Vec<u8>, cleanup: LiveRequestStateCleanup) -> Response {
    let state = SingleLiveStreamState::Chunk { chunk, cleanup };
    let stream = futures_util::stream::unfold(state, |state| async move {
        match state {
            SingleLiveStreamState::Chunk { chunk, mut cleanup } => {
                cleanup.note_terminal_delivered();
                Some((
                    Ok::<Bytes, std::io::Error>(Bytes::from(chunk)),
                    SingleLiveStreamState::Hold(cleanup),
                ))
            }
            SingleLiveStreamState::Hold(_cleanup) => None,
        }
    });
    event_stream_response(stream)
}

struct LiveStreamFrame {
    item: Result<Bytes, std::io::Error>,
    delivery_ack: Option<tokio::sync::oneshot::Sender<()>>,
}

impl LiveStreamFrame {
    fn data(chunk: Vec<u8>) -> Self {
        Self {
            item: Ok(Bytes::from(chunk)),
            delivery_ack: None,
        }
    }

    fn terminal(chunk: Vec<u8>, delivery_ack: tokio::sync::oneshot::Sender<()>) -> Self {
        Self {
            item: Ok(Bytes::from(chunk)),
            delivery_ack: Some(delivery_ack),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn remaining_live_stream_response(
    mut upstream_events: websocket::CodexWebSocketEventStream,
    mut translator: LiveStreamTranslator,
    first_chunk: Vec<u8>,
    mut deferred_terminal_chunk: Vec<u8>,
    ctx: RequestContext,
    request_body: translate::request::ResponsesRequest,
    mut upstream_sse_body: Vec<u8>,
    mut cleanup: LiveRequestStateCleanup,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<LiveStreamFrame>(64);
    tokio::spawn(async move {
        if tx.send(LiveStreamFrame::data(first_chunk)).await.is_err() {
            cleanup.abort();
            return;
        }
        let mut heartbeat = tokio::time::interval(LIVE_STREAM_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await;
        loop {
            let item = tokio::select! {
                biased;
                _ = tx.closed() => {
                    cleanup.abort();
                    return;
                }
                item = upstream_events.recv() => item,
                _ = heartbeat.tick() => {
                    let chunk = translator.ping_chunk(None);
                    if !chunk.is_empty() {
                        if tx.send(LiveStreamFrame::data(chunk.clone())).await.is_err() {
                            cleanup.abort();
                            return;
                        }
                        record_live_stream_downstream_capture(&ctx, &chunk);
                        record_live_stream_progress(&ctx, &chunk);
                    }
                    continue;
                }
            };
            let Some(item) = item else {
                break;
            };
            match item {
                Ok(payload) => {
                    append_upstream_sse_payload(&mut upstream_sse_body, &payload);
                    let (chunk, terminal, downstream_finished) =
                        match translate_live_stream_payload(&mut translator, &payload, None) {
                            Ok(result) => result,
                            Err(message) => {
                                cleanup.abort();
                                let mut chunk = translator.error_chunk(&message, "api_error", None);
                                if chunk.is_empty() {
                                    chunk = live_stream_failure_chunk(&message, "api_error");
                                }
                                if tx.send(LiveStreamFrame::data(chunk.clone())).await.is_ok() {
                                    record_live_stream_downstream_capture(&ctx, &chunk);
                                    record_live_stream_progress(&ctx, &chunk);
                                }
                                return;
                            }
                        };
                    if translator.has_semantic_output() {
                        cleanup.mark_semantic_output_started();
                    }
                    if terminal {
                        deferred_terminal_chunk.extend_from_slice(&chunk);
                        let terminal_chunk = match cleanup.finalize(
                            &request_body,
                            &upstream_sse_body,
                            upstream_events.socket_id(),
                        ) {
                            Ok(()) => std::mem::take(&mut deferred_terminal_chunk),
                            Err(error) => {
                                cleanup.abort();
                                live_stream_commit_failure_chunk(&error)
                            }
                        };
                        if terminal_chunk.is_empty() {
                            return;
                        }
                        let (delivery_tx, delivery_rx) = tokio::sync::oneshot::channel();
                        if tx
                            .send(LiveStreamFrame::terminal(
                                terminal_chunk.clone(),
                                delivery_tx,
                            ))
                            .await
                            .is_ok()
                            && delivery_rx.await.is_ok()
                        {
                            record_live_stream_downstream_capture(&ctx, &terminal_chunk);
                            record_live_stream_progress(&ctx, &terminal_chunk);
                            cleanup.note_terminal_delivered();
                        }
                        return;
                    }
                    if downstream_finished {
                        deferred_terminal_chunk.extend_from_slice(&chunk);
                        continue;
                    }
                    if !chunk.is_empty() {
                        if tx.send(LiveStreamFrame::data(chunk.clone())).await.is_err() {
                            cleanup.abort();
                            return;
                        }
                        record_live_stream_downstream_capture(&ctx, &chunk);
                        record_live_stream_progress(&ctx, &chunk);
                    }
                }
                Err(err) => {
                    cleanup.abort();
                    let error_type = codex_stream_error_type(&err);
                    let mut chunk =
                        translator.error_chunk(codex_error_message(&err), error_type, None);
                    if chunk.is_empty() {
                        chunk = live_stream_failure_chunk(codex_error_message(&err), error_type);
                    }
                    if tx.send(LiveStreamFrame::data(chunk.clone())).await.is_ok() {
                        record_live_stream_downstream_capture(&ctx, &chunk);
                        record_live_stream_progress(&ctx, &chunk);
                    }
                    return;
                }
            }
        }

        cleanup.abort();
        let mut chunk = translator.error_chunk(
            "Upstream event stream closed before terminal Codex response event",
            "api_error",
            None,
        );
        if chunk.is_empty() {
            chunk = live_stream_failure_chunk(
                "Upstream event stream closed before terminal Codex response event",
                "api_error",
            );
        }
        if tx.send(LiveStreamFrame::data(chunk.clone())).await.is_ok() {
            record_live_stream_downstream_capture(&ctx, &chunk);
            record_live_stream_progress(&ctx, &chunk);
        }
    });

    let stream = futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|mut frame| {
            if let Some(delivery_ack) = frame.delivery_ack.take() {
                let _ = delivery_ack.send(());
            }
            (frame.item, rx)
        })
    });
    event_stream_response(stream)
}

fn append_upstream_sse_payload(buffer: &mut Vec<u8>, payload: &serde_json::Value) {
    let text = payload.to_string();
    for line in text.lines() {
        buffer.extend_from_slice(b"data: ");
        buffer.extend_from_slice(line.as_bytes());
        buffer.push(b'\n');
    }
    buffer.push(b'\n');
}

fn event_stream_response<S>(stream: S) -> Response
where
    S: futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    let headers = [
        (http::header::CONTENT_TYPE, "text/event-stream"),
        (http::header::CACHE_CONTROL, "no-cache"),
        (http::header::CONNECTION, "keep-alive"),
    ];
    (headers, Body::from_stream(stream)).into_response()
}

fn empty_buffered_completion_error() -> client::CodexError {
    client::CodexError {
        status: 503,
        message: "Codex completed without producing output".to_string(),
        detail: Some(EMPTY_CODEX_COMPLETION_DETAIL.to_string()),
        retry_after: None,
        origin: match config::codex_transport() {
            config::CodexTransport::Http => client::CodexErrorOrigin::BufferedHttp,
            _ => client::CodexErrorOrigin::BufferedWebSocket,
        },
    }
}

/// True when the buffered upstream body ended in a successful terminal event
/// without ever producing semantic output (text, thinking, tool, web search).
fn is_empty_codex_success_completion(upstream_sse: &[u8]) -> bool {
    use self::translate::reducer::{ReducerEvent, TERM_COMPLETED, TERM_DONE};

    let Ok(events) = self::translate::reducer::reduce_upstream_bytes(upstream_sse) else {
        return false;
    };
    let mut saw_success_terminal = false;
    for event in &events {
        match event {
            ReducerEvent::TextDelta { text, .. } if !text.is_empty() => return false,
            ReducerEvent::ThinkingStart { .. }
            | ReducerEvent::ToolStart { .. }
            | ReducerEvent::WebSearch { .. } => return false,
            ReducerEvent::Finish { terminal_type, .. }
                if terminal_type == TERM_COMPLETED || terminal_type == TERM_DONE =>
            {
                saw_success_terminal = true;
            }
            _ => {}
        }
    }
    saw_success_terminal
}

fn is_codex_terminal_event(payload: &serde_json::Value) -> bool {
    matches!(
        payload.get("type").and_then(|v| v.as_str()),
        Some("response.completed")
            | Some("response.incomplete")
            | Some("response.done")
            | Some("response.failed")
            | Some("response.error")
            | Some("error")
    )
}

fn is_codex_success_terminal_event(payload: &serde_json::Value) -> bool {
    matches!(
        payload.get("type").and_then(|v| v.as_str()),
        Some("response.completed") | Some("response.done")
    )
}

fn retryable_live_start_codex_error(err: &client::CodexError) -> bool {
    if err.origin == client::CodexErrorOrigin::WebSocketHandshake {
        if err.detail.as_deref() == Some(websocket::WEBSOCKET_PROXY_TUNNEL_REJECTED_DETAIL) {
            return false;
        }
        return err.status == 0 || matches!(err.status, 429 | 500 | 502 | 503 | 504 | 529);
    }
    matches!(err.status, 429 | 500 | 502 | 503 | 504 | 529)
        || (err.status == 0 && retryable_live_message(codex_error_message(err)))
}

fn is_missing_previous_response_error(err: &client::CodexError) -> bool {
    matches!(
        err.detail.as_deref(),
        Some("previous_response_not_found")
            | Some(websocket::WEBSOCKET_CONTINUATION_SOCKET_MISSING_DETAIL)
    )
}

fn drop_live_continuation_for_retry(continuation: &mut Option<ContinuationReservation>) -> bool {
    if continuation
        .as_ref()
        .and_then(|reservation| reservation.candidate().previous_response_id.as_deref())
        .is_none()
    {
        return false;
    }

    if let Some(reservation) = continuation.as_ref() {
        *continuation = Some(reservation.full_context_retry());
    }
    true
}

fn retryable_live_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    [
        "overloaded",
        "rate limit",
        "you can retry your request",
        "temporarily unavailable",
        "timed out",
        "connection closed",
        "connection reset",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn retryable_live_start_payload(payload: &serde_json::Value, _message: &str) -> bool {
    events::classify_event_failure(payload).is_some_and(|failure| failure.retryable())
}

fn retry_after_from_live_payload(payload: &serde_json::Value) -> Option<String> {
    events::classify_event_failure(payload).and_then(|failure| failure.retry_after)
}

fn codex_stream_error_type(err: &client::CodexError) -> &'static str {
    match err.status {
        429 => "rate_limit_error",
        529 => "overloaded_error",
        _ if codex_error_message(err)
            .to_lowercase()
            .contains("overloaded") =>
        {
            "overloaded_error"
        }
        _ => "api_error",
    }
}

#[allow(clippy::too_many_arguments)]
fn update_continuation_from_upstream(
    session_id: Option<&str>,
    continuation: &ContinuationReservation,
    compaction_attempt: Option<CompactionAttempt>,
    request_body: &translate::request::ResponsesRequest,
    upstream_body: &[u8],
    socket_id: Option<u64>,
    compact_boundary: bool,
) {
    match finish_metadata_from_upstream(upstream_body) {
        Ok(Some(finish)) if finish.continuation_eligible => {
            if compact_boundary {
                activate_compaction(
                    session_id,
                    compaction_attempt,
                    &request_body.model,
                    &finish.output_items,
                );
            }
            record_continuation_for_owner(
                continuation,
                request_body,
                finish.response_id.as_deref(),
                socket_id,
                &finish.output_items,
            );
        }
        _ => {
            abort_compaction_attempt(session_id, compaction_attempt);
            abort_continuation_for_owner(continuation);
        }
    }
}

fn commit_inline_compaction(
    prepared: &inline_compaction::PreparedInlineCompaction,
    upstream_body: &[u8],
) -> Result<inline_compaction::CommitReport, inline_compaction::InlineCompactionError> {
    let finish = finish_metadata_from_upstream(upstream_body)
        .map_err(|_| {
            inline_compaction::InlineCompactionError::InvalidUpstream(
                "completion metadata could not be reduced",
            )
        })?
        .filter(|finish| finish.continuation_eligible)
        .ok_or(inline_compaction::InlineCompactionError::InvalidUpstream(
            "completion metadata is missing or incomplete",
        ))?;
    let report =
        inline_compaction::commit_response_report(prepared, upstream_body, &finish.output_items)?;
    log_inline_compaction_commit(&report);
    Ok(report)
}

fn log_inline_compaction_commit(report: &inline_compaction::CommitReport) {
    let mut fields = serde_json::Map::from_iter([
        (
            "commitOutcome".to_string(),
            serde_json::json!(report.outcome.as_str()),
        ),
        (
            "compactionCount".to_string(),
            serde_json::json!(report.compaction_count),
        ),
        (
            "lastItemIndex".to_string(),
            serde_json::json!(report.last_item_index),
        ),
        (
            "suffixCount".to_string(),
            serde_json::json!(report.suffix_count),
        ),
        (
            "usageInputTokens".to_string(),
            serde_json::json!(report.usage_input_tokens),
        ),
        (
            "stateBytes".to_string(),
            serde_json::json!(report.state_bytes),
        ),
        ("laneHash".to_string(), serde_json::json!(&report.lane_hash)),
        (
            "invalidationReason".to_string(),
            serde_json::json!(report.invalidation_reason),
        ),
        (
            "anomalyReason".to_string(),
            serde_json::json!(report.anomaly_reason),
        ),
    ]);
    if let Some(context) = report.effective_reasoning_context.as_deref() {
        fields.insert("reasoningContext".to_string(), serde_json::json!(context));
    }
    let log = create_logger("codex_inline_compaction");
    if report.anomaly_reason.is_some() {
        log.warn("anomaly", Some(fields));
    } else {
        log.info("commit", Some(fields));
    }
}

fn log_inline_compaction_error(
    req_id: Option<&str>,
    session_hash: Option<&str>,
    lane_hash: Option<&str>,
    error: &inline_compaction::InlineCompactionError,
    state_backend_degraded: bool,
) {
    let mut fields = serde_json::Map::from_iter([
        ("commitOutcome".to_string(), serde_json::json!("error")),
        (
            "invalidationReason".to_string(),
            serde_json::json!(error.reason_code()),
        ),
        (
            "stateBackendDegraded".to_string(),
            serde_json::json!(state_backend_degraded),
        ),
    ]);
    if let Some(req_id) = req_id {
        fields.insert("reqId".to_string(), serde_json::json!(req_id));
    }
    if let Some(session_hash) = session_hash {
        fields.insert("sessionHash".to_string(), serde_json::json!(session_hash));
    }
    if let Some(lane_hash) = lane_hash {
        fields.insert("laneHash".to_string(), serde_json::json!(lane_hash));
    }
    if let Some(details) = error.durability_details() {
        fields.extend([
            (
                "oldRevision".to_string(),
                serde_json::json!(details.old_revision),
            ),
            (
                "candidateRevision".to_string(),
                serde_json::json!(details.candidate_revision),
            ),
            (
                "oldSidecarHash".to_string(),
                serde_json::json!(&details.old_sidecar_hash),
            ),
            (
                "newSidecarHash".to_string(),
                serde_json::json!(&details.new_sidecar_hash),
            ),
            (
                "tempFsyncedAtMs".to_string(),
                serde_json::json!(details.temp_fsynced_at_ms),
            ),
            (
                "renameAtMs".to_string(),
                serde_json::json!(details.rename_at_ms),
            ),
            (
                "directoryFsyncStartedAtMs".to_string(),
                serde_json::json!(details.directory_fsync_started_at_ms),
            ),
            (
                "directoryFsyncFailedAtMs".to_string(),
                serde_json::json!(details.directory_fsync_failed_at_ms),
            ),
            ("errno".to_string(), serde_json::json!(details.errno)),
            (
                "filesystemType".to_string(),
                serde_json::json!(&details.filesystem_type),
            ),
            ("mountId".to_string(), serde_json::json!(details.mount_id)),
            (
                "mountOptions".to_string(),
                serde_json::json!(&details.mount_options),
            ),
            ("namespaceSwitched".to_string(), serde_json::json!(true)),
            ("sidecarCommitted".to_string(), serde_json::json!(false)),
            ("continuityFinalized".to_string(), serde_json::json!(false)),
            ("terminalDelivered".to_string(), serde_json::json!(false)),
            (
                "transparentRetryAllowed".to_string(),
                serde_json::json!(false),
            ),
            ("recoveryRequired".to_string(), serde_json::json!(true)),
        ]);
    }
    create_logger("codex_inline_compaction").warn("commit", Some(fields));
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn log_upstream_capacity_error(req_id: &str, class: UpstreamClass, error: &UpstreamCapacityError) {
    create_logger("codex_upstream_capacity").warn(
        "acquire_failed",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), serde_json::json!(req_id)),
            ("class".to_string(), serde_json::json!(class.as_str())),
            ("error".to_string(), serde_json::json!(error.to_string())),
        ])),
    );
}

fn map_upstream_capacity_error_to_response(error: &UpstreamCapacityError) -> Response {
    json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "api_error",
        error.to_string(),
    )
}

fn map_inline_compaction_error_to_response(
    error: &inline_compaction::InlineCompactionError,
) -> Response {
    if error.durability_details().is_some() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "type": "error",
                "error": {
                    "type": "api_error",
                    "code": STATE_DURABILITY_INDETERMINATE_CODE,
                    "message": STATE_DURABILITY_INDETERMINATE_MESSAGE,
                    "retryable": false,
                    "resume_required": true
                }
            })),
        )
            .into_response();
    }
    json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "api_error",
        error.to_string(),
    )
}

fn map_codex_error_to_response(err: &client::CodexError) -> Response {
    let message = codex_error_message(err);
    if is_context_window_overflow(message) {
        return map_codex_failure_to_response(message);
    }
    if err.detail.as_deref() == Some(EMPTY_CODEX_COMPLETION_DETAIL) {
        return json_error(StatusCode::SERVICE_UNAVAILABLE, "api_error", &err.message);
    }

    match err.status {
        401 => json_error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            err.detail.as_deref().unwrap_or("Authentication failed"),
        ),
        403 => json_error(
            StatusCode::FORBIDDEN,
            "permission_error",
            err.detail.as_deref().unwrap_or("Permission denied"),
        ),
        429 => {
            let response = json_error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                &err.message,
            );
            if let Some(retry_after) = err.retry_after.as_deref() {
                ([(http::header::RETRY_AFTER, retry_after)], response).into_response()
            } else {
                response
            }
        }
        status @ (400..=599) => {
            let response = json_error(
                StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
                if status == 529 {
                    "overloaded_error"
                } else {
                    "api_error"
                },
                codex_error_message(err),
            );
            if let Some(retry_after) = err.retry_after.as_deref() {
                ([(http::header::RETRY_AFTER, retry_after)], response).into_response()
            } else {
                response
            }
        }
        _ => json_error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            codex_error_message(err),
        ),
    }
}

fn map_codex_failure_to_response(message: &str) -> Response {
    if is_context_window_overflow(message) {
        json_error(StatusCode::PAYLOAD_TOO_LARGE, "request_too_large", message)
    } else {
        json_error(StatusCode::BAD_GATEWAY, "api_error", message)
    }
}

fn is_context_window_overflow(message: &str) -> bool {
    message.to_ascii_lowercase().contains("context window")
}

fn codex_error_message(err: &client::CodexError) -> &str {
    err.detail.as_deref().unwrap_or({
        if err.status == 0 {
            err.message.as_str()
        } else {
            "Upstream error"
        }
    })
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

pub(crate) struct CodexCli;

impl CliHandlers for CodexCli {
    fn login(&self) -> Result<(), anyhow::Error> {
        let tokens = run_browser_login()?;
        let store = file_store();
        let manager = CodexAuthManager::new(store);
        let saved = manager.persist_initial_tokens(&tokens)?;
        print!(
            "{}",
            format_auth_saved_output(&manager.store.auth_path(), saved.account_id.as_deref())
        );
        Ok(())
    }

    fn device(&self) -> Result<(), anyhow::Error> {
        let tokens = DeviceAuthClient::new().run()?;
        let store = file_store();
        let manager = CodexAuthManager::new(store);
        let saved = manager.persist_initial_tokens(&tokens)?;
        print!(
            "{}",
            format_auth_saved_output(&manager.store.auth_path(), saved.account_id.as_deref())
        );
        Ok(())
    }

    fn status(&self) -> Result<(), anyhow::Error> {
        let store = file_store();
        let stored = store.load_auth()?;
        match stored {
            Some(auth) => {
                println!(
                    "Account: {}",
                    auth.account_id.as_deref().unwrap_or("(none)")
                );
                println!("{}", format_expiry(auth.expires, now_ms()));
                println!("Storage: {}", store.auth_path());
                Ok(())
            }
            None => {
                anyhow::bail!("Not authenticated");
            }
        }
    }

    fn logout(&self) -> Result<(), anyhow::Error> {
        let store = file_store();
        store.clear_auth()?;
        println!("Logged out");
        Ok(())
    }
}

pub(crate) static CODEX_CLI: CodexCli = CodexCli;

// ---------------------------------------------------------------------------
// CLI helpers
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn format_expiry(expires: u64, now: u64) -> String {
    let remaining = (i128::from(expires) - i128::from(now)).div_euclid(1000);
    let iso = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(expires) * 1_000_000)
        .ok()
        .and_then(|dt| {
            let fmt = time::format_description::parse_borrowed::<2>(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
            )
            .ok()?;
            dt.format(&fmt).ok()
        })
        .unwrap_or_else(|| "invalid".to_string());
    format!("Expires: {iso} (in {remaining}s)")
}

fn format_auth_saved_output(auth_path: &str, account_id: Option<&str>) -> String {
    let mut out = format!("Auth saved in {auth_path}\n");
    if let Some(account_id) = account_id {
        out.push_str(&format!("Account: {account_id}\n"));
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use futures_util::{SinkExt, StreamExt};
    use http_body_util::BodyExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::tungstenite::Message;

    use super::*;

    fn live_test_request(text: &str) -> translate::request::ResponsesRequest {
        translate::request::ResponsesRequest {
            model: "gpt-5.6-sol".to_string(),
            instructions: None,
            input: vec![translate::request::ResponsesInputItem::Message {
                role: "user".to_string(),
                content: vec![translate::request::ResponsesContentPart::InputText {
                    text: text.to_string(),
                }],
            }],
            tools: None,
            tool_choice: None,
            store: false,
            stream: true,
            parallel_tool_calls: true,
            include: None,
            client_metadata: None,
            service_tier: None,
            prompt_cache_key: None,
            text: translate::request::ResponsesText {
                verbosity: None,
                format: None,
            },
            reasoning: None,
            context_management: None,
            raw_input_override: None,
        }
    }

    fn live_test_context(session_id: &str) -> RequestContext {
        RequestContext {
            req_id: format!("request-{session_id}"),
            session_id: Some(session_id.to_string()),
            session_seq: None,
            provider: "codex".to_string(),
            traffic: None,
            monitor: None,
        }
    }

    fn live_test_cleanup(
        continuation: ContinuationReservation,
        session_id: Option<&str>,
        compaction: LiveStreamCompaction,
        request: &translate::request::ResponsesRequest,
    ) -> LiveRequestStateCleanup {
        LiveRequestStateCleanup::new(LiveRequestLease::new(
            LiveStreamResources::default(),
            continuation,
            session_id.map(str::to_string),
            compaction,
            session_id
                .map(|value| format!("request-{value}"))
                .unwrap_or_else(|| "request-test".to_string()),
            request,
        ))
    }

    fn authenticated_live_test_client(base_url: String) -> Arc<CodexHttpClient> {
        let client = CodexHttpClient::new_for_test(
            reqwest::Client::builder().no_proxy().build().unwrap(),
            base_url,
            1_000,
            1_000,
            0,
        );
        client
            .auth_manager()
            .set_test_auth(auth::token_store::StoredAuth {
                access: "test".to_string(),
                refresh: String::new(),
                expires: u64::MAX,
                account_id: Some("acct".to_string()),
            });
        Arc::new(client)
    }

    async fn next_live_websocket_request(
        websocket: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
    ) -> serde_json::Value {
        loop {
            match websocket.next().await {
                Some(Ok(Message::Ping(payload))) => {
                    websocket.send(Message::Pong(payload)).await.unwrap();
                }
                Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
                other => panic!("unexpected WebSocket request frame: {other:?}"),
            }
        }
    }

    async fn emit_live_event(
        websocket: &mut tokio_tungstenite::WebSocketStream<TcpStream>,
        event: &serde_json::Value,
    ) {
        websocket
            .send(Message::Text(event.to_string()))
            .await
            .unwrap();
    }

    fn upstream_sse(events: &[serde_json::Value]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for event in events {
            bytes.extend_from_slice(format!("data: {event}\n\n").as_bytes());
        }
        bytes
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn live_directory_fsync_failure_requires_resume_without_success_stop() {
        let _continuation_guard = continuation::lock_continuation_registry_for_tests();
        let _failure_guard = inline_compaction::lock_test_write_failures();
        let session_id = "live-directory-fsync-indeterminate";
        let owner = ConversationIdentity::Main(session_id.to_string());
        continuation::clear_continuation_for_owner(Some(&owner));

        let temp = tempfile::TempDir::new().unwrap();
        let config =
            inline_compaction::InlineCompactionConfig::for_tests(temp.path().join("state"), 32_768);
        let mut request = live_test_request("durability boundary");
        let prepared =
            inline_compaction::prepare_request(&config, session_id, &mut request, false).unwrap();
        let lane_hash = prepared.lane_hash().to_string();
        inline_compaction::inject_directory_fsync_failure_for_tests(&prepared, 5); // EIO

        let recovery = Arc::new(inline_compaction::StateRecoveryRegistry::default());
        let continuation = continuation_candidate_for_owner(Some(&owner), &request, false);
        let lease = LiveRequestLease::new(
            LiveStreamResources {
                inline_prepared: Some(prepared),
                inline_recovery: recovery.clone(),
                ..LiveStreamResources::default()
            },
            continuation.clone(),
            Some(session_id.to_string()),
            LiveStreamCompaction {
                compact_boundary: false,
                attempt: None,
            },
            "request-directory-fsync-indeterminate".to_string(),
            &request,
        );
        let mut cleanup = LiveRequestStateCleanup::new(lease);
        let upstream = upstream_sse(&[
            serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {"type": "compaction", "encrypted_content": "opaque-candidate"}
            }),
            serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 1,
                "item": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "candidate output"}]
                }
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_directory_fsync_indeterminate",
                    "status": "completed",
                    "reasoning": {"context": "all_turns"},
                    "usage": {"input_tokens": 40000, "output_tokens": 10}
                }
            }),
        ]);
        let error = cleanup.finalize(&request, &upstream, None).unwrap_err();

        assert_eq!(error.reason_code(), "state_durability_indeterminate");
        assert!(cleanup.lease.terminal_seen);
        assert!(!cleanup.lease.committed);
        assert!(!cleanup.lease.finalized);
        assert!(!cleanup.lease.terminal_delivered);
        assert!(recovery.recovery_required(&lane_hash));
        assert!(recovery.backend_degraded());

        let chunk = live_stream_commit_failure_chunk(&error);
        let rendered = String::from_utf8(chunk.clone()).unwrap();
        assert!(!rendered.contains("message_stop"));
        let events = parse_sse_events(&chunk);
        assert_eq!(events.len(), 1);
        let payload: serde_json::Value = serde_json::from_str(&events[0].data).unwrap();
        assert_eq!(
            payload
                .pointer("/error/code")
                .and_then(|value| value.as_str()),
            Some(STATE_DURABILITY_INDETERMINATE_CODE)
        );
        assert_eq!(
            payload
                .pointer("/error/retryable")
                .and_then(|value| value.as_bool()),
            Some(false)
        );
        assert_eq!(
            payload
                .pointer("/error/resume_required")
                .and_then(|value| value.as_bool()),
            Some(true)
        );

        cleanup.abort();
        assert!(cleanup.lease.finalized);
        assert!(!cleanup.lease.committed);
        assert!(!continuation::is_current_turn_for_owner(&continuation));
        continuation::clear_continuation_for_owner(Some(&owner));
    }

    #[test]
    fn terminal_only_completed_upstream_is_empty_completion() {
        let body = upstream_sse(&[serde_json::json!({
            "type": "response.completed",
            "response": {"id": "resp_1", "status": "completed", "incomplete_details": null, "usage": {"input_tokens": 5, "output_tokens": 0}}
        })]);
        assert!(is_empty_codex_success_completion(&body));
    }

    #[test]
    fn terminal_only_done_upstream_is_empty_completion() {
        let body = upstream_sse(&[serde_json::json!({
            "type": "response.done",
            "response": {"id": "resp_1", "usage": {}}
        })]);
        assert!(is_empty_codex_success_completion(&body));
    }

    #[test]
    fn empty_message_item_is_empty_completion() {
        let body = upstream_sse(&[
            serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "message", "id": "msg_1"}
            }),
            serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {"type": "message"}
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {"id": "resp_1", "usage": {}}
            }),
        ]);
        assert!(is_empty_codex_success_completion(&body));
    }

    #[test]
    fn upstream_with_text_is_not_empty_completion() {
        let body = upstream_sse(&[
            serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "message", "id": "msg_1"}
            }),
            serde_json::json!({
                "type": "response.output_text.delta",
                "output_index": 0,
                "delta": "hello"
            }),
            serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {"type": "message"}
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {"id": "resp_1", "usage": {}}
            }),
        ]);
        assert!(!is_empty_codex_success_completion(&body));
    }

    #[test]
    fn upstream_with_tool_call_is_not_empty_completion() {
        let body = upstream_sse(&[
            serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": "call_1", "name": "Read", "arguments": ""}
            }),
            serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {"type": "function_call", "call_id": "call_1", "name": "Read", "arguments": "{}"}
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {"id": "resp_1", "usage": {}}
            }),
        ]);
        assert!(!is_empty_codex_success_completion(&body));
    }

    #[test]
    fn terminal_only_incomplete_upstream_is_not_empty_completion() {
        let body = upstream_sse(&[serde_json::json!({
            "type": "response.incomplete",
            "response": {"id": "resp_1", "incomplete_details": {"reason": "max_output_tokens"}, "usage": {}}
        })]);
        assert!(!is_empty_codex_success_completion(&body));
    }

    #[test]
    fn upstream_without_terminal_event_is_not_empty_completion() {
        assert!(!is_empty_codex_success_completion(&upstream_sse(&[])));
    }

    fn request_with_tools(tools: serde_json::Value) -> MessagesRequest {
        serde_json::from_value(serde_json::json!({
            "model": "gpt-5.6-luna",
            "messages": [{"role":"user", "content":"find it"}],
            "tools": tools
        }))
        .unwrap()
    }

    #[test]
    fn web_search_requests_leave_lite_lane_and_upgrade_luna() {
        let body = request_with_tools(serde_json::json!([
            {"type":"web_search_20250305", "name":"web_search"}
        ]));
        for (resolved, expected) in [
            ("gpt-5.6-luna", "gpt-5.6-sol"),
            ("gpt-5.6-sol", "gpt-5.6-sol"),
            ("gpt-5.6-terra", "gpt-5.6-terra"),
            ("gpt-5.4", "gpt-5.4"),
        ] {
            let mut model = resolved.to_string();
            let lite = apply_model_lane_for_request(&mut model, &body);
            assert!(!lite, "{resolved} with web_search must use the full lane");
            assert_eq!(model, expected);
        }
    }

    #[test]
    fn requests_without_web_search_keep_model_and_lite_lane() {
        let body = request_with_tools(serde_json::json!([
            {"name":"Bash", "input_schema":{}}
        ]));
        for (resolved, lite_expected) in [
            ("gpt-5.6-luna", true),
            ("gpt-5.6-sol", true),
            ("gpt-5.4", false),
        ] {
            let mut model = resolved.to_string();
            let lite = apply_model_lane_for_request(&mut model, &body);
            assert_eq!(model, resolved, "model must not change without web_search");
            assert_eq!(lite, lite_expected);
        }
    }

    #[test]
    fn generation_timing_ignores_control_events() {
        assert!(!codex_generation_event(&serde_json::json!({
            "type": "codex.rate_limits"
        })));
        assert!(!codex_generation_event(&serde_json::json!({
            "type": "keepalive"
        })));
        assert!(codex_generation_event(&serde_json::json!({
            "type": "response.created"
        })));
    }

    #[test]
    fn live_stream_progress_records_terminal_usage() {
        let monitor = crate::monitor::MonitorHandle::new(10);
        monitor.request_started(
            "request",
            None,
            None,
            crate::monitor::EndpointKind::Messages,
        );
        let ctx = RequestContext {
            req_id: "request".to_string(),
            session_id: None,
            session_seq: None,
            provider: "codex".to_string(),
            traffic: None,
            monitor: Some(monitor.clone()),
        };
        let chunk = b"event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"input_tokens\":12,\"output_tokens\":48}}\n\n";

        record_live_stream_progress(&ctx, chunk);

        let state = monitor.snapshot();
        assert_eq!(state.active[0].input_tokens, Some(12));
        assert_eq!(state.active[0].output_tokens, Some(48));
    }

    #[tokio::test]
    async fn live_stream_response_emits_downstream_frames_before_terminal_event() {
        use http_body_util::BodyExt as _;

        let body = request_with_tools(serde_json::json!([]));
        let request_body = translate_request(
            &body,
            TranslateOptions {
                session_id: None,
                service_tier: None,
                model: "gpt-5.6-sol".to_string(),
                use_responses_lite: true,
            },
        )
        .unwrap();
        let ctx = RequestContext {
            req_id: "incremental-http".to_string(),
            session_id: None,
            session_seq: None,
            provider: "codex".to_string(),
            traffic: None,
            monitor: None,
        };
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tx.send(Ok(serde_json::json!({"type": "keepalive"})))
            .await
            .unwrap();
        tx.send(Ok(serde_json::json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {"type": "message", "id": "msg_up"}
        })))
        .await
        .unwrap();
        tx.send(Ok(serde_json::json!({
            "type": "response.output_text.delta",
            "output_index": 0,
            "delta": "first"
        })))
        .await
        .unwrap();

        let (rx, _) = websocket::CodexWebSocketEventStream::pending(rx);
        let continuation = ContinuationReservation::for_owner_turn(None, None);
        let cleanup = live_test_cleanup(
            continuation,
            None,
            LiveStreamCompaction {
                compact_boundary: false,
                attempt: None,
            },
            &request_body,
        );
        let response = match live_stream_response_once(
            rx,
            "msg_test".to_string(),
            "claude-opus-4-8",
            ctx,
            request_body,
            cleanup,
        )
        .await
        {
            LiveStreamStart::Response(response) => response,
            LiveStreamStart::Retry { error, .. } => panic!("unexpected retry: {error}"),
        };
        let mut body = response.into_body();
        let first = tokio::time::timeout(Duration::from_millis(200), body.frame())
            .await
            .expect("initial downstream frame must be available immediately")
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        let first = String::from_utf8(first.to_vec()).unwrap();
        assert!(first.contains("event: message_start"));
        assert!(first.contains("event: ping"));
        assert!(first.contains("event: content_block_start"));
        assert!(first.contains("event: content_block_delta"));

        tx.send(Ok(serde_json::json!({
            "type": "response.output_text.delta",
            "output_index": 0,
            "delta": "second"
        })))
        .await
        .unwrap();
        let second = tokio::time::timeout(Duration::from_millis(200), body.frame())
            .await
            .expect("text delta must arrive before the terminal event")
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert!(
            String::from_utf8(second.to_vec())
                .unwrap()
                .contains("event: content_block_delta")
        );

        for payload in [
            serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {"type": "message"}
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_1",
                    "status": "completed",
                    "incomplete_details": null,
                    "usage": {"input_tokens": 1, "output_tokens": 1}
                }
            }),
        ] {
            tx.send(Ok(payload)).await.unwrap();
        }
        drop(tx);
        while let Some(frame) = body.frame().await {
            frame.unwrap();
        }
    }

    #[test]
    fn supported_models_includes_fast_variants() {
        let provider = CodexProvider::new();
        let models = provider.supported_models();
        assert!(models.contains(&"gpt-5.6-sol".to_string()));
        assert!(models.contains(&"gpt-5.6-sol-fast".to_string()));
        assert!(models.contains(&"gpt-5.6-terra".to_string()));
        assert!(models.contains(&"gpt-5.6-luna".to_string()));
        assert!(models.contains(&"gpt-5.4".to_string()));
        assert!(models.contains(&"gpt-5.4-mini".to_string()));
    }

    #[test]
    fn format_auth_saved_output_with_account() {
        assert_eq!(
            format_auth_saved_output("/tmp/auth.json", Some("acct_1")),
            "Auth saved in /tmp/auth.json\nAccount: acct_1\n"
        );
    }

    #[test]
    fn format_auth_saved_output_without_account() {
        assert_eq!(
            format_auth_saved_output("/tmp/auth.json", None),
            "Auth saved in /tmp/auth.json\n"
        );
    }

    #[test]
    fn format_expiry_with_future_expiry() {
        // 2100-01-01T00:00:00Z in ms
        let expires = 4102444800000;
        let now = 4102444790000; // 10s before
        let output = format_expiry(expires, now);
        assert!(output.starts_with("Expires: 2100-01-01T00:00:00.000Z (in "));
        assert!(output.ends_with("s)"));
    }

    #[test]
    fn format_expiry_with_past_expiry() {
        // 2000-01-01T00:00:00Z in ms
        let expires = 946684800000;
        let now = 946684810000; // 10s after
        let output = format_expiry(expires, now);
        assert!(output.starts_with("Expires: 2000-01-01T00:00:00.000Z (in -"));
    }

    #[tokio::test]
    async fn live_upstream_status_and_retry_after_are_preserved() {
        let err = client::CodexError {
            status: 422,
            message: "invalid request".to_string(),
            detail: Some("invalid request".to_string()),
            retry_after: Some("7".to_string()),
            origin: client::CodexErrorOrigin::WebSocketHandshake,
        };
        let response = map_codex_error_to_response(&err);
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            response.headers().get(http::header::RETRY_AFTER).unwrap(),
            "7"
        );
    }

    #[tokio::test]
    async fn statusless_codex_error_returns_source_message() {
        let err = client::CodexError {
            status: 0,
            message: "WebSocket connect error: HTTP error: 502 Bad Gateway".to_string(),
            detail: None,
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocket,
        };

        let response = map_codex_error_to_response(&err);
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body.pointer("/error/message").and_then(|v| v.as_str()),
            Some("WebSocket connect error: HTTP error: 502 Bad Gateway")
        );
    }

    #[tokio::test]
    async fn empty_live_completion_maps_to_explicit_service_unavailable() {
        let err = empty_live_completion_error();

        assert_eq!(err.status, 503);
        assert_eq!(err.detail.as_deref(), Some(EMPTY_CODEX_COMPLETION_DETAIL));
        assert_eq!(
            map_codex_error_to_response(&err).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn live_start_statusless_websocket_handshake_error_is_retryable() {
        let err = client::CodexError {
            status: 0,
            message: "WebSocket connect timeout after 15000ms".to_string(),
            detail: None,
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocketHandshake,
        };

        assert!(retryable_live_start_codex_error(&err));
    }

    #[test]
    fn live_start_proxy_tunnel_rejection_is_not_retryable() {
        let err = client::CodexError {
            status: 0,
            message: "WebSocket proxy tunnel was rejected".to_string(),
            detail: Some(websocket::WEBSOCKET_PROXY_TUNNEL_REJECTED_DETAIL.to_string()),
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocketHandshake,
        };

        assert!(!retryable_live_start_codex_error(&err));
    }

    #[test]
    fn live_start_payload_retry_detection_covers_rate_limit_and_overload() {
        assert!(retryable_live_start_payload(
            &serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {"limit_reached": true}
            }),
            "rate limit reached",
        ));
        assert!(retryable_live_start_payload(
            &serde_json::json!({
                "type": "response.failed",
                "response": {"error": {"type": "overloaded_error", "message": "overloaded"}}
            }),
            "overloaded",
        ));
        assert!(!retryable_live_start_payload(
            &serde_json::json!({
                "type": "response.failed",
                "response": {"error": {"message": "bad request"}}
            }),
            "bad request",
        ));
    }

    async fn run_live_failure_case(
        session_id: &str,
        event: serde_json::Value,
        expected_attempts: usize,
    ) -> StatusCode {
        let owner = ConversationIdentity::Main(session_id.to_string());
        continuation::clear_continuation_for_owner(Some(&owner));
        websocket::invalidate_codex_websocket_pool_owner(&owner);
        let request = live_test_request("one");
        let continuation = continuation_candidate_for_owner(Some(&owner), &request, true);
        let compaction_attempt = begin_compaction(session_id, &request.model);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..expected_attempts {
                let (socket, _) = listener.accept().await.unwrap();
                let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
                let _ = next_live_websocket_request(&mut websocket).await;
                emit_live_event(&mut websocket, &event).await;
                drop(websocket);
            }
        });
        let client = authenticated_live_test_client(format!("http://{addr}/responses"));
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            live_stream_response(
                client,
                "message".to_string(),
                &request.model,
                live_test_context(session_id),
                request.clone(),
                continuation.clone(),
                LiveStreamCompaction {
                    compact_boundary: false,
                    attempt: Some(compaction_attempt),
                },
                LiveStreamResources::default(),
                config::CodexTransport::WebSocket,
            ),
        )
        .await
        .expect("live failure case timed out");
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("live failure server timed out")
            .expect("live failure server failed");

        assert!(!continuation::is_current_turn_for_owner(&continuation));
        assert!(!store_compaction(
            session_id,
            compaction_attempt,
            Vec::new()
        ));
        websocket::invalidate_codex_websocket_pool_owner(&owner);
        response.status()
    }

    #[tokio::test]
    async fn dropping_live_stream_during_retry_backoff_aborts_request_state() {
        let _registry_guard = continuation::lock_continuation_registry_for_async_tests().await;
        let _pool_guard = websocket::lock_codex_websocket_pool_for_tests().await;
        let session_id = "live-retry-backoff-cleanup";
        let owner = ConversationIdentity::Main(session_id.to_string());
        continuation::clear_continuation_for_owner(Some(&owner));
        websocket::invalidate_codex_websocket_pool_owner(&owner);
        let request = live_test_request("one");
        let continuation = continuation_candidate_for_owner(Some(&owner), &request, true);
        let compaction_attempt = begin_compaction(session_id, &request.model);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (event_sent_tx, event_sent_rx) = tokio::sync::oneshot::channel();
        let (socket_closed_tx, socket_closed_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let _ = next_live_websocket_request(&mut websocket).await;
            emit_live_event(
                &mut websocket,
                &serde_json::json!({
                    "type": "codex.rate_limits",
                    "rate_limits": {"allowed": false, "limit_reached": true}
                }),
            )
            .await;
            event_sent_tx.send(()).unwrap();
            drop(websocket);
            socket_closed_tx.send(()).unwrap();
        });
        let client = authenticated_live_test_client(format!("http://{addr}/responses"));
        let task_request = request.clone();
        let task_continuation = continuation.clone();
        let response_task = tokio::spawn(async move {
            let model = task_request.model.clone();
            live_stream_response(
                client,
                "message".to_string(),
                &model,
                live_test_context(session_id),
                task_request,
                task_continuation,
                LiveStreamCompaction {
                    compact_boundary: false,
                    attempt: Some(compaction_attempt),
                },
                LiveStreamResources::default(),
                config::CodexTransport::WebSocket,
            )
            .await
        });

        event_sent_rx.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), socket_closed_rx)
            .await
            .expect("retry handoff did not close the abandoned attempt socket")
            .expect("retry handoff socket-close sender dropped");
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(
            !response_task.is_finished(),
            "logical request must still be waiting in retry backoff"
        );
        response_task.abort();
        assert!(response_task.await.unwrap_err().is_cancelled());

        assert!(!continuation::is_current_turn_for_owner(&continuation));
        assert!(!store_compaction(
            session_id,
            compaction_attempt,
            Vec::new()
        ));
        server.await.unwrap();
        websocket::invalidate_codex_websocket_pool_owner(&owner);
    }

    #[tokio::test]
    async fn dropping_live_response_body_after_first_chunk_aborts_request_state() {
        let _registry_guard = continuation::lock_continuation_registry_for_async_tests().await;
        let _pool_guard = websocket::lock_codex_websocket_pool_for_tests().await;
        let session_id = "live-response-body-drop-cleanup";
        let owner = ConversationIdentity::Main(session_id.to_string());
        continuation::clear_continuation_for_owner(Some(&owner));
        websocket::invalidate_codex_websocket_pool_owner(&owner);
        let request = live_test_request("one");
        let continuation = continuation_candidate_for_owner(Some(&owner), &request, true);
        let compaction_attempt = begin_compaction(session_id, &request.model);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (socket_closed_tx, socket_closed_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let _ = next_live_websocket_request(&mut websocket).await;
            emit_live_event(
                &mut websocket,
                &serde_json::json!({
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": {"type": "message", "id": "msg-partial"}
                }),
            )
            .await;
            emit_live_event(
                &mut websocket,
                &serde_json::json!({
                    "type": "response.output_text.delta",
                    "output_index": 0,
                    "delta": "partial"
                }),
            )
            .await;
            while websocket.next().await.is_some() {}
            socket_closed_tx.send(()).unwrap();
        });
        let client = authenticated_live_test_client(format!("http://{addr}/responses"));

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            live_stream_response(
                client,
                "message".to_string(),
                &request.model,
                live_test_context(session_id),
                request.clone(),
                continuation.clone(),
                LiveStreamCompaction {
                    compact_boundary: false,
                    attempt: Some(compaction_attempt),
                },
                LiveStreamResources::default(),
                config::CodexTransport::WebSocket,
            ),
        )
        .await
        .expect("live response did not publish the first chunk");
        let mut body = response.into_body();
        tokio::time::timeout(std::time::Duration::from_secs(1), body.frame())
            .await
            .expect("first downstream chunk timed out")
            .expect("live response body ended before the first chunk")
            .expect("first downstream chunk failed");
        drop(body);

        tokio::time::timeout(std::time::Duration::from_secs(1), socket_closed_rx)
            .await
            .expect("dropping the downstream body did not close the upstream socket")
            .expect("socket-close acknowledgement sender dropped");
        assert!(!continuation::is_current_turn_for_owner(&continuation));
        assert!(!store_compaction(
            session_id,
            compaction_attempt,
            Vec::new()
        ));
        server.await.unwrap();
        websocket::invalidate_codex_websocket_pool_owner(&owner);
    }

    #[tokio::test]
    async fn stale_request_cleanup_preserves_newer_turn_and_compaction_attempt() {
        let _registry_guard = continuation::lock_continuation_registry_for_async_tests().await;
        let session_id = "stale-live-request-cleanup";
        let owner = ConversationIdentity::Main(session_id.to_string());
        continuation::clear_continuation_for_owner(Some(&owner));
        let request = live_test_request("one");
        let stale_continuation = continuation_candidate_for_owner(Some(&owner), &request, true);
        let stale_compaction = begin_compaction(session_id, &request.model);
        let stale_cleanup = live_test_cleanup(
            stale_continuation,
            Some(session_id),
            LiveStreamCompaction {
                compact_boundary: false,
                attempt: Some(stale_compaction),
            },
            &request,
        );

        let newer_continuation = continuation_candidate_for_owner(Some(&owner), &request, true);
        let newer_compaction = begin_compaction(session_id, &request.model);
        drop(stale_cleanup);

        assert!(continuation::is_current_turn_for_owner(&newer_continuation));
        assert!(store_compaction(session_id, newer_compaction, Vec::new()));
        abort_request_state(
            Some(session_id),
            &newer_continuation,
            Some(newer_compaction),
        );
    }

    #[tokio::test]
    async fn retry_exhaustion_aborts_live_request_state_after_eleven_attempts() {
        let _registry_guard = continuation::lock_continuation_registry_for_async_tests().await;
        let _pool_guard = websocket::lock_codex_websocket_pool_for_tests().await;
        let status = run_live_failure_case(
            "live-retry-exhaustion-cleanup",
            serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {
                    "allowed": false,
                    "limit_reached": true,
                    "primary": {"reset_after_seconds": 0}
                }
            }),
            11,
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn excessive_retry_after_aborts_live_request_state() {
        let _registry_guard = continuation::lock_continuation_registry_for_async_tests().await;
        let _pool_guard = websocket::lock_codex_websocket_pool_for_tests().await;
        let status = run_live_failure_case(
            "live-excessive-retry-after-cleanup",
            serde_json::json!({
                "type": "codex.rate_limits",
                "rate_limits": {
                    "allowed": false,
                    "limit_reached": true,
                    "primary": {"reset_after_seconds": 31}
                }
            }),
            1,
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn nonretryable_live_error_aborts_request_state() {
        let _registry_guard = continuation::lock_continuation_registry_for_async_tests().await;
        let _pool_guard = websocket::lock_codex_websocket_pool_for_tests().await;
        let status = run_live_failure_case(
            "live-nonretryable-cleanup",
            serde_json::json!({
                "type": "response.failed",
                "response": {
                    "status": "failed",
                    "error": {"message": "invalid request"}
                }
            }),
            1,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn cancellation_while_replacement_startup_is_blocked_aborts_request_state() {
        let _registry_guard = continuation::lock_continuation_registry_for_async_tests().await;
        let _pool_guard = websocket::lock_codex_websocket_pool_for_tests().await;
        let session_id = "live-blocked-replacement-cleanup";
        let owner = ConversationIdentity::Main(session_id.to_string());
        continuation::clear_continuation_for_owner(Some(&owner));
        websocket::invalidate_codex_websocket_pool_owner(&owner);
        let request = live_test_request("one");
        let continuation = continuation_candidate_for_owner(Some(&owner), &request, true);
        let compaction_attempt = begin_compaction(session_id, &request.model);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (replacement_accepted_tx, replacement_accepted_rx) = tokio::sync::oneshot::channel();
        let (release_replacement_tx, release_replacement_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (first_socket, _) = listener.accept().await.unwrap();
            let mut first_websocket = tokio_tungstenite::accept_async(first_socket).await.unwrap();
            let _ = next_live_websocket_request(&mut first_websocket).await;
            emit_live_event(
                &mut first_websocket,
                &serde_json::json!({
                    "type": "codex.rate_limits",
                    "rate_limits": {
                        "allowed": false,
                        "limit_reached": true,
                        "primary": {"reset_after_seconds": 0}
                    }
                }),
            )
            .await;
            drop(first_websocket);

            let (_replacement_socket, _) = listener.accept().await.unwrap();
            replacement_accepted_tx.send(()).unwrap();
            let _ = release_replacement_rx.await;
        });
        let client = authenticated_live_test_client(format!("http://{addr}/responses"));
        let task_request = request.clone();
        let task_continuation = continuation.clone();
        let response_task = tokio::spawn(async move {
            let model = task_request.model.clone();
            live_stream_response(
                client,
                "message".to_string(),
                &model,
                live_test_context(session_id),
                task_request,
                task_continuation,
                LiveStreamCompaction {
                    compact_boundary: false,
                    attempt: Some(compaction_attempt),
                },
                LiveStreamResources::default(),
                config::CodexTransport::WebSocket,
            )
            .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(2), replacement_accepted_rx)
            .await
            .expect("replacement startup did not reach the blocked handshake")
            .expect("replacement startup acknowledgement sender dropped");
        response_task.abort();
        assert!(response_task.await.unwrap_err().is_cancelled());
        let _ = release_replacement_tx.send(());
        server.await.unwrap();

        assert!(!continuation::is_current_turn_for_owner(&continuation));
        assert!(!store_compaction(
            session_id,
            compaction_attempt,
            Vec::new()
        ));
        websocket::invalidate_codex_websocket_pool_owner(&owner);
    }
}
