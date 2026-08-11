# Claudex Architecture

## Goal and boundary

Claudex treats Claude Code as a harness, not as a chat client to replace. Claude Code
continues to own tool execution, approval, agent lifecycle, parent/child context, and the
interactive interface. GPT Responses owns model reasoning. The bridge translates the
protocol and maintains only the state required for that translation.

`max{Claude Code, Codex}` is a layered design rule: retain Claude Code's native behavior
at the harness layer and preserve Responses semantics at the model/protocol layer. It
does not merge two permission systems, state stores, or executors into a new framework,
and it is not a performance benchmark claim.

## Data path

```text
User
  ↓
Claude Code 2.1.222
  ├─ settings / tools / agents / system prompt
  ├─ Hook: admission, budgets, results, and task gates
  └─ native tool loop, permissions, and agent scheduling
  ↓ Anthropic Messages + SSE
Rust bridge
  ├─ request identity and lanes
  ├─ Messages → Responses
  ├─ reasoning / tools / Web Search compatibility
  ├─ inline compaction and durable commit
  └─ Responses SSE → Anthropic SSE
  ↓
Operator-authorized Responses-compatible endpoint
```

The public version has no tunnel, production service unit, or provider-specific network
topology. Endpoint and credential are explicit external boundaries and never become part
of Claudex scheduling or recovery state.

## Claude Code control plane

Claudex does not patch the Claude Code binary. `claude-code/profile/` freezes the
external control plane validated together with the client version:

- `settings.json`: sandbox, narrow permissions, and Hook event wiring.
- `agents.json`: only official Claude Code Agent fields.
- `claudex-agent-policy.json`: work budgets and result contracts that must not be
  disguised as official fields.
- `harness.md`: when main may delegate, the required task/result contract, and when to
  stop.
- `hooks/claudex_hook.py`: a synthetic-testable policy state machine that does not parse
  or store the transcript.
- `claudex-tools-2.1.222.txt`: raw tool inventory aligned with the target client.

Default role layering:

| Role | Default model | Work + finalization | Responsibility |
|---|---|---:|---|
| main | Sol / xhigh | Claude Code main loop | Implementation, synthesis, and final delivery |
| Explore | Terra / max | 12 + 2 | Bounded discovery only when the target is unknown |
| Plan | Sol / max | 10 + 2 | Plan from accepted facts |
| Verify | Sol / max | 16 + 2 | Read-only verification against acceptance criteria |
| Forensic | Sol / max | 32 + 2 | Deep investigation only when explicitly selected |

The launcher's main model, effort, and permission mode remain explicit overrides scoped
to one session. Agent role models are fixed in the profile so a single Agent call cannot
cross an audited policy boundary.

### Why `maxTurns` is not a sufficient budget

A physical `maxTurns` can stop a loop, but cannot explain whether the stop means success.
The Hook divides the total into a work budget and two finalization turns. After work is
exhausted, operational tools are denied while the agent still has an opportunity to
produce a structured result. Empty output, malformed output, one repair attempt, an
explicit extension, and failure delivery remain distinct states, so the parent never has
to infer “completed” from “stopped.”

A blocking Agent is forced into the foreground and its valid result enters the parent
context through native `PostToolUse(Agent)`. A non-blocking background Agent keeps Claude
Code's native notification behavior. Claudex adds no second mailbox or transcript.

## Bridge request semantics

The bridge uses Serde `RawValue` to retain JSON fragments that it cannot safely
interpret, preventing future fields from being lost during deserialize/serialize cycles.
The main translations are:

- Anthropic message content → Responses input items.
- Tool definitions, tool use, and tool results → function tools/calls/outputs.
- Responses reasoning summaries → Claude thinking blocks, while encrypted continuation
  remains opaque.
- Hosted and forced Web Search → server tool/result blocks recognized by Claude Code.
- Responses SSE lifecycle → one legal Anthropic terminal sequence.

Tool compatibility changes must remain narrow. The current `Grep` fix preserves original
fields and optionality, rejects unknown top-level fields, and directs the model to
`context`. It does not silently rewrite an unknown argument, nor enable global
`strict=true`.

## Identity, lanes, and concurrency

Main and each direct Agent in one Claude Code session require stable, separate lanes.
Requests with absent or ambiguous identity may use a stateless path, but cannot enter a
continuation or compaction path that requires exact ownership.

A stable lane is serialized before it requests session and process permits. Public
defaults are:

- Per-session upstream concurrency: `4`.
- Process-global capacity: `3`.
- Data capacity: `2`, for main, workers, and Web Search.
- Control capacity: `1`, for the hidden reviewer.
- Queue timeout: `300s`.

Category permits are acquired before the global permit, so queued data work cannot hold
the reviewer's reserved slot. A live lease remains held until the downstream consumer
actually receives the terminal; enqueueing an upstream terminal is not completion.

The hidden reviewer is fixed to low effort and publishes no continuation or compaction
state. If it is unavailable, the request fails closed instead of falling back to the main
work model and silently changing the control request's cost and semantics.

## Inline compaction

The ordinary HTTP Responses path uses this contract:

1. `store:false`; no dependency on a server-side response chain.
2. No `previous_response_id`; input is reconstructed from the current transcript and
   local state.
3. Retain the session-derived `prompt_cache_key`; local replay does not disable upstream
   implicit caching.
4. Request context compaction and retain encrypted reasoning content.
5. If multiple compaction items exist, store only the latest item in output order and its
   exact raw suffix.
6. A startup retry reuses the same serialized body; it cannot silently fall back to the
   full transcript.
7. Commit only after the body ends, exactly one completed terminal exists, and no JSON
   payload follows it.

Successful ordering is:

```text
upstream terminal
  → reducer selects compaction and raw suffix
  → temporary file write + fsync
  → rename
  → parent directory fsync
  → continuity update
  → message_delta / message_stop
  → downstream consumes terminal
  → lease release
```

If directory fsync fails after rename, the canonical path may have changed even though a
durable commit cannot be proven. The bridge returns an indeterminate-durability error,
emits no success stop, performs no transparent retry or immediate rollback, and requires
the next request to recover with the full transcript. Crossing a usage threshold without
a real compaction item never counts as successful compaction.

The state directory has one writer. Writer-lock contention or errors fail closed. A
sidecar stores only the opaque recovery item, exact suffix, hashes, revision, and required
identity; it does not store a token, prompt, or assistant transcript.

## Frameworks and mechanisms

| Component | Purpose |
|---|---|
| Axum / Tower | Loopback HTTP server, routing, and test services |
| Tokio | Async runtime, networking, synchronization, queues, and timeouts |
| reqwest / rustls | Upstream HTTP, SSE, and TLS |
| Serde / `RawValue` | Typed boundaries and preservation of unknown JSON semantics |
| tokio-tungstenite | Upstream WebSocket transport |
| SHA-256 / UUID | State keys, evidence, and request-identity support |
| Python standard library | Hook state machine, file lock, atomic state, and policy validation |

## Explicit non-goals

- Do not execute tools or recreate Claude Code approvals in the bridge.
- Do not replace deterministic Hook gates with prompt-only conventions.
- Do not persist a one-session override as a long-term default.
- Do not silently fall back from a failed reviewer to another model.
- Do not trim a transcript merely because token usage crossed a threshold.
- Do not turn the public repository into a credential distributor, subscription-sharing
  service, or public proxy.
