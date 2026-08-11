# Claudex

**Claudex is a compatibility and control plane for running GPT Responses inside the
Claude Code harness—without reducing the integration to a model alias and a changed
base URL.**

The goal is not merely to make GPT answer inside Claude Code. It is to preserve GPT's
Responses reasoning semantics while retaining Claude Code's strongest harness
features: tool execution, permissions, hooks, subagents, editor integration, and
parent/child result delivery.

We summarize that engineering target as `max{Claude Code, Codex}`: keep the stronger
mechanism at each layer. This is a design objective, not an unverified benchmark claim.

The current public baseline is `1.0.0`, with a versioned control plane for Claude Code
`2.1.222` and a Rust bridge snapshot. The public repository contains no production
deployment snapshot, private network topology, internal provenance, credential, log,
conversation, or runtime state.

## Why Claudex matters

A common online recommendation is to run a compatible API gateway—such as
[CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI)—point
`ANTHROPIC_BASE_URL` at it, map a Claude model name to a GPT model, and start Claude
Code. That is a useful connectivity test. CLIProxyAPI itself documents compatible API
surfaces, provider routing, tools, streaming, and multiple accounts.

But an API gateway and a coding-agent harness solve different problems. A gateway can
route and translate traffic without owning Claude Code's agent lifecycle, transcript,
permission state, tool-result delivery, or compaction boundary. In long, tool-heavy
coding sessions, the missing harness layer becomes the actual failure surface.

These are representative failure modes of the gateway-only setup:

| Shortcut symptom | Underlying mechanism | Claudex response |
|---|---|---|
| Text chat works, so the integration is declared complete | A single turn does not exercise tool-call/result pairing, reasoning replay, or terminal ordering | Test the complete multi-turn agent loop and fail closed on malformed protocol state |
| The context grows until the upstream returns HTTP 400, then the session appears stuck | Claude Code's transcript boundary and the Responses compaction boundary are not coordinated; retrying the same oversized body cannot create a valid lineage | Request native inline compaction, persist the opaque item and exact suffix, and only acknowledge success after a durable commit |
| One `Explore` subagent keeps searching, over-engineers the task, exhausts its physical turn limit, or returns no useful result | `maxTurns` only stops execution; it does not define admission, work budget, finalization budget, result validity, or parent delivery | Enforce one worker, bounded work turns, two reserved finalization turns, a structured result contract, and explicit failure delivery |
| Main, worker, Web Search, and reviewer interfere with one another | Session identity alone is not a sufficient ownership key, and one global semaphore can starve control traffic | Give main and direct agents separate lanes and reserve independent data/control capacity |
| A tool works once but later loops or is rejected | Messages and Responses do not have identical tool schemas or call/result lifecycle rules | Preserve tool identity and pairing, and apply only narrow, reproduced schema fixes |
| Claude Code receives a success stop but the next turn has lost state | The bridge emitted the downstream terminal before compaction state became durable | Require file fsync, rename, and directory fsync before the success terminal |
| Permissions become inconsistent or approvals repeat | A bridge-side approval layer competes with Claude Code's native permission lifecycle | Keep all approval and tool side effects inside Claude Code |

These are not claims that CLIProxyAPI inherently contains each defect. They are the
failure modes that remain when any protocol gateway is used as the *entire* Claude Code
integration. Claudex supplies the missing bridge/harness contract.

See [Why gateway-only tutorials fail](docs/why-naive-proxies-fail.md) for the detailed
mechanisms and minimum acceptance tests.

## Architecture at a glance

| Layer | Owner |
|---|---|
| Claude Code | Native tool loop, permission approval, agent scheduling, parent/child result delivery, and UI |
| Versioned profile | Tool inventory, model roles, sandbox, hooks, and session defaults |
| Python Hook | Agent admission, work/finalization budgets, result contracts, task gates, and destructive-resource gates |
| Rust bridge | Messages→Responses translation, SSE, reasoning/tool pairing, inline compaction, and capacity isolation |
| Upstream boundary | A Responses-compatible endpoint and credential the operator is authorized to use |

Claudex does not patch the Claude Code binary. Its Claude Code adaptation uses public
settings, agent definitions, hook events, and launch arguments, so the control plane can
be audited and revalidated for each client version.

Read the [architecture](docs/architecture.md) for the complete state and ownership model.

## Repository layout

```text
bridge/                 Rust bridge: protocol, streaming, state, and capacity
claude-code/profile/    Settings, agents, policy, Hook, and tool inventory for CC 2.1.222
claude-code/tests/      Synthetic Hook regression tests
docs/                   Architecture, failure analysis, security, and verification
claudex                 Minimal public launcher; it does not manage services
```

`bridge/` is based on the open-source
[claude-code-proxy](https://github.com/raine/claude-code-proxy) project and retains its
MIT license. See [NOTICE](NOTICE) for attribution and modification scope.

## Technology

- Rust 2024, Tokio, Axum, reqwest, and Serde/`RawValue` for async HTTP/SSE and exact
  preservation of unknown JSON items.
- Tokio semaphores, mutexes, and channels for lane serialization, per-session permits,
  and data/control capacity isolation.
- Atomic rename, file/directory fsync, and a writer lock for fail-closed compaction state.
- Python 3 standard library for the Claude Code Hook state machine and policy validation.
- Bash for a minimal launcher that wires versioned assets without managing production
  services.

## Quick verification

You need a Rust toolchain, Python 3, Bash, and a separately installed Claude Code
`2.1.222`.

```bash
cargo fmt --check --manifest-path bridge/Cargo.toml
cargo clippy --manifest-path bridge/Cargo.toml --locked --all-targets -- -D warnings
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
  cargo test --manifest-path bridge/Cargo.toml --locked -- --test-threads=1
python3 -B -m unittest claude-code/tests/test_harness.py
```

The verified public snapshot passes `1009` Rust tests with `1` ignored child-process
entry and all `14` Hook tests. Exact scope and caveats are recorded in
[Verification](docs/verification.md).

## Running the public example

Before starting the bridge, configure a Responses-compatible endpoint that you are
authorized to use. The documented public mode reads exactly one bearer token from a
regular `0600` file. Symlinks, empty files, multiline content, and broader Unix
permissions fail closed.

```bash
install -m 600 /dev/null "$HOME/.config/claudex/bearer.token"
# Write an authorized token to that file. Never commit it.

CCP_CODEX_AUTH_MODE=bearer_file \
CCP_CODEX_BEARER_TOKEN_FILE="$HOME/.config/claudex/bearer.token" \
CCP_CODEX_BASE_URL="https://your-authorized-endpoint.example/v1/responses" \
CCP_CODEX_TRANSPORT=http \
  cargo run --locked --manifest-path bridge/Cargo.toml -- serve
```

After the local bridge is healthy, start Claude Code in another terminal:

```bash
./claudex --model gpt-5.6-sol --effort xhigh --permission-mode auto
```

The launcher does not start, stop, or probe remote services. It does not acquire,
refresh, share, or bypass a third-party subscription. Credential origin, model access,
and endpoint compliance remain the operator's responsibility. Inherited upstream OAuth
capabilities are not part of the documented Claudex deployment contract.

## Current engineering contracts

- HTTP Responses requests use `store:false`, do not depend on `previous_response_id`,
  and retain a session-derived `prompt_cache_key`.
- Inline compaction stores only the latest opaque compaction item and its exact raw
  suffix; the success terminal is emitted only after a durable commit.
- Main, worker, and Web Search traffic use data capacity; the hidden reviewer uses
  control capacity so completion control cannot be starved by ordinary work.
- The reviewer is fixed to low effort and fails closed; it never falls back to the main
  work model.
- Agents have separate work and two-turn finalization budgets. Empty output is not
  success, and a malformed structured result receives at most one repair attempt.
- `Grep` receives a narrow schema compatibility fix without silently rewriting unknown
  arguments or enabling global strict mode.
- Main model, effort, and permission mode remain explicit per-session overrides; one
  session never rewrites persistent defaults.

## Security and scope

- Bind the bridge to loopback. It is a development component, not a public
  authentication gateway.
- The repository does not provide subscription acquisition, sharing, probing, retries
  around service restrictions, or quota bypass.
- Never attach credentials, real transcripts, production logs, or private infrastructure
  details to an issue, pull request, diagnostic artifact, or program application.
- Claude Code, model, and endpoint usage must independently comply with their applicable
  licenses and service terms.

See [SECURITY.md](SECURITY.md) and the [public release policy](docs/public-release.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Bug reports should use synthetic, minimal
reproductions and must not include real transcripts, request bodies, logs, or tokens.

## License

Claudex-owned content is available under the [MIT License](LICENSE). Upstream copyright
and license terms for `bridge/` remain in [bridge/LICENSE](bridge/LICENSE); see
[NOTICE](NOTICE).
