# Verification

Verification is divided into source behavior, the public release surface, and real
deployment evidence. This public repository provides the first two only. Unit tests must
not be described as production evidence.

## Rust bridge

Run from the repository root with the lockfile:

```bash
cargo fmt --check --manifest-path bridge/Cargo.toml
cargo clippy --manifest-path bridge/Cargo.toml --locked --all-targets -- -D warnings
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
  cargo test --manifest-path bridge/Cargo.toml --locked -- --test-threads=1
cargo build --manifest-path bridge/Cargo.toml --release --locked
```

These checks cover compilation, lint, unit/integration regressions, and release build.
Protocol and state tests use local servers, synthetic SSE, and temporary directories.
They do not access a real endpoint or credential.

Result for this public snapshot on `2026-08-11`: fmt passed, clippy with `-D warnings`
passed, Rust reported `1009 passed / 0 failed / 1 ignored`, and release build passed. The
tests explicitly bypassed environment proxies and ran single-threaded. A parallel run
observed one transient Cursor resume fixture timing failure; its focused rerun and the
final single-threaded full suite passed, so the parallel run is not recorded as a clean
pass. The ignored test is a child-process entry used by its parent crash-recovery matrix.

## Claude Code control plane

```bash
bash -n claudex
python3 -m json.tool claude-code/profile/settings.json >/dev/null
python3 -m json.tool claude-code/profile/agents.json >/dev/null
python3 -m json.tool claude-code/profile/claudex-agent-policy.json >/dev/null
python3 -B claude-code/profile/hooks/claudex_hook.py \
  --validate-profile \
  claude-code/profile/agents.json \
  claude-code/profile/claudex-agent-policy.json
python3 -B -m unittest claude-code/tests/test_harness.py
```

Hook tests write only to a temporary directory and use synthetic session, Agent, task,
and tool events. They validate the control-plane state machine, not compatibility with an
unlisted Claude Code version. A client upgrade requires recapturing the raw tool and Hook
contracts.

Result for this public snapshot on `2026-08-11`: profile validation passed, all `14/14`
Hook tests passed, and launcher shell syntax, `version`, and `models` output passed.

## Public release checks

```bash
git diff --cached --check
git ls-files
find . -type d -name target -prune -o -type f -print
```

Run an independent secret scanner against the worktree and the Git objects that will be
pushed, followed by maintainer review for organization-specific identifiers. The public
tree must not contain the parent repository history or local application material under
`.release/`.

## Claims not established by this repository

- Availability, model access, quota, or terms for any third-party endpoint.
- Compatibility with Claude Code versions not listed here.
- Long-term production stability, cost, or real concurrent throughput.
- A performance ranking implied by `max{Claude Code, Codex}`.

Those claims require separate live evidence or a public benchmark and cannot be inferred
from the current unit tests.
