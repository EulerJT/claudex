# Contributing to Claudex

Issues and pull requests are welcome when they improve protocol correctness, the Claude
Code control plane, verifiability, or documentation clarity.

## Contribution boundaries

- Describe the observable defect, its mechanism, and a minimal reproduction before
  proposing an implementation.
- Prefer the existing ownership layers. Do not add a second executor, transcript, or
  bridge-side approval framework.
- Endpoint access, credentials, and subscription acquisition are outside project scope.
  Do not submit provider-private implementations or real authentication material.
- Do not commit logs, conversations, prompts, assistant output, sidecars, build artifacts,
  or machine-specific paths.
- Write project documentation and change notes in English, while retaining upstream
  licenses and necessary source identifiers.

## Local checks

```bash
cargo fmt --check --manifest-path bridge/Cargo.toml
cargo clippy --manifest-path bridge/Cargo.toml --locked --all-targets -- -D warnings
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
  cargo test --manifest-path bridge/Cargo.toml --locked -- --test-threads=1
cargo build --manifest-path bridge/Cargo.toml --release --locked
bash -n claudex
python3 -B claude-code/profile/hooks/claudex_hook.py \
  --validate-profile \
  claude-code/profile/agents.json \
  claude-code/profile/claudex-agent-policy.json
python3 -B -m unittest claude-code/tests/test_harness.py
git diff --check
```

One clean pass is enough for a focused deterministic change. Public protocol, state,
concurrency, authentication, or data-integrity changes require tests for their relevant
failure paths. List any check you could not run in the pull request.

## Pull request description

Please include:

1. The contract being fixed or added.
2. Why the existing behavior is insufficient.
3. Directly affected files and compatibility boundaries.
4. Validation commands and results.
