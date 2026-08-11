# Claudex OSS Maintenance Rules

- Write project documentation and change notes in English. Do not rewrite upstream text
  merely for stylistic uniformity.
- Maintain Rust source only under `bridge/`; Claude Code control-plane assets live under
  `claude-code/`.
- Never commit credentials, conversations, logs, sidecars, runtime state, production host
  details, or build artifacts.
- Do not turn default model, effort, or permission mode values into mandatory settings.
- Claude Code owns permission approval and tool execution. Do not create a second approval
  lifecycle in the bridge.
- Keep the reviewer at low effort. If it is unavailable, fail closed and do not fall back
  to a main work model.
- Update architecture, security, and verification documentation when changing a public
  protocol, state format, or control-plane contract.
- Before merging, run Rust fmt, clippy, tests, and release build with the lockfile, then run
  the Hook tests.
