# Security Policy

## Supported versions

Only the latest public baseline receives security fixes. A report should identify the
affected version, minimal reproduction, expected and actual behavior, and impact. Never
attach a real credential, complete transcript, production log, or private infrastructure
detail.

## Private reports

After the GitHub repository is created, maintainers should enable Private vulnerability
reporting. Submit reports through `Security` → `Report a vulnerability`. Do not disclose
exploitable details publicly before that channel is available.

If private reporting has not yet been enabled, open an ordinary issue without exploit
details and ask the maintainers to establish a private contact channel.

## Threat boundary

- The bridge is a loopback development component, not a public authentication gateway.
  Do not bind it directly to a public interface.
- A bearer-token file must be regular, non-symlink, non-empty, single-line, and mode
  `0600` on Unix.
- Runtime state, Hook sidecars, requests, responses, and diagnostics may contain sensitive
  context and must never be committed.
- Claude Code owns tool approvals and side effects. Any path that bypasses or duplicates
  its approval lifecycle is a security defect.
- Incorrect success acknowledgement of compaction state is a context-integrity failure
  and should be treated as a data-integrity defect.
- The operator supplies endpoint and model access. Claudex grants no right to use a
  third-party service or subscription.

## Coordinated disclosure

Coordinate public disclosure after a fix exists and affected versions are understood.
Security advisories should use synthetic data and clearly distinguish source behavior,
test evidence, and actual deployment observations.
