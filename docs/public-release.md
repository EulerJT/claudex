# Public Release Policy

## Why the public repository needs clean history

The private maintenance repository contains deployment audits and immutable provenance.
Deleting files from its latest worktree would not remove them from old commits, tags,
blobs, release assets, or forks. The public repository must therefore start from this
directory as a new repository. Never push the parent `.git` history or export this tree
as a history-preserving subtree.

## Content allowed in the public repository

- Buildable Rust bridge source, lockfile, and tests.
- Versioned Claude Code profile, Hook, policy, and synthetic tests.
- Generic launcher and architecture, security, contribution, and verification docs.
- Upstream licenses, attribution, and synthetic fixtures.

## Content prohibited from the public repository

- Production launchers, service units, tunnel configuration, deployment snapshots, or
  internal provenance.
- Real hostnames, IPs, port layouts, usernames, home-directory paths, SSH information,
  or provider-private identities.
- Tokens, keys, cookies, sessions, logs, transcripts, sidecars, runtime state, or backups.
- Production binaries, source archives, `target/`, or objects from the private history.
- Internal application notes that the submission process does not need and does not
  promise to keep confidential.

## Release procedure

This directory already has an independent Git repository. Before the first commit:

```bash
git status --short
git diff --cached --check
git ls-files
```

Confirm that `.release/`, credentials, runtime state, and `target/` are absent. Then create
the first commit, add the intended GitHub remote, and push only this repository:

```bash
git commit -m "release: publish Claudex 1.0.0"
git remote add origin git@github.com:OWNER/claudex.git
git push -u origin main
```

Do not rewrite the parent repository and do not copy its `.git/` directory.

## Pre-publish audit

1. Run the build and tests in [Verification](verification.md).
2. Run an independent secret scanner on both the public worktree and the Git objects that
   will be pushed.
3. Search for production hosts, user paths, private provider names, historical ports, and
   organization-specific identifiers.
4. Inspect `git ls-files` and confirm that ignored files were not force-added.
5. Review LICENSE, NOTICE, SECURITY, README, and contribution guidance.
6. Review the default branch, release assets, Actions logs, and issue settings on GitHub.

A passing scan means only that its current rules found nothing. A maintainer who knows
the private infrastructure must still perform the final review.

## Suggested GitHub settings

- Description: `Run GPT Responses inside the Claude Code harness with protocol-correct tools, agents, streaming, and compaction.`
- Topics: `claude-code`, `codex`, `openai`, `responses-api`, `rust`, `coding-agents`.
- Enable branch protection, required CI, Dependabot, and Private vulnerability reporting.
- Publish source tags only until reproducible builds and artifact signing are available.
