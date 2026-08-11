# Claudex Minimal Execution Contract

- Call `Agent` only when the target is unknown and the work is genuinely independent.
  Use local tools directly for a known UUID, path, filename, or symbol. Run one worker at
  a time and never nest Agents.
- `Agent.prompt` must contain one line:
  `CLAUDEX_TASK_CONTRACT={"goal":"...","scope":"...","acceptance_criteria":["..."],"stop_condition":"...","blocking":true,"resources":["/absolute/path"],"task_id":"optional-native-task-id"}`.
  `resources` may be omitted. Include `task_id` only when the Agent explicitly serves a
  tracked blocking task; every other field is required. Never add an approval that the
  user did not grant.
- A custom agent's final line must be the `CLAUDEX_AGENT_RESULT={...}` required by its
  definition. Return `needs_extension` only when a decision-critical unknown remains and
  the next evidence is explicit. The control plane permits at most one bounded extension.
- Only main may invoke a Skill, and the current user prompt must explicitly contain
  `$skill-name`, `/skill-name`, or `CLAUDEX_SKILLS=["skill-name"]`. Never load a Skill
  merely because it may be relevant.
- If a Task is used, its initial description must include
  `CLAUDEX_TASK_POLICY={"blocking":true,"tests_required":true,"required_artifacts":["..."],"verifier_required":true,"resources":["/absolute/path"]}`.
  Before completion, update the description with
  `CLAUDEX_TASK_RESULT={"acceptance_criteria_closed":true,"tests":"passed|not_required","required_artifacts":"present|not_required","blocking_verifier":"passed|not_required","evidence_sha256":"64-lowercase-hex-sha256"}`.
  Never delete an unfinished or untracked task to bypass the gate.
- `rm`, `rmdir`, `unlink`, `git gc/prune/clean`, or overwriting a backup/snapshot is
  allowed only when the current user prompt already provides
  `CLAUDEX_RESOURCE_LEASE={"resource":"/absolute/path","evidence_sha256":"64-lowercase-hex-sha256","approval":"destructive_change"}`,
  and main includes `# CLAUDEX_RESOURCE="/absolute/path"` in the Bash command. Never
  manufacture a user lease.
- Stop when acceptance criteria are met. The versioned policy owns work budgets. During
  finalization, call no operational tool and immediately produce the result contract.
  During final freeze, create no Agent and load no Skill. Close every active blocking
  worker and blocking task, then synthesize once; do not poll through the Stop hook.
  Non-blocking background Agents keep Claude Code's native completion notification and
  use no second mailbox.
