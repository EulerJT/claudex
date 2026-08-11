#!/usr/bin/env python3
"""Deterministic Claudex admission, completion, freeze, and resource gates."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shlex
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
ALLOWED_AGENT_TYPES = {"Explore", "Plan", "Verify", "Forensic"}
AGENT_POLICY_FILENAME = "claudex-agent-policy.json"
OFFICIAL_AGENT_FIELDS = {
    "background",
    "color",
    "description",
    "disallowedTools",
    "effort",
    "hooks",
    "isolation",
    "maxTurns",
    "mcpServers",
    "memory",
    "model",
    "permissionMode",
    "prompt",
    "skills",
    "tools",
}
TERMINAL_TASK_STATUSES = {"completed", "failed", "stopped", "done", "cancelled"}
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
EXPANSION_RE = re.compile(r"[\*?\[\]\$`(){}]")


class HookBlock(Exception):
    pass


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def positive_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def validate_agent_policy(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or raw.get("schema_version") != 1:
        raise ValueError("unsupported Claudex agent policy schema")
    profiles = raw.get("profiles")
    if not isinstance(profiles, dict) or set(profiles) != ALLOWED_AGENT_TYPES:
        raise ValueError("agent policy must define exactly the admitted Claudex roles")
    normalized: dict[str, Any] = {"schema_version": 1, "profiles": {}}
    for agent_type in sorted(ALLOWED_AGENT_TYPES):
        profile = profiles.get(agent_type)
        if not isinstance(profile, dict):
            raise ValueError(f"{agent_type} agent policy must be an object")
        work_budget = positive_integer(profile.get("work_turn_budget"), "work_turn_budget")
        reserve = positive_integer(profile.get("finalization_reserve"), "finalization_reserve")
        contract_version = positive_integer(
            profile.get("result_contract_version"), "result_contract_version"
        )
        denied_tools = sorted(
            set(string_list(profile.get("finalization_denied_tools"), "finalization_denied_tools"))
        )
        if not denied_tools:
            raise ValueError("finalization_denied_tools must not be empty")
        normalized["profiles"][agent_type] = {
            "work_turn_budget": work_budget,
            "finalization_reserve": reserve,
            "result_contract_version": contract_version,
            "finalization_denied_tools": denied_tools,
        }
    return normalized


def read_json_object(path: Path, field: str) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a JSON object")
    return value


def validate_profile_documents(agents: Any, policy_raw: Any) -> dict[str, Any]:
    if not isinstance(agents, dict) or set(agents) != ALLOWED_AGENT_TYPES:
        raise ValueError("agents.json must define exactly the admitted Claudex roles")
    policy = validate_agent_policy(policy_raw)
    for agent_type, definition in agents.items():
        if not isinstance(definition, dict):
            raise ValueError(f"{agent_type} agent definition must be an object")
        unknown = sorted(set(definition) - OFFICIAL_AGENT_FIELDS)
        if unknown:
            raise ValueError(f"{agent_type} contains non-official agent fields: {', '.join(unknown)}")
        physical_max = positive_integer(definition.get("maxTurns"), f"{agent_type}.maxTurns")
        profile = policy["profiles"][agent_type]
        expected = profile["work_turn_budget"] + profile["finalization_reserve"]
        if physical_max != expected:
            raise ValueError(
                f"{agent_type}.maxTurns must equal work_turn_budget + finalization_reserve ({expected})"
            )
    return policy


def load_agent_policy() -> dict[str, Any]:
    assets_value = os.environ.get("CLAUDEX_PROFILE_ASSETS_DIR", "")
    if not assets_value or not os.path.isabs(assets_value):
        raise ValueError("CLAUDEX_PROFILE_ASSETS_DIR must be an absolute path")
    return validate_agent_policy(
        read_json_object(Path(assets_value) / AGENT_POLICY_FILENAME, "Claudex agent policy")
    )


def agent_policy_snapshot(policy: dict[str, Any], agent_type: str) -> dict[str, Any]:
    profile = policy["profiles"].get(agent_type)
    if not isinstance(profile, dict):
        raise ValueError(f"no Claudex agent policy exists for {agent_type}")
    return {
        "policy_version": policy["schema_version"],
        "policy_hash": f"sha256:{stable_hash(policy)}",
        "work_turn_budget": profile["work_turn_budget"],
        "finalization_reserve": profile["finalization_reserve"],
        "physical_max_turns": profile["work_turn_budget"] + profile["finalization_reserve"],
        "result_contract_version": profile["result_contract_version"],
        "finalization_denied_tools": profile["finalization_denied_tools"],
    }


def now_seconds() -> int:
    return int(time.time())


def marker_values(text: str, name: str) -> list[Any]:
    pattern = re.compile(rf"(?m)^[ \t]*{re.escape(name)}[ \t]*=([^\r\n]+?)[ \t]*$")
    values = []
    for match in pattern.finditer(text):
        values.append(json.loads(match.group(1)))
    return values


def one_marker(text: str, name: str, *, required: bool) -> Any | None:
    values = marker_values(text, name)
    if not values and not required:
        return None
    if len(values) != 1:
        qualifier = "exactly one" if required else "at most one"
        raise ValueError(f"{name} requires {qualifier} single-line JSON marker")
    return values[0]


def normalize_absolute_path(value: Any) -> str:
    if not isinstance(value, str) or not value.strip() or not os.path.isabs(value):
        raise ValueError("resource paths must be non-empty absolute paths")
    return os.path.normpath(value)


def normalize_resources(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("resources must be an array")
    return sorted({normalize_absolute_path(item) for item in value})


def nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


def string_list(value: Any, field: str, *, allow_empty: bool = True) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be an array")
    items = [nonempty_string(item, field) for item in value]
    if not allow_empty and not items:
        raise ValueError(f"{field} must not be empty")
    return items


def default_state() -> dict[str, Any]:
    return {
        "schema": SCHEMA_VERSION,
        "revision": 0,
        "phase": "RUNNING",
        "prompt_generation": 0,
        "prompt_hash": None,
        "allowed_skills": [],
        "pending_agent": None,
        "agents": {},
        "tasks": {},
        "resource_leases": {},
        "freeze": {},
    }


class StateFile:
    def __init__(self, session_id: str):
        root_value = os.environ.get("CLAUDEX_HARNESS_STATE_DIR", "")
        if not root_value or not os.path.isabs(root_value):
            raise ValueError("CLAUDEX_HARNESS_STATE_DIR must be an absolute path")
        self.root = Path(root_value)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.root.is_symlink() or not self.root.is_dir():
            raise ValueError("Claudex harness state root must be a real directory")
        key = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
        self.path = self.root / f"{key}.json"
        self.lock_path = self.root / f"{key}.lock"
        flags = os.O_RDWR | os.O_CREAT
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        self.lock_fd = os.open(self.lock_path, flags, 0o600)
        os.chmod(self.lock_path, 0o600)

    def __enter__(self) -> tuple[dict[str, Any], "StateFile"]:
        fcntl.flock(self.lock_fd, fcntl.LOCK_EX)
        if not self.path.exists():
            return default_state(), self
        with self.path.open("r", encoding="utf-8") as handle:
            state = json.load(handle)
        if not isinstance(state, dict) or state.get("schema") != SCHEMA_VERSION:
            raise ValueError("unsupported Claudex harness state schema")
        return state, self

    def save(self, state: dict[str, Any]) -> None:
        state["revision"] = int(state.get("revision", 0)) + 1
        fd, temporary_name = tempfile.mkstemp(prefix=f".{self.path.stem}.", dir=self.root)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                fd = -1
                json.dump(state, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, self.path)
        except BaseException:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
            raise

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> None:
        fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
        os.close(self.lock_fd)


def pretool_deny(reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def pretool_allow_updated(tool_input: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "permissionDecisionReason": reason,
            "updatedInput": tool_input,
        }
    }


def hook_context(event: str, message: str) -> dict[str, Any]:
    return {"hookSpecificOutput": {"hookEventName": event, "additionalContext": message}}


def extract_explicit_skills(prompt: str) -> list[str]:
    skills = {
        match.group(1)
        for match in re.finditer(r"(?<![A-Za-z0-9_])(?:\$|/)([A-Za-z][A-Za-z0-9:_-]*)", prompt)
    }
    explicit = one_marker(prompt, "CLAUDEX_SKILLS", required=False)
    if explicit is not None:
        skills.update(string_list(explicit, "CLAUDEX_SKILLS"))
    return sorted(skills)


def handle_user_prompt(data: dict[str, Any], state: dict[str, Any]) -> None:
    prompt = nonempty_string(data.get("prompt"), "prompt")
    state["prompt_generation"] = int(state.get("prompt_generation", 0)) + 1
    state["prompt_hash"] = stable_hash(prompt)
    state["allowed_skills"] = extract_explicit_skills(prompt)
    state["phase"] = "RUNNING"
    state["freeze"] = {}

    leases: dict[str, Any] = {}
    for raw in marker_values(prompt, "CLAUDEX_RESOURCE_LEASE"):
        if not isinstance(raw, dict):
            raise ValueError("CLAUDEX_RESOURCE_LEASE must be an object")
        resource = normalize_absolute_path(raw.get("resource"))
        evidence_hash = nonempty_string(raw.get("evidence_sha256"), "evidence_sha256").lower()
        if not SHA256_RE.fullmatch(evidence_hash):
            raise ValueError("evidence_sha256 must be a lowercase SHA-256")
        if raw.get("approval") != "destructive_change":
            raise ValueError("resource lease approval must equal destructive_change")
        leases[resource] = {
            "resource": resource,
            "evidence_sha256": evidence_hash,
            "approval_generation": state["prompt_generation"],
            "approved_at": now_seconds(),
        }
    state["resource_leases"] = leases


def validate_agent_contract(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("CLAUDEX_TASK_CONTRACT must be an object")
    goal = nonempty_string(raw.get("goal"), "goal")
    scope = nonempty_string(raw.get("scope"), "scope")
    acceptance = string_list(raw.get("acceptance_criteria"), "acceptance_criteria", allow_empty=False)
    stop_condition = nonempty_string(raw.get("stop_condition"), "stop_condition")
    if not isinstance(raw.get("blocking"), bool):
        raise ValueError("blocking must be true or false")
    resources = normalize_resources(raw.get("resources"))
    if not resources and os.path.isabs(scope):
        resources = [os.path.normpath(scope)]
    task_id = raw.get("task_id")
    if task_id is not None:
        task_id = nonempty_string(task_id, "task_id")
    return {
        "goal_hash": stable_hash(goal),
        "scope_hash": stable_hash(scope),
        "acceptance_hash": stable_hash(acceptance),
        "stop_condition_hash": stable_hash(stop_condition),
        "blocking": raw["blocking"],
        "resources": resources,
        "task_key": stable_hash(task_id) if task_id else None,
    }


def active_agents(state: dict[str, Any]) -> list[dict[str, Any]]:
    return [agent for agent in state.get("agents", {}).values() if agent.get("active")]


def handle_agent_admission(data: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    if data.get("agent_id"):
        return pretool_deny("Nested Agent calls are disabled by the Claudex harness.")
    if state.get("phase") != "RUNNING":
        return pretool_deny("Claudex final freeze has started; new Agent calls are closed.")
    tool_input = data.get("tool_input") or {}
    if "model" in tool_input:
        return pretool_deny(
            "Claudex agent models are owned by the versioned role profile; per-call overrides are disabled."
        )
    agent_type = tool_input.get("subagent_type")
    if agent_type not in ALLOWED_AGENT_TYPES:
        return pretool_deny("Use one of the admitted Claudex agents: Explore, Plan, Verify, or Forensic.")
    if state.get("pending_agent"):
        return pretool_deny("One Claudex worker is already pending or active; wait for it to stop.")
    try:
        contract = validate_agent_contract(
            one_marker(str(tool_input.get("prompt", "")), "CLAUDEX_TASK_CONTRACT", required=True)
        )
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        return pretool_deny(f"Agent task contract rejected: {error}")
    task_reference = contract.get("task_key")
    if task_reference:
        task = state.get("tasks", {}).get(task_reference)
        if not contract["blocking"]:
            return pretool_deny("An Agent linked to a blocking task must itself be blocking.")
        if task is None or not task.get("blocking") or task.get("completed"):
            return pretool_deny("Agent task_id must name an unresolved blocking task tracked by this session.")
    requested_background = tool_input.get("run_in_background", False)
    if not isinstance(requested_background, bool):
        return pretool_deny("Agent run_in_background must be true or false.")
    scope_hash = contract["scope_hash"]
    for agent in active_agents(state):
        if agent.get("scope_hash") == scope_hash:
            return pretool_deny("An Agent with the exact same scope is already active.")
    if active_agents(state):
        return pretool_deny("One Claudex worker is already active; wait for it to stop.")
    state["pending_agent"] = {
        **contract,
        "tool_use_hash": stable_hash(data.get("tool_use_id", "")),
        "agent_type": agent_type,
        "admitted_at": now_seconds(),
        "cwd": normalize_absolute_path(data.get("cwd")),
        "launch_mode": "background" if requested_background else "foreground",
    }
    if contract["blocking"] and requested_background:
        updated_input = dict(tool_input)
        updated_input["run_in_background"] = False
        state["pending_agent"]["launch_mode"] = "foreground"
        return pretool_allow_updated(
            updated_input,
            "Blocking Claudex Agents run in the foreground so their terminal result is delivered synchronously.",
        )
    return None


def skill_name(tool_input: dict[str, Any]) -> str:
    raw = tool_input.get("skill") or tool_input.get("name") or tool_input.get("command") or ""
    return str(raw).strip().lstrip("/").split(maxsplit=1)[0]


def handle_skill(data: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    if data.get("agent_id"):
        return pretool_deny("Skills are available only to the Claudex main agent.")
    if state.get("phase") != "RUNNING":
        return pretool_deny("Skills cannot be loaded after final freeze begins.")
    requested = skill_name(data.get("tool_input") or {})
    if not requested or requested not in state.get("allowed_skills", []):
        return pretool_deny("The user must explicitly name this Skill in the current prompt before it can load.")
    return None


def command_segments(command: str) -> list[list[str]]:
    segments = re.split(r"(?:&&|\|\||[;\n|])", command)
    parsed = []
    for segment in segments:
        stripped = segment.strip().lstrip("(").strip()
        if not stripped or stripped.startswith("#"):
            continue
        try:
            tokens = shlex.split(stripped, comments=True, posix=True)
        except ValueError:
            if re.search(r"\b(?:rm|rmdir|unlink|git|cp|mv|rsync)\b", stripped):
                raise ValueError("destructive command could not be parsed safely")
            continue
        while tokens:
            executable = os.path.basename(tokens[0])
            if "=" in tokens[0] and not tokens[0].startswith("="):
                tokens.pop(0)
                continue
            if executable in {"command", "builtin", "nohup"}:
                tokens.pop(0)
                continue
            if executable == "env":
                tokens.pop(0)
                if tokens and tokens[0].startswith("-"):
                    raise ValueError("env options are not accepted for a destructive lease")
                continue
            if executable == "sudo":
                tokens.pop(0)
                if tokens and tokens[0].startswith("-"):
                    raise ValueError("sudo options are not accepted for a destructive lease")
                continue
            break
        if tokens:
            parsed.append(tokens)
    return parsed


def resolve_target(cwd: str, value: str) -> str:
    if EXPANSION_RE.search(value):
        raise ValueError("destructive targets cannot contain shell expansion or globs")
    return os.path.normpath(value if os.path.isabs(value) else os.path.join(cwd, value))


def destructive_targets(command: str, cwd: str, depth: int = 0) -> tuple[bool, list[str]]:
    if depth > 2:
        raise ValueError("nested shell command is too deep to inspect")
    destructive = False
    targets: list[str] = []
    for tokens in command_segments(command):
        executable = os.path.basename(tokens[0])
        if executable in {"rm", "rmdir", "unlink"}:
            destructive = True
            operands = [token for token in tokens[1:] if token != "--" and not token.startswith("-")]
            if not operands:
                raise ValueError("destructive command has no inspectable target")
            targets.extend(resolve_target(cwd, operand) for operand in operands)
            continue
        if executable in {"bash", "dash", "sh", "zsh"}:
            for index, token in enumerate(tokens[1:], start=1):
                if token.startswith("-") and "c" in token and index + 1 < len(tokens):
                    nested_destructive, nested_targets = destructive_targets(
                        tokens[index + 1], cwd, depth + 1
                    )
                    destructive = destructive or nested_destructive
                    targets.extend(nested_targets)
                    break
            continue
        if executable == "git":
            git_cwd = cwd
            subcommand = None
            index = 1
            while index < len(tokens):
                token = tokens[index]
                if token == "-C":
                    if index + 1 >= len(tokens):
                        raise ValueError("git -C has no inspectable path")
                    git_cwd = resolve_target(cwd, tokens[index + 1])
                    index += 2
                    continue
                if token == "-c":
                    index += 2
                    continue
                if token.startswith("-"):
                    index += 1
                    continue
                subcommand = token
                break
            if subcommand in {"gc", "prune", "clean"}:
                if any(
                    token == "--git-dir"
                    or token.startswith("--git-dir=")
                    or token == "--work-tree"
                    or token.startswith("--work-tree=")
                    for token in tokens[1:]
                ):
                    raise ValueError("git directory overrides are not accepted for a destructive lease")
                destructive = True
                targets.append(git_cwd)
            continue
        if executable in {"cp", "mv", "rsync"} and re.search(
            r"(?:backups?|snapshots?|\.bak)(?:/|\b)", " ".join(tokens), re.IGNORECASE
        ):
            destructive = True
            operands = [token for token in tokens[1:] if not token.startswith("-")]
            if len(operands) < 2:
                raise ValueError("backup overwrite has no inspectable destination")
            targets.append(resolve_target(cwd, operands[-1]))
    for match in re.finditer(r">{1,2}[ \t]*([^\s;&|]+)", command):
        target = match.group(1).strip("'\"")
        if re.search(r"(?:backups?|snapshots?|\.bak)(?:/|\b)", target, re.IGNORECASE):
            destructive = True
            targets.append(resolve_target(cwd, target))
    return destructive, sorted(set(targets))


def path_within(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([path, root]) == root
    except ValueError:
        return False


def paths_overlap(left: str, right: str) -> bool:
    return path_within(left, right) or path_within(right, left)


def resource_marker(command: str) -> str:
    matches = re.findall(r"(?m)#[ \t]*CLAUDEX_RESOURCE[ \t]*=([^\r\n]+?)[ \t]*$", command)
    if len(matches) != 1:
        raise ValueError("destructive Bash requires exactly one # CLAUDEX_RESOURCE=\"/absolute/path\" marker")
    return normalize_absolute_path(json.loads(matches[0]))


def handle_destructive_bash(data: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    command = str((data.get("tool_input") or {}).get("command", ""))
    cwd = normalize_absolute_path(data.get("cwd"))
    try:
        destructive, targets = destructive_targets(command, cwd)
    except ValueError as error:
        return pretool_deny(f"Destructive command rejected: {error}")
    if not destructive:
        return None
    if data.get("agent_id"):
        return pretool_deny("Subagents cannot perform destructive Bash operations.")
    if state.get("phase") != "RUNNING":
        return pretool_deny("Destructive Bash is closed during final freeze.")
    try:
        resource = resource_marker(command)
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        return pretool_deny(str(error))
    lease = state.get("resource_leases", {}).get(resource)
    if not lease or lease.get("approval_generation") != state.get("prompt_generation"):
        return pretool_deny("No current user-approved Claudex resource lease exists for this exact resource.")
    if not SHA256_RE.fullmatch(str(lease.get("evidence_sha256", ""))):
        return pretool_deny("The resource evidence manifest is not frozen by SHA-256.")
    if not targets or any(not path_within(target, resource) for target in targets):
        return pretool_deny("Every destructive target must resolve inside the exact approved resource.")
    for agent in active_agents(state):
        if path_within(agent.get("cwd", "/"), resource):
            return pretool_deny("An active Agent cwd is inside the destructive target resource.")
        if any(paths_overlap(item, resource) for item in agent.get("resources", [])):
            return pretool_deny("An active Agent still holds a reader lease on this resource.")
    pending = state.get("pending_agent")
    if pending and any(paths_overlap(item, resource) for item in pending.get("resources", [])):
        return pretool_deny("A pending Agent still holds a reader lease on this resource.")
    for task in state.get("tasks", {}).values():
        if task.get("blocking") and not task.get("completed"):
            resources = task.get("resources", [])
            if not resources or any(paths_overlap(item, resource) for item in resources):
                return pretool_deny("A blocking task still depends on this resource.")
    return None


def handle_task_update(data: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    tool_input = data.get("tool_input") or {}
    if tool_input.get("status") != "deleted":
        return None
    if state.get("phase") != "RUNNING":
        return pretool_deny("Tasks cannot be deleted after Claudex final freeze begins.")
    identifier = tool_input.get("taskId") or tool_input.get("task_id")
    if not isinstance(identifier, str) or not identifier.strip():
        return pretool_deny("Task deletion requires an inspectable task identifier.")
    task = state.get("tasks", {}).get(task_key(identifier))
    if task is None:
        return pretool_deny("Only a completed task tracked by the current Claudex session can be deleted.")
    if not task.get("completed"):
        return pretool_deny("An incomplete Claudex task cannot be deleted to bypass its completion gate.")
    return None


def handle_finalization_pretool(
    data: dict[str, Any], state: dict[str, Any]
) -> dict[str, Any] | None:
    identifier = data.get("agent_id")
    if not identifier:
        return None
    record = state.get("agents", {}).get(agent_key(identifier))
    if not record or not record.get("active") or record.get("agent_phase") != "FINALIZING":
        return None
    tool_name = str(data.get("tool_name", ""))
    if tool_name not in record.get("finalization_denied_tools", []):
        return None
    record["finalization_tool_denials"] = int(record.get("finalization_tool_denials", 0)) + 1
    return pretool_deny(
        "This Agent has reached its work-turn budget. Do not call more tools; immediately return the required single-line CLAUDEX_AGENT_RESULT envelope."
    )


def handle_pretool(data: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    finalization_decision = handle_finalization_pretool(data, state)
    if finalization_decision is not None:
        return finalization_decision
    tool_name = data.get("tool_name")
    if tool_name == "Agent":
        return handle_agent_admission(data, state)
    if tool_name == "Skill":
        return handle_skill(data, state)
    if tool_name == "Bash":
        return handle_destructive_bash(data, state)
    if tool_name == "TaskUpdate":
        return handle_task_update(data, state)
    return None


def agent_key(value: Any) -> str:
    return stable_hash(str(value))


def handle_post_tool_batch(data: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    identifier = data.get("agent_id")
    if not identifier:
        return None
    record = state.get("agents", {}).get(agent_key(identifier))
    if not record or not record.get("active"):
        return None
    budget = record.get("work_turn_budget")
    if not isinstance(budget, int) or isinstance(budget, bool) or budget <= 0:
        return None
    record["work_turns_seen"] = int(record.get("work_turns_seen", 0)) + 1
    if record.get("agent_phase") == "FINALIZING" or record["work_turns_seen"] < budget:
        return None
    record["agent_phase"] = "FINALIZING"
    record["work_budget_reached"] = True
    record["finalization_entered_at"] = now_seconds()
    return hook_context(
        "PostToolBatch",
        "Claudex work-turn budget reached. Do not call any more tools. Synthesize the evidence already gathered and immediately return the required single-line CLAUDEX_AGENT_RESULT envelope.",
    )


def handle_subagent_start(data: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    pending = state.get("pending_agent")
    key = agent_key(data.get("agent_id", ""))
    policy = load_agent_policy()
    agent_type = str(data.get("agent_type", "unknown"))
    if not pending or pending.get("agent_type") != data.get("agent_type"):
        state["agents"][key] = {
            "active": True,
            "admitted": False,
            "blocking": True,
            "agent_type": agent_type,
            "cwd": normalize_absolute_path(data.get("cwd")),
            "resources": [],
            "started_at": now_seconds(),
            "result_retry_used": False,
            "extension_used": False,
            "result_status": "unknown",
            "reported_to_parent": False,
            "termination_cause": "unknown",
            "agent_phase": "WORKING",
            "work_turns_seen": 0,
        }
        if agent_type in ALLOWED_AGENT_TYPES:
            state["agents"][key].update(agent_policy_snapshot(policy, agent_type))
        return hook_context(
            "SubagentStart",
            "Claudex admission state is missing. Stop without doing work and return a blocked result.",
        )
    record = {**pending, "active": True, "admitted": True, "started_at": now_seconds()}
    record.update(agent_policy_snapshot(policy, agent_type))
    record["cwd"] = normalize_absolute_path(data.get("cwd"))
    record["result_retry_used"] = False
    record["extension_used"] = False
    record["result_status"] = "unknown"
    record["reported_to_parent"] = False
    record["termination_cause"] = "unknown"
    record["agent_phase"] = "WORKING"
    record["work_turns_seen"] = 0
    record["work_budget_reached"] = False
    record["finalization_tool_denials"] = 0
    state["agents"][key] = record
    task_reference = record.get("task_key")
    if task_reference:
        task = state.get("tasks", {}).get(task_reference)
        if task is not None:
            task["latest_agent_key"] = key
    state["pending_agent"] = None
    return None


def validate_agent_result(raw: Any, agent_type: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("CLAUDEX_AGENT_RESULT must be an object")
    status = nonempty_string(raw.get("status"), "status")
    allowed = {"pass", "fail", "blocked"} if agent_type == "Verify" else {
        "complete",
        "blocked",
        "needs_extension",
    }
    if status not in allowed:
        raise ValueError(f"invalid {agent_type} status")
    acceptance = string_list(raw.get("acceptance_criteria_met", []), "acceptance_criteria_met")
    evidence = string_list(raw.get("evidence", []), "evidence")
    unknowns = string_list(raw.get("critical_unknowns", []), "critical_unknowns")
    next_evidence = raw.get("next_evidence")
    extension_reason = raw.get("extension_reason")
    if next_evidence is not None:
        next_evidence = nonempty_string(next_evidence, "next_evidence")
    if extension_reason is not None:
        extension_reason = nonempty_string(extension_reason, "extension_reason")
    if status == "needs_extension" and (not unknowns or not next_evidence or not extension_reason):
        raise ValueError("needs_extension requires critical_unknowns, next_evidence, and extension_reason")
    if status in {"complete", "pass"} and unknowns:
        raise ValueError("complete/pass cannot retain critical unknowns")
    return {
        "status": status,
        "acceptance_hash": stable_hash(acceptance),
        "evidence_hash": stable_hash(evidence),
        "critical_unknowns_hash": stable_hash(unknowns),
        "has_critical_unknowns": bool(unknowns),
        "next_evidence_hash": stable_hash(next_evidence) if next_evidence else None,
        "extension_reason_hash": stable_hash(extension_reason) if extension_reason else None,
    }


def semantic_result_status(result: dict[str, Any]) -> str:
    status = result.get("status")
    if status in {"complete", "pass"}:
        return "valid_result"
    if status == "needs_extension":
        return "needs_extension"
    if status == "blocked":
        return "blocked"
    if status == "fail":
        return "failed"
    return "invalid_result"


def finish_agent(
    record: dict[str, Any],
    result_status: str,
    result: dict[str, Any] | None = None,
    observations: dict[str, Any] | None = None,
) -> None:
    if result is not None:
        record.update(result)
    record["result_status"] = result_status
    record["termination_cause"] = "unknown"
    if observations is not None:
        record["observations"] = observations
    record["active"] = False
    record["stopped_at"] = now_seconds()
    record["wall_seconds"] = max(0, record["stopped_at"] - int(record.get("started_at", 0)))


def handle_subagent_stop(data: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    key = agent_key(data.get("agent_id", ""))
    record = state.get("agents", {}).setdefault(
        key,
        {
            "active": True,
            "admitted": False,
            "blocking": True,
            "agent_type": str(data.get("agent_type", "unknown")),
            "started_at": now_seconds(),
            "result_retry_used": False,
            "extension_used": False,
            "result_status": "unknown",
            "reported_to_parent": False,
            "termination_cause": "unknown",
            "agent_phase": "WORKING",
            "work_turns_seen": 0,
        },
    )
    message = str(data.get("last_assistant_message", ""))
    if not message.strip():
        budget = record.get("work_turn_budget")
        budget_reached = bool(record.get("work_budget_reached")) or (
            isinstance(budget, int)
            and not isinstance(budget, bool)
            and int(record.get("work_turns_seen", 0)) >= budget
        )
        finalizing = record.get("agent_phase") == "FINALIZING"
        finish_agent(
            record,
            "exhausted_or_empty" if budget_reached or finalizing else "stopped_without_valid_result",
            observations={
                "last_assistant_message_empty": True,
                "work_budget_reached": budget_reached,
                "finalization_phase_entered": finalizing,
                "finalization_tool_denials": int(record.get("finalization_tool_denials", 0)),
            },
        )
        return None
    try:
        raw = one_marker(message, "CLAUDEX_AGENT_RESULT", required=True)
        result = validate_agent_result(raw, record.get("agent_type", str(data.get("agent_type", ""))))
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        if state.get("phase") == "RUNNING" and not record.get("result_retry_used"):
            record["result_retry_used"] = True
            record["repair_attempts"] = 1
            return {
                "decision": "block",
                "reason": f"Return one valid single-line CLAUDEX_AGENT_RESULT marker: {error}",
            }
        finish_agent(
            record,
            "invalid_result",
            result={
                "evidence_hash": stable_hash([]),
                "critical_unknowns_hash": stable_hash(["invalid_result"]),
                "has_critical_unknowns": True,
            },
            observations={
                "last_assistant_message_empty": False,
                "result_contract_valid": False,
                "repair_attempts": int(record.get("repair_attempts", 0)),
            },
        )
        return None
    if (
        result["status"] == "needs_extension"
        and state.get("phase") == "RUNNING"
        and record.get("agent_phase") != "FINALIZING"
        and not record.get("extension_used")
    ):
        record["extension_used"] = True
        record.update(result)
        record["result_status"] = "needs_extension"
        return {
            "decision": "block",
            "reason": "One bounded extension is approved. Gather only the declared next evidence, then stop.",
        }
    finish_agent(
        record,
        semantic_result_status(result),
        result=result,
        observations={
            "last_assistant_message_empty": False,
            "result_contract_valid": True,
            "work_budget_reached": bool(record.get("work_budget_reached")),
            "finalization_phase_entered": record.get("agent_phase") == "FINALIZING",
            "finalization_tool_denials": int(record.get("finalization_tool_denials", 0)),
        },
    )
    return None


def task_key(value: Any) -> str:
    return stable_hash(str(value))


def validate_task_contract(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("CLAUDEX_TASK_POLICY must be an object")
    blocking = raw.get("blocking", False)
    tests_required = raw.get("tests_required", False)
    verifier_required = raw.get("verifier_required", False)
    if not all(isinstance(value, bool) for value in (blocking, tests_required, verifier_required)):
        raise ValueError("task policy flags must be true or false")
    artifacts = string_list(raw.get("required_artifacts", []), "required_artifacts")
    return {
        "blocking": blocking,
        "tests_required": tests_required,
        "verifier_required": verifier_required,
        "required_artifacts": bool(artifacts),
        "required_artifacts_hash": stable_hash(artifacts),
        "resources": normalize_resources(raw.get("resources")),
    }


def handle_task_created(data: dict[str, Any], state: dict[str, Any]) -> None:
    if state.get("phase") != "RUNNING":
        raise HookBlock("Claudex final freeze has started; new tasks cannot be created.")
    description = str(data.get("task_description", ""))
    raw = one_marker(description, "CLAUDEX_TASK_POLICY", required=True)
    policy = validate_task_contract(raw)
    state["tasks"][task_key(data.get("task_id", ""))] = {
        **policy,
        "completed": False,
        "latest_agent_key": None,
        "subject_hash": stable_hash(str(data.get("task_subject", ""))),
        "created_at": now_seconds(),
    }


def validate_task_result(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("CLAUDEX_TASK_RESULT must be an object")
    if raw.get("acceptance_criteria_closed") is not True:
        raise ValueError("acceptance_criteria_closed must be true")
    tests = raw.get("tests")
    artifacts = raw.get("required_artifacts")
    verifier = raw.get("blocking_verifier")
    agent_failure_handled = raw.get("agent_failure_handled", False)
    if tests not in {"passed", "not_required"}:
        raise ValueError("tests must be passed or not_required")
    if artifacts not in {"present", "not_required"}:
        raise ValueError("required_artifacts must be present or not_required")
    if verifier not in {"passed", "not_required"}:
        raise ValueError("blocking_verifier must be passed or not_required")
    if not isinstance(agent_failure_handled, bool):
        raise ValueError("agent_failure_handled must be true or false")
    evidence_hash = nonempty_string(raw.get("evidence_sha256"), "evidence_sha256").lower()
    if not SHA256_RE.fullmatch(evidence_hash):
        raise ValueError("evidence_sha256 must be a lowercase SHA-256")
    return {
        "tests": tests,
        "required_artifacts_status": artifacts,
        "blocking_verifier_status": verifier,
        "agent_failure_handled": agent_failure_handled,
        "evidence_sha256": evidence_hash,
    }


def handle_task_completed(data: dict[str, Any], state: dict[str, Any]) -> None:
    if any(agent.get("blocking") for agent in active_agents(state)):
        raise HookBlock("A blocking verifier or worker is still active.")
    description = str(data.get("task_description", ""))
    try:
        result = validate_task_result(one_marker(description, "CLAUDEX_TASK_RESULT", required=True))
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise HookBlock(f"Task completion evidence rejected: {error}") from error
    key = task_key(data.get("task_id", ""))
    task = state.get("tasks", {}).setdefault(key, {**validate_task_contract({}), "completed": False})
    latest_agent_key = task.get("latest_agent_key")
    if latest_agent_key:
        agent = state.get("agents", {}).get(latest_agent_key)
        if (
            agent is not None
            and agent.get("result_status") != "valid_result"
            and not result["agent_failure_handled"]
        ):
            raise HookBlock(
                "The latest Agent linked to this blocking task did not produce a valid result; resolve or explicitly take over that failure before completing the task."
            )
    if task.get("tests_required") and result["tests"] != "passed":
        raise HookBlock("This task requires passing tests.")
    if task.get("required_artifacts") and result["required_artifacts_status"] != "present":
        raise HookBlock("This task requires its declared artifacts.")
    if task.get("verifier_required") and result["blocking_verifier_status"] != "passed":
        raise HookBlock("This task requires a passing blocking verifier.")
    task.update(result)
    task["completed"] = True
    task["completed_at"] = now_seconds()


def agent_response_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        raise ValueError("Agent content must be text or an array of content blocks")
    text_parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            text_parts.append(block)
        elif isinstance(block, dict) and isinstance(block.get("text"), str):
            text_parts.append(block["text"])
    return "\n".join(text_parts)


def record_agent_usage(record: dict[str, Any], response: dict[str, Any]) -> None:
    total_tokens = response.get("totalTokens")
    if isinstance(total_tokens, int) and not isinstance(total_tokens, bool) and total_tokens >= 0:
        record["total_tokens"] = total_tokens
    model = response.get("resolvedModel")
    if isinstance(model, str) and model:
        record["resolved_model"] = model
    models = response.get("modelsUsed")
    if isinstance(models, list) and all(isinstance(item, str) for item in models):
        record["models_used"] = models


def delivery_failure_context(
    identifier: Any, runtime_status: str, result_status: str, result_available: bool
) -> dict[str, Any]:
    payload = {
        "v": 1,
        "agent_id": str(identifier or "unknown"),
        "runtime_status": runtime_status,
        "semantic_status": result_status,
        "semantic_success": False,
        "result_available": result_available,
        "reported_to_parent": True,
        "retry_automatically": False,
    }
    return hook_context(
        "PostToolUse",
        "CLAUDEX_AGENT_DELIVERY="
        + json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    )


def update_agent_tool_result(
    data: dict[str, Any], state: dict[str, Any]
) -> dict[str, Any] | None:
    response = data.get("tool_response") or {}
    if not isinstance(response, dict):
        response = {}
    identifier = response.get("agentId")
    record = None
    if identifier:
        record = state.get("agents", {}).get(agent_key(identifier))
    if record is not None:
        record_agent_usage(record, response)
    runtime_status = str(response.get("status", "unknown"))
    if runtime_status == "async_launched":
        if record is not None:
            record["launch_status"] = "async_launched"
            record["delivery_method"] = "background_native_notification"
            record["reported_to_parent"] = False
        state["pending_agent"] = None
        return None
    if runtime_status != "completed":
        if record is not None:
            record["result_status"] = "failed"
            record["reported_to_parent"] = True
            record["delivery_method"] = "foreground_agent_tool_result"
        state["pending_agent"] = None
        return delivery_failure_context(identifier, runtime_status, "failed", False)

    try:
        content_text = agent_response_text(response.get("content"))
    except (TypeError, ValueError):
        content_text = ""
    parsed_result = None
    result_status = "unknown"
    result_available = False
    if content_text.strip():
        try:
            raw = one_marker(content_text, "CLAUDEX_AGENT_RESULT", required=True)
            agent_type = (
                record.get("agent_type")
                if record is not None
                else str((data.get("tool_input") or {}).get("subagent_type", ""))
            )
            parsed_result = validate_agent_result(raw, agent_type)
            result_status = semantic_result_status(parsed_result)
            result_available = True
        except (TypeError, ValueError, json.JSONDecodeError):
            result_status = "invalid_result"
    else:
        prior_status = record.get("result_status") if record is not None else None
        result_status = (
            prior_status
            if prior_status
            in {"exhausted_or_empty", "stopped_without_valid_result", "invalid_result", "failed"}
            else "exhausted_or_empty"
        )

    if record is not None:
        if parsed_result is not None:
            record.update(parsed_result)
        record["result_status"] = result_status
        record["reported_to_parent"] = True
        record["delivery_method"] = "foreground_agent_tool_result"
        record["active"] = False
        record.setdefault("stopped_at", now_seconds())
    state["pending_agent"] = None
    if result_status == "valid_result":
        return None
    return delivery_failure_context(identifier, runtime_status, result_status, result_available)


def clear_failed_agent(data: dict[str, Any], state: dict[str, Any]) -> None:
    pending = state.get("pending_agent")
    if pending and pending.get("tool_use_hash") == stable_hash(data.get("tool_use_id", "")):
        state["pending_agent"] = None


def stop_blockers(data: dict[str, Any], state: dict[str, Any]) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    detached: list[str] = []
    pending = state.get("pending_agent")
    if pending:
        role = "blocking" if pending.get("blocking") else "nonblocking"
        target = blockers if pending.get("blocking") else detached
        target.append(f"pending_agent:{role}")
    current_agents = active_agents(state)
    for agent in current_agents:
        role = "blocking" if agent.get("blocking") else "nonblocking"
        target = blockers if agent.get("blocking") else detached
        target.append(f"agent:{role}:{agent.get('agent_type', 'unknown')}")
    for task in state.get("tasks", {}).values():
        if task.get("blocking") and not task.get("completed"):
            blockers.append("blocking_task")
    for task in data.get("background_tasks") or []:
        if not isinstance(task, dict) or task.get("status") in TERMINAL_TASK_STATUSES:
            continue
        detached.append(f"background:{task.get('type', 'unknown')}")
    for _cron in data.get("session_crons") or []:
        detached.append("session_cron")
    return sorted(blockers), sorted(detached)


def handle_stop(data: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    if state.get("phase") == "FINAL_EMITTED":
        return None
    if state.get("phase") == "RUNNING":
        state["phase"] = "FREEZING"
        state["freeze"] = {"started_at": now_seconds()}
    blockers, detached = stop_blockers(data, state)
    state["freeze"]["detached_hash"] = stable_hash(detached)
    if blockers:
        signature = stable_hash(blockers)
        if state["freeze"].get("block_signature") != signature:
            state["freeze"]["block_signature"] = signature
            state["freeze"]["blocked_once"] = True
            return hook_context(
                "Stop",
                "Claudex final freeze is waiting for an active blocking Agent or unresolved blocking task. Resolve it; do not poll the Stop hook.",
            )
        return {
            "continue": False,
            "stopReason": "Claudex final freeze remains blocked by unchanged work. Resolve it, then continue in a new prompt; the hook will not poll.",
        }
    state["phase"] = "FROZEN"
    state["freeze"]["frozen_at"] = now_seconds()
    state["freeze"]["final_digest"] = stable_hash(str(data.get("last_assistant_message", "")))
    state["phase"] = "FINAL_EMITTED"
    state["freeze"]["final_emitted_at"] = now_seconds()
    return None


def dispatch(data: dict[str, Any], state: dict[str, Any]) -> dict[str, Any] | None:
    event = data.get("hook_event_name")
    if event == "UserPromptSubmit":
        handle_user_prompt(data, state)
        return None
    if event == "PreToolUse":
        return handle_pretool(data, state)
    if event == "PostToolBatch":
        return handle_post_tool_batch(data, state)
    if event == "SubagentStart":
        return handle_subagent_start(data, state)
    if event == "SubagentStop":
        return handle_subagent_stop(data, state)
    if event == "TaskCreated":
        handle_task_created(data, state)
        return None
    if event == "TaskCompleted":
        handle_task_completed(data, state)
        return None
    if event == "PostToolUse" and data.get("tool_name") == "Agent":
        return update_agent_tool_result(data, state)
    if event == "PostToolUseFailure" and data.get("tool_name") == "Agent":
        clear_failed_agent(data, state)
        return None
    if event == "Stop":
        return handle_stop(data, state)
    return None


def fail_closed(event: str, message: str) -> int:
    reason = f"Claudex harness failed closed: {message}"
    if event == "PreToolUse":
        print(json.dumps(pretool_deny(reason), ensure_ascii=False, separators=(",", ":")))
        return 0
    if event == "Stop":
        print(json.dumps({"continue": False, "stopReason": reason}, ensure_ascii=False, separators=(",", ":")))
        return 0
    if event == "SubagentStop":
        print(json.dumps({"decision": "block", "reason": reason}, ensure_ascii=False, separators=(",", ":")))
        return 0
    if event == "SubagentStart":
        print(json.dumps(hook_context(event, reason), ensure_ascii=False, separators=(",", ":")))
        return 0
    if event in {"PostToolBatch", "PostToolUse"}:
        print(json.dumps(hook_context(event, reason), ensure_ascii=False, separators=(",", ":")))
        return 0
    if event in {"UserPromptSubmit", "TaskCreated", "TaskCompleted"}:
        print(reason, file=sys.stderr)
        return 2
    print(reason, file=sys.stderr)
    return 0


def validate_profile_cli(arguments: list[str]) -> int:
    if len(arguments) != 3 or arguments[0] != "--validate-profile":
        print("usage: claudex_hook.py --validate-profile AGENTS POLICY", file=sys.stderr)
        return 2
    try:
        agents = read_json_object(Path(arguments[1]), "agents.json")
        policy = read_json_object(Path(arguments[2]), "Claudex agent policy")
        validate_profile_documents(agents, policy)
        return 0
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"Claudex profile validation failed: {error}", file=sys.stderr)
        return 2


def main() -> int:
    if len(sys.argv) > 1:
        return validate_profile_cli(sys.argv[1:])
    event = "unknown"
    try:
        data = json.load(sys.stdin)
        if not isinstance(data, dict):
            raise ValueError("hook input must be an object")
        event = str(data.get("hook_event_name", "unknown"))
        session_id = nonempty_string(data.get("session_id"), "session_id")
        with StateFile(session_id) as (state, state_file):
            initial_hash = stable_hash(state)
            try:
                output = dispatch(data, state)
            except HookBlock as error:
                if stable_hash(state) != initial_hash:
                    state_file.save(state)
                print(str(error), file=sys.stderr)
                return 2
            if stable_hash(state) != initial_hash:
                state_file.save(state)
        if output is not None:
            print(json.dumps(output, ensure_ascii=False, separators=(",", ":")))
        return 0
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        return fail_closed(event, str(error))


if __name__ == "__main__":
    raise SystemExit(main())
