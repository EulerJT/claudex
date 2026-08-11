#!/usr/bin/env python3

import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
HOOK = (
    PROJECT_ROOT
    / "claude-code/profile/hooks/claudex_hook.py"
)
PROFILE = PROJECT_ROOT / "claude-code/profile"
AGENTS = PROFILE / "agents.json"
POLICY = PROFILE / "claudex-agent-policy.json"
EVIDENCE_HASH = "a" * 64


class HarnessTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.state_dir = Path(self.temporary.name) / "state"
        self.session_id = "synthetic-session"

    def tearDown(self):
        self.temporary.cleanup()

    def run_hook(self, event, **fields):
        payload = {
            "session_id": self.session_id,
            "cwd": "/workspace",
            "hook_event_name": event,
            **fields,
        }
        environment = os.environ.copy()
        environment["CLAUDEX_HARNESS_STATE_DIR"] = str(self.state_dir)
        environment["CLAUDEX_PROFILE_ASSETS_DIR"] = str(PROFILE)
        return subprocess.run(
            [str(HOOK)],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            env=environment,
            check=False,
        )

    def prompt(self, text="ordinary request"):
        return self.run_hook("UserPromptSubmit", prompt=text)

    @staticmethod
    def contract(scope="/workspace/resource", blocking=True, task_id=None):
        value = {
            "goal": "bounded goal",
            "scope": scope,
            "acceptance_criteria": ["direct evidence"],
            "stop_condition": "acceptance met",
            "blocking": blocking,
            "resources": [scope],
        }
        if task_id is not None:
            value["task_id"] = task_id
        return "CLAUDEX_TASK_CONTRACT=" + json.dumps(value, separators=(",", ":"))

    @staticmethod
    def agent_result(status="complete", unknowns=None, next_evidence=None, reason=None):
        value = {
            "status": status,
            "acceptance_criteria_met": ["direct evidence"],
            "evidence": ["synthetic"],
            "critical_unknowns": unknowns or [],
            "next_evidence": next_evidence,
            "extension_reason": reason,
        }
        return "CLAUDEX_AGENT_RESULT=" + json.dumps(value, separators=(",", ":"))

    @staticmethod
    def task_result(agent_failure_handled=False):
        value = {
            "acceptance_criteria_closed": True,
            "tests": "passed",
            "required_artifacts": "present",
            "blocking_verifier": "passed",
            "agent_failure_handled": agent_failure_handled,
            "evidence_sha256": EVIDENCE_HASH,
        }
        return "CLAUDEX_TASK_RESULT=" + json.dumps(value, separators=(",", ":"))

    def read_state(self):
        return json.loads(next(self.state_dir.glob("*.json")).read_text())

    def admit_agent(
        self,
        blocking=True,
        *,
        agent_type="Explore",
        agent_id="agent-1",
        task_id=None,
        run_in_background=False,
    ):
        result = self.run_hook(
            "PreToolUse",
            tool_name="Agent",
            tool_use_id="tool-1",
            tool_input={
                "subagent_type": agent_type,
                "prompt": self.contract(blocking=blocking, task_id=task_id),
                "description": "bounded",
                "run_in_background": run_in_background,
            },
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        if not (blocking and run_in_background):
            self.assertEqual(result.stdout, "")
        started = self.run_hook("SubagentStart", agent_id=agent_id, agent_type=agent_type)
        self.assertEqual(started.returncode, 0, started.stderr)
        return result

    def test_agent_schema_and_policy_budget_match(self):
        agents = json.loads(AGENTS.read_text())
        policy = json.loads(POLICY.read_text())
        for agent_type, definition in agents.items():
            profile = policy["profiles"][agent_type]
            self.assertEqual(
                definition["maxTurns"],
                profile["work_turn_budget"] + profile["finalization_reserve"],
            )
        valid = subprocess.run(
            [str(HOOK), "--validate-profile", str(AGENTS), str(POLICY)],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(valid.returncode, 0, valid.stderr)
        for forbidden in (
            "workTurnBudget",
            "finalizationReserve",
            "operationalTools",
            "resultContractVersion",
        ):
            with self.subTest(forbidden=forbidden), tempfile.TemporaryDirectory() as directory:
                mutated = json.loads(AGENTS.read_text())
                mutated["Explore"][forbidden] = 1
                mutated_path = Path(directory) / "agents.json"
                mutated_path.write_text(json.dumps(mutated))
                rejected = subprocess.run(
                    [str(HOOK), "--validate-profile", str(mutated_path), str(POLICY)],
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(rejected.returncode, 2)
                self.assertIn("non-official agent fields", rejected.stderr)

    def test_schema_one_sidecar_without_policy_snapshot_remains_readable(self):
        self.state_dir.mkdir(mode=0o700, parents=True)
        state_path = self.state_dir / (
            hashlib.sha256(self.session_id.encode()).hexdigest() + ".json"
        )
        legacy_state = {
            "schema": 1,
            "revision": 1,
            "phase": "RUNNING",
            "prompt_generation": 0,
            "prompt_hash": None,
            "allowed_skills": [],
            "pending_agent": None,
            "agents": {"legacy": {"active": False, "status": "complete"}},
            "tasks": {},
            "resource_leases": {},
            "freeze": {},
        }
        state_path.write_text(json.dumps(legacy_state))
        resumed = self.prompt("resume without rewriting old Agent policy")
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        state = self.read_state()
        self.assertEqual(state["schema"], 1)
        self.assertNotIn("policy_hash", state["agents"]["legacy"])

    def test_agent_contract_and_single_worker_gate(self):
        self.assertEqual(self.prompt().returncode, 0)
        missing = self.run_hook(
            "PreToolUse",
            tool_name="Agent",
            tool_use_id="tool-0",
            tool_input={"subagent_type": "Explore", "prompt": "look around"},
        )
        self.assertEqual(
            json.loads(missing.stdout)["hookSpecificOutput"]["permissionDecision"], "deny"
        )
        overridden = self.run_hook(
            "PreToolUse",
            tool_name="Agent",
            tool_use_id="tool-model",
            tool_input={
                "subagent_type": "Explore",
                "model": "opus",
                "prompt": self.contract(),
            },
        )
        self.assertEqual(
            json.loads(overridden.stdout)["hookSpecificOutput"]["permissionDecision"], "deny"
        )
        self.admit_agent()
        duplicate = self.run_hook(
            "PreToolUse",
            tool_name="Agent",
            tool_use_id="tool-2",
            tool_input={"subagent_type": "Explore", "prompt": self.contract()},
        )
        self.assertEqual(
            json.loads(duplicate.stdout)["hookSpecificOutput"]["permissionDecision"], "deny"
        )
        stopped = self.run_hook(
            "SubagentStop",
            agent_id="agent-1",
            agent_type="Explore",
            last_assistant_message=self.agent_result(),
        )
        self.assertEqual(stopped.returncode, 0, stopped.stderr)
        self.assertEqual(stopped.stdout, "")

    def test_work_budget_enters_finalization_and_empty_is_not_success(self):
        self.assertEqual(self.prompt().returncode, 0)
        self.admit_agent()
        for index in range(11):
            batch = self.run_hook(
                "PostToolBatch",
                agent_id="agent-1",
                agent_type="Explore",
                tool_calls=[{"tool_use_id": f"read-{index}", "tool_name": "Read"}],
            )
            self.assertEqual(batch.stdout, "")
        boundary = self.run_hook(
            "PostToolBatch",
            agent_id="agent-1",
            agent_type="Explore",
            tool_calls=[{"tool_use_id": "read-11", "tool_name": "Read"}],
        )
        self.assertIn("additionalContext", boundary.stdout)
        denied = self.run_hook(
            "PreToolUse",
            agent_id="agent-1",
            agent_type="Explore",
            tool_name="Grep",
            tool_use_id="grep-finalizing",
            tool_input={"pattern": "more"},
        )
        self.assertEqual(
            json.loads(denied.stdout)["hookSpecificOutput"]["permissionDecision"], "deny"
        )
        stopped = self.run_hook(
            "SubagentStop",
            agent_id="agent-1",
            agent_type="Explore",
            last_assistant_message="",
        )
        self.assertEqual(stopped.stdout, "")
        agent = next(iter(self.read_state()["agents"].values()))
        self.assertEqual(agent["result_status"], "exhausted_or_empty")
        self.assertEqual(agent["termination_cause"], "unknown")
        self.assertEqual(agent["policy_version"], 1)
        self.assertEqual(agent["physical_max_turns"], 14)
        self.assertTrue(agent["policy_hash"].startswith("sha256:"))
        self.assertTrue(agent["observations"]["last_assistant_message_empty"])
        self.assertEqual(agent["observations"]["finalization_tool_denials"], 1)
        self.assertFalse(agent["result_retry_used"])

    def test_nonempty_invalid_result_gets_one_format_repair(self):
        self.assertEqual(self.prompt().returncode, 0)
        self.admit_agent()
        first = self.run_hook(
            "SubagentStop",
            agent_id="agent-1",
            agent_type="Explore",
            last_assistant_message="plain but nonempty result",
        )
        self.assertEqual(json.loads(first.stdout)["decision"], "block")
        second = self.run_hook(
            "SubagentStop",
            agent_id="agent-1",
            agent_type="Explore",
            last_assistant_message="still invalid",
        )
        self.assertEqual(second.stdout, "")
        agent = next(iter(self.read_state()["agents"].values()))
        self.assertEqual(agent["result_status"], "invalid_result")
        self.assertEqual(agent["termination_cause"], "unknown")
        self.assertEqual(agent["observations"]["repair_attempts"], 1)

    def test_foreground_valid_result_is_reported_without_consume_protocol(self):
        self.assertEqual(self.prompt().returncode, 0)
        self.admit_agent()
        result_text = self.agent_result()
        self.assertEqual(
            self.run_hook(
                "SubagentStop",
                agent_id="agent-1",
                agent_type="Explore",
                last_assistant_message=result_text,
            ).returncode,
            0,
        )
        delivered = self.run_hook(
            "PostToolUse",
            tool_name="Agent",
            tool_use_id="tool-1",
            tool_input={"subagent_type": "Explore"},
            tool_response={
                "status": "completed",
                "agentId": "agent-1",
                "content": [{"type": "text", "text": result_text}],
                "totalTokens": 123,
            },
        )
        self.assertEqual(delivered.stdout, "")
        state = self.read_state()
        agent = next(iter(state["agents"].values()))
        self.assertEqual(agent["result_status"], "valid_result")
        self.assertTrue(agent["reported_to_parent"])
        self.assertEqual(agent["delivery_method"], "foreground_agent_tool_result")
        serialized = json.dumps(state, sort_keys=True)
        self.assertNotIn("PARENT_CONSUMED", serialized)
        self.assertNotIn("consume", serialized)

    def test_foreground_empty_result_reports_failure_and_blocks_linked_task(self):
        self.assertEqual(self.prompt().returncode, 0)
        task_policy = {
            "blocking": True,
            "tests_required": True,
            "required_artifacts": ["artifact"],
            "verifier_required": True,
            "resources": ["/workspace/resource"],
        }
        self.assertEqual(
            self.run_hook(
                "TaskCreated",
                task_id="task-empty",
                task_subject="synthetic",
                task_description="CLAUDEX_TASK_POLICY="
                + json.dumps(task_policy, separators=(",", ":")),
            ).returncode,
            0,
        )
        self.admit_agent(task_id="task-empty")
        stopped = self.run_hook(
            "SubagentStop",
            agent_id="agent-1",
            agent_type="Explore",
            last_assistant_message="",
        )
        self.assertEqual(stopped.stdout, "")
        delivered = self.run_hook(
            "PostToolUse",
            tool_name="Agent",
            tool_use_id="tool-1",
            tool_input={"subagent_type": "Explore"},
            tool_response={"status": "completed", "agentId": "agent-1", "content": []},
        )
        delivery = json.loads(delivered.stdout)["hookSpecificOutput"]["additionalContext"]
        self.assertIn('"semantic_success":false', delivery)
        self.assertIn('"reported_to_parent":true', delivery)
        rejected = self.run_hook(
            "TaskCompleted",
            task_id="task-empty",
            task_subject="synthetic",
            task_description=self.task_result(),
        )
        self.assertEqual(rejected.returncode, 2)
        self.assertIn("did not produce a valid result", rejected.stderr)
        taken_over = self.run_hook(
            "TaskCompleted",
            task_id="task-empty",
            task_subject="synthetic",
            task_description=self.task_result(agent_failure_handled=True),
        )
        self.assertEqual(taken_over.returncode, 0, taken_over.stderr)

    def test_blocking_agent_is_rewritten_to_foreground(self):
        self.assertEqual(self.prompt().returncode, 0)
        admitted = self.run_hook(
            "PreToolUse",
            tool_name="Agent",
            tool_use_id="tool-background-blocking",
            tool_input={
                "subagent_type": "Explore",
                "prompt": self.contract(blocking=True),
                "description": "bounded",
                "run_in_background": True,
            },
        )
        output = json.loads(admitted.stdout)["hookSpecificOutput"]
        self.assertEqual(output["permissionDecision"], "allow")
        self.assertFalse(output["updatedInput"]["run_in_background"])
        self.assertEqual(self.read_state()["pending_agent"]["launch_mode"], "foreground")

    def test_background_launch_does_not_claim_parent_delivery(self):
        self.assertEqual(self.prompt().returncode, 0)
        self.admit_agent(blocking=False, run_in_background=True)
        launched = self.run_hook(
            "PostToolUse",
            tool_name="Agent",
            tool_use_id="tool-1",
            tool_input={"subagent_type": "Explore", "run_in_background": True},
            tool_response={"status": "async_launched", "agentId": "agent-1"},
        )
        self.assertEqual(launched.stdout, "")
        agent = next(iter(self.read_state()["agents"].values()))
        self.assertFalse(agent["reported_to_parent"])
        self.assertEqual(agent["delivery_method"], "background_native_notification")

    def test_skill_requires_explicit_current_prompt(self):
        self.assertEqual(self.prompt("Use $claude-api for this request").returncode, 0)
        allowed = self.run_hook(
            "PreToolUse", tool_name="Skill", tool_use_id="skill-1", tool_input={"skill": "claude-api"}
        )
        self.assertEqual(allowed.stdout, "")
        denied = self.run_hook(
            "PreToolUse", tool_name="Skill", tool_use_id="skill-2", tool_input={"skill": "another"}
        )
        self.assertEqual(
            json.loads(denied.stdout)["hookSpecificOutput"]["permissionDecision"], "deny"
        )
        self.assertEqual(self.prompt().returncode, 0)
        expired = self.run_hook(
            "PreToolUse", tool_name="Skill", tool_use_id="skill-3", tool_input={"skill": "claude-api"}
        )
        self.assertEqual(
            json.loads(expired.stdout)["hookSpecificOutput"]["permissionDecision"], "deny"
        )

    def test_one_agent_extension_and_task_completion_gate(self):
        self.assertEqual(self.prompt().returncode, 0)
        self.admit_agent()
        extension = self.run_hook(
            "SubagentStop",
            agent_id="agent-1",
            agent_type="Explore",
            last_assistant_message=self.agent_result(
                "needs_extension", ["one unknown"], "read one file", "decision critical"
            ),
        )
        self.assertEqual(json.loads(extension.stdout)["decision"], "block")
        second = self.run_hook(
            "SubagentStop",
            agent_id="agent-1",
            agent_type="Explore",
            last_assistant_message=self.agent_result(
                "needs_extension", ["still unknown"], "same evidence", "still critical"
            ),
        )
        self.assertEqual(second.stdout, "")

        policy = {
            "blocking": True,
            "tests_required": True,
            "required_artifacts": ["artifact"],
            "verifier_required": True,
        }
        created = self.run_hook(
            "TaskCreated",
            task_id="task-1",
            task_subject="synthetic",
            task_description="CLAUDEX_TASK_POLICY=" + json.dumps(policy, separators=(",", ":")),
        )
        self.assertEqual(created.returncode, 0, created.stderr)
        deleted_early = self.run_hook(
            "PreToolUse",
            tool_name="TaskUpdate",
            tool_use_id="task-update-1",
            tool_input={"taskId": "task-1", "status": "deleted"},
        )
        self.assertEqual(
            json.loads(deleted_early.stdout)["hookSpecificOutput"]["permissionDecision"], "deny"
        )
        rejected = self.run_hook(
            "TaskCompleted", task_id="task-1", task_subject="synthetic", task_description="not ready"
        )
        self.assertEqual(rejected.returncode, 2)
        task_result = {
            "acceptance_criteria_closed": True,
            "tests": "passed",
            "required_artifacts": "present",
            "blocking_verifier": "passed",
            "evidence_sha256": EVIDENCE_HASH,
        }
        completed = self.run_hook(
            "TaskCompleted",
            task_id="task-1",
            task_subject="synthetic",
            task_description="CLAUDEX_TASK_RESULT="
            + json.dumps(task_result, separators=(",", ":")),
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        deleted_after_completion = self.run_hook(
            "PreToolUse",
            tool_name="TaskUpdate",
            tool_use_id="task-update-2",
            tool_input={"taskId": "task-1", "status": "deleted"},
        )
        self.assertEqual(deleted_after_completion.returncode, 0, deleted_after_completion.stderr)
        self.assertEqual(deleted_after_completion.stdout, "")

    def test_resource_lease_waits_for_reader_and_checks_target(self):
        lease = {
            "resource": "/workspace/resource",
            "evidence_sha256": EVIDENCE_HASH,
            "approval": "destructive_change",
        }
        self.assertEqual(
            self.prompt("CLAUDEX_RESOURCE_LEASE=" + json.dumps(lease, separators=(",", ":"))).returncode,
            0,
        )
        self.admit_agent(blocking=False)
        command = 'rm -rf /workspace/resource/old\n# CLAUDEX_RESOURCE="/workspace/resource"'
        blocked = self.run_hook(
            "PreToolUse", tool_name="Bash", tool_use_id="bash-1", tool_input={"command": command}
        )
        self.assertEqual(
            json.loads(blocked.stdout)["hookSpecificOutput"]["permissionDecision"], "deny"
        )
        self.assertEqual(
            self.run_hook(
                "SubagentStop",
                agent_id="agent-1",
                agent_type="Explore",
                last_assistant_message=self.agent_result(),
            ).returncode,
            0,
        )
        allowed = self.run_hook(
            "PreToolUse", tool_name="Bash", tool_use_id="bash-2", tool_input={"command": command}
        )
        self.assertEqual(allowed.returncode, 0, allowed.stderr)
        self.assertEqual(allowed.stdout, "")
        outside = self.run_hook(
            "PreToolUse",
            tool_name="Bash",
            tool_use_id="bash-3",
            tool_input={
                "command": 'rm -rf /workspace/other\n# CLAUDEX_RESOURCE="/workspace/resource"'
            },
        )
        self.assertEqual(
            json.loads(outside.stdout)["hookSpecificOutput"]["permissionDecision"], "deny"
        )

    def test_final_freeze_blocks_once_then_records_final(self):
        self.assertEqual(self.prompt().returncode, 0)
        policy = {"blocking": True}
        self.assertEqual(
            self.run_hook(
                "TaskCreated",
                task_id="task-1",
                task_subject="synthetic",
                task_description="CLAUDEX_TASK_POLICY=" + json.dumps(policy, separators=(",", ":")),
            ).returncode,
            0,
        )
        first = self.run_hook(
            "Stop", stop_hook_active=False, last_assistant_message="first", background_tasks=[], session_crons=[]
        )
        self.assertIn("additionalContext", first.stdout)
        second = self.run_hook(
            "Stop", stop_hook_active=True, last_assistant_message="second", background_tasks=[], session_crons=[]
        )
        self.assertFalse(json.loads(second.stdout)["continue"])
        task_result = {
            "acceptance_criteria_closed": True,
            "tests": "not_required",
            "required_artifacts": "not_required",
            "blocking_verifier": "not_required",
            "evidence_sha256": EVIDENCE_HASH,
        }
        self.assertEqual(
            self.run_hook(
                "TaskCompleted",
                task_id="task-1",
                task_subject="synthetic",
                task_description="CLAUDEX_TASK_RESULT="
                + json.dumps(task_result, separators=(",", ":")),
            ).returncode,
            0,
        )
        final = self.run_hook(
            "Stop", stop_hook_active=True, last_assistant_message="final", background_tasks=[], session_crons=[]
        )
        self.assertEqual(final.stdout, "")
        state_path = next(self.state_dir.glob("*.json"))
        state = json.loads(state_path.read_text())
        self.assertEqual(state["phase"], "FINAL_EMITTED")

    def test_nonblocking_agent_does_not_block_final(self):
        self.assertEqual(self.prompt().returncode, 0)
        self.admit_agent(blocking=False)
        final = self.run_hook(
            "Stop", stop_hook_active=False, last_assistant_message="first", background_tasks=[], session_crons=[]
        )
        self.assertEqual(final.stdout, "")
        state = self.read_state()
        self.assertEqual(state["phase"], "FINAL_EMITTED")
        self.assertTrue(next(iter(state["agents"].values()))["active"])


if __name__ == "__main__":
    unittest.main()
