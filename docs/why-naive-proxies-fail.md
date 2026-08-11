# Why Gateway-Only Claude Code Tutorials Fail

Many tutorials reduce the integration to two steps: point `ANTHROPIC_BASE_URL` at a
compatible gateway and rename the model. CLIProxyAPI is a prominent example of a gateway
that exposes OpenAI, Gemini, Claude, Codex, and Grok-compatible API surfaces. That solves
transport, account routing, and a useful portion of protocol compatibility.

It does not, by itself, define how GPT should behave inside the Claude Code harness.
The acceptance target is the complete coding loop: repeated tool calls, exact result
pairing, reasoning lineage, one valid terminal, long-context recovery, agent ownership,
and deterministic delivery to the parent.

The failures below are therefore not allegations that one gateway always fails. They are
the unowned contracts left behind when gateway connectivity is treated as the complete
integration.

## 1. HTTP 400 at the context boundary can look like a frozen session

Claude Code owns a growing Messages transcript, while a GPT Responses endpoint owns a
different context and compaction protocol. A generic gateway may correctly translate
every request and still have no authority to coordinate those two boundaries.

A typical failure sequence is:

```text
tool-heavy Claude Code transcript grows
  → translated Responses input exceeds the accepted context shape/size
  → upstream returns HTTP 400
  → client or gateway retries an equivalent oversized request
  → no valid compaction item or downstream completion is produced
  → the user sees repeated failure or an apparently stuck session
```

Token usage crossing a threshold does not prove that compaction happened. Claudex only
accepts a real opaque compaction item, stores the exact suffix that follows it, and emits
success only after that state is durable. If no item exists, it preserves the full input
and exposes the failure instead of pretending the context was reduced.

## 2. A generic `Explore` agent can over-engineer and still return nothing

Claude Code can launch a subagent, but a bare agent definition or prompt does not answer:

- Was delegation necessary, or was the target path already known?
- What is the exact scope and stop condition?
- How many turns are for investigation, and how many are reserved for finalization?
- Is the result empty, malformed, failed, or complete?
- Is the agent blocking the current task, or merely running in the background?
- How does the parent receive a valid result or a deterministic failure?

With only a physical `maxTurns`, an `Explore` agent can continue reading and searching,
expand the scope, spend its final turn on another tool call, and stop without a useful
answer. From the parent's perspective, “the child stopped” is not equivalent to “the
child completed.” Raising `maxTurns` merely gives the same uncontrolled loop more room.

Claudex admits an Agent only with a bounded task contract, allows one worker with depth
one, separates work from two reserved finalization turns, denies operational tools during
finalization, validates a structured result, and delivers either success or failure
through Claude Code's native foreground result path.

## 3. Protocol translation is not field renaming

Messages content blocks and Responses input/output items have different lifecycles.
Function call IDs, tool results, reasoning items, server tools, and terminal events must
retain the same meaning in both protocols.

A simplified proxy often produces a false positive: one text turn succeeds, but a later
tool result has no matching call, or `message_stop` is emitted before additional upstream
data arrives. Minimum tests must cover multi-turn tools, upstream rejection, stream
truncation, duplicate terminals, and payload after terminal.

## 4. A session ID is not a sufficient state owner

One Claude Code session can contain main, direct subagents, a reviewer, and Web Search.
If all requests share one continuation or compaction record, any branch can corrupt
another branch's lineage.

Claudex derives stable lanes from identity exposed by Claude Code. Main and each direct
Agent are separate; one lane is serialized. An absent or ambiguous identity must remain
stateless rather than being guessed.

## 5. Upstream completion is not yet downstream success

If a proxy sends Claude Code a success terminal and only then writes the compaction
sidecar, the process can crash between those operations. Claude Code advances while the
bridge retains old state, permanently forking the lineage.

Claudex performs file fsync, rename, and directory fsync before success. Indeterminate
durability enters an explicit recovery path and cannot be hidden by an automatic retry.

## 6. One semaphore can starve the control plane

If main and worker traffic fill every global permit, the reviewer may never run. If that
reviewer participates in a completion gate, the session can wait on control work that its
own data work prevents from starting.

Claudex acquires data/control category capacity before global capacity. The reserved
control slot is not a throughput optimization; it guarantees that the completion
protocol can still progress under load.

## 7. Global strict mode and silent argument rewriting both change meaning

One incompatible tool schema does not justify forcing every tool into strict mode.
Global `strict=true` can change unrelated optional fields, while silently converting an
unknown argument into a known argument may execute an operation the model did not
request.

Claudex applies a narrow fix only to a reproduced contract mismatch and keeps unknown
arguments visibly invalid. A compatibility layer repairs a contract; it must not invent
intent.

## 8. The bridge must not own approval

Claude Code already controls user authorization, sandboxing, and tool execution. Adding
approval semantics to a bridge creates two lifecycles: one side may consider an action
approved while the other does not, or a network retry may consume approval twice.

Claudex translates model protocol only. Permission, tool side effects, and approval stay
inside the Claude Code harness.

## Minimum acceptance baseline

A project claiming that “GPT works in Claude Code” should independently verify:

- Multi-turn function call/result pairing and reasoning continuation.
- Normal SSE completion, abnormal completion, duplicate terminal, and trailing payload.
- Lane isolation between main, each direct Agent, and reviewer.
- Empty Agent result, budget exhaustion, format repair, and foreground delivery.
- Compaction success, missing item, persistence failure, and restart recovery.
- Control progress while data capacity is saturated.
- Tool-schema repair that does not affect unrelated tools.

A one-turn chat screenshot, one successful command, or unit tests that skip the actual
state boundary are not evidence for this complete contract.
