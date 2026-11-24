# SYNERGY Long-Term Agent Runtime

Persistent mission orchestration primitives for long-running agents. This repository is provider-independent: it stores mission state, append-only events, and checkpoints; it does not own model prompts, tool execution, or semantic-memory policy.

## Implemented foundation

- SQLite WAL-backed mission journal with synchronous=FULL and foreign keys.
- Namespace-scoped mission identity and reads.
- Monotonic per-mission event sequence and optimistic concurrency.
- Idempotent event append; changed payload under a reused key is rejected.
- SHA-256 verification for persisted event payloads and checkpoint state.
- Version-bound checkpoints and deterministic JSON serialization.
- Bounded payload sizes, query limits, strict integer validation, and terminal-state write protection.
- Python 3.11+, standard library only.

## Quick start

    from synergy_agent_long_term_memory import MissionStore

    with MissionStore("agent-state.sqlite") as store:
        mission = store.create_mission("tenant-42", mission_id="research-001")
        event = store.append_event(
            "tenant-42", mission.mission_id, "mission.started",
            {"goal": "audit repository"},
            expected_version=mission.version,
            idempotency_key="start:research-001", status="running",
        )
        store.checkpoint(
            "tenant-42", mission.mission_id, {"task_cursor": "inspect-readme"},
            expected_version=event.sequence,
        )

## Integration boundaries

- Long-Term Agent owns mission lifecycle, durable event journal, progress checkpoints, and recovery orchestration.
- Hierarchical Cognitive Memory owns versioned System Core, trust-controlled semantic records, RAG, graph retrieval, provenance, and consolidation.
- AI Coder owns repository worktrees, patches, command policy, test evidence, review contracts, and release candidates.
- Ring Buffer owns bounded low-latency recent-event windows. It is not the durable source of truth; a full buffer blocks or raises instead of silently overwriting unconsumed entries.

The shared integration envelope should carry namespace, mission_id, agent_id, repository_id, task_id, event_id, sequence, memory_version, checkpoint_id, commit_sha, and evidence references. Unknown schema versions must be rejected.

## Security and correctness boundaries

Namespace filtering is a data partition, not authentication or authorization. Authenticate callers and authorize namespace access at the service boundary. SHA-256 detects persisted-byte changes relative to the stored digest; it does not authenticate writers or prove claims. SQLite WAL is not a backup strategy. A MissionStore connection is thread-confined; use one connection per thread/process and rely on SQLite transactions for cross-connection writer serialization. Do not expose the database or future API to an untrusted network.

This is a first durable-runtime milestone, not a complete autonomous agent. No model provider, task DAG scheduler, retry engine, subagent executor, ring buffer, or cross-repository transaction coordinator is claimed as implemented.

## Validation

Run python -m unittest discover -s tests -v or pytest -q. CI evidence must be tied to the exact commit SHA. A queued job, missing steps, or missing logs is NOT_RUN, not PASS.
