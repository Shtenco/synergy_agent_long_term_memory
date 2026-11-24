"""Versioned cross-repository event envelope for SYNERGY engineering state."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Mapping

from .runtime import canonical_json

SCHEMA_VERSION = "synergy.engineering-state-transition/v1"
_SHA40 = re.compile(r"^[0-9a-f]{40}$")


def _identifier(value: Any, name: str, limit: int = 160) -> str:
    if not isinstance(value, str) or not value or len(value) > limit or any(ch.isspace() for ch in value):
        raise ValueError(f"{name} must be non-empty whitespace-free text (max {limit})")
    if any(ord(ch) < 32 for ch in value):
        raise ValueError(f"{name} contains control characters")
    return value


@dataclass(frozen=True)
class EngineeringStateTransition:
    """Commit- and sequence-addressable event shared by Coder, Agent and Memory."""

    event_id: str
    namespace: str
    mission_id: str
    agent_id: str
    repository_id: str
    task_id: str
    sequence: int
    transition: str
    occurred_at: float
    memory_version: int | None = None
    checkpoint_id: str | None = None
    commit_sha: str | None = None
    evidence_refs: tuple[str, ...] = ()
    schema_version: str = SCHEMA_VERSION

    def validate(self) -> "EngineeringStateTransition":
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError("unsupported engineering transition schema version")
        for name in ("event_id", "namespace", "mission_id", "agent_id", "repository_id", "task_id", "transition"):
            _identifier(getattr(self, name), name)
        if isinstance(self.sequence, bool) or not isinstance(self.sequence, int) or self.sequence < 1:
            raise ValueError("sequence must be a positive integer")
        if isinstance(self.occurred_at, bool) or not isinstance(self.occurred_at, (int, float)) or not math.isfinite(self.occurred_at):
            raise ValueError("occurred_at must be a finite timestamp")
        if self.memory_version is not None and (
            isinstance(self.memory_version, bool) or not isinstance(self.memory_version, int) or self.memory_version < 0
        ):
            raise ValueError("memory_version must be a non-negative integer or None")
        if self.checkpoint_id is not None:
            _identifier(self.checkpoint_id, "checkpoint_id")
        if self.commit_sha is not None and not _SHA40.fullmatch(self.commit_sha):
            raise ValueError("commit_sha must be a full 40-character lowercase Git SHA")
        if not isinstance(self.evidence_refs, tuple) or len(self.evidence_refs) > 256:
            raise ValueError("evidence_refs must be a tuple containing at most 256 references")
        for ref in self.evidence_refs:
            _identifier(ref, "evidence_ref", 2048)
        if len(set(self.evidence_refs)) != len(self.evidence_refs):
            raise ValueError("evidence_refs must be unique")
        return self

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "namespace": self.namespace,
            "mission_id": self.mission_id,
            "agent_id": self.agent_id,
            "repository_id": self.repository_id,
            "task_id": self.task_id,
            "sequence": self.sequence,
            "transition": self.transition,
            "occurred_at": self.occurred_at,
            "memory_version": self.memory_version,
            "checkpoint_id": self.checkpoint_id,
            "commit_sha": self.commit_sha,
            "evidence_refs": list(self.evidence_refs),
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "EngineeringStateTransition":
        if not isinstance(value, Mapping):
            raise ValueError("transition must be a mapping")
        allowed = {
            "schema_version", "event_id", "namespace", "mission_id", "agent_id",
            "repository_id", "task_id", "sequence", "transition", "occurred_at",
            "memory_version", "checkpoint_id", "commit_sha", "evidence_refs",
        }
        if set(value) != allowed:
            raise ValueError("transition fields do not match the v1 contract")
        refs = value["evidence_refs"]
        if not isinstance(refs, list):
            raise ValueError("evidence_refs must be a JSON array")
        result = cls(
            schema_version=value["schema_version"],
            event_id=value["event_id"],
            namespace=value["namespace"],
            mission_id=value["mission_id"],
            agent_id=value["agent_id"],
            repository_id=value["repository_id"],
            task_id=value["task_id"],
            sequence=value["sequence"],
            transition=value["transition"],
            occurred_at=value["occurred_at"],
            memory_version=value["memory_version"],
            checkpoint_id=value["checkpoint_id"],
            commit_sha=value["commit_sha"],
            evidence_refs=tuple(refs),
        )
        return result.validate()
