"""SQLite-backed append-only mission state with idempotent events and checkpoints.

This is a durable orchestration primitive, not an LLM agent or tenant
authentication system. Callers must authenticate principals and authorize
namespace access before using the store.
"""
from __future__ import annotations
import hashlib
import json
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

_SCHEMA = 2
_MAX_NAMESPACE = 160
_MAX_ID = 160
_MAX_EVENT_TYPE = 120
_MAX_PAYLOAD_BYTES = 1_048_576
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TERMINAL = {"succeeded", "failed", "cancelled"}
_ALLOWED_STATUS = {"queued", "running", "waiting", "succeeded", "failed", "cancelled"}
_ALLOWED_TRANSITIONS = {"queued": {"running", "failed", "cancelled"}, "running": {"waiting", "succeeded", "failed", "cancelled"}, "waiting": {"running", "failed", "cancelled"}}

class MissionError(RuntimeError):
    """Base class for mission runtime errors."""
class MissionNotFoundError(MissionError):
    """The requested mission does not exist in the supplied namespace."""
class MissionConflictError(MissionError):
    """An optimistic version or idempotency precondition did not match."""
class MissionIntegrityError(MissionError):
    """Persisted event or checkpoint bytes failed integrity verification."""

def _text(value: str, name: str, limit: int) -> str:
    if not isinstance(value, str) or not value or len(value) > limit:
        raise ValueError(f"{name} must be non-empty text of at most {limit} characters")
    if any(ord(ch) < 32 for ch in value):
        raise ValueError(f"{name} contains control characters")
    return value

def canonical_json(value: Any) -> str:
    """Serialize JSON-compatible data deterministically; reject NaN and custom objects."""
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("value must be finite JSON-compatible data") from exc

def _digest(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()

@dataclass(frozen=True)
class Mission:
    namespace: str
    mission_id: str
    status: str
    version: int
    created_at: float
    updated_at: float

@dataclass(frozen=True)
class MissionEvent:
    namespace: str
    mission_id: str
    sequence: int
    event_id: str
    event_type: str
    status_after: str
    payload: Any
    payload_sha256: str
    idempotency_key: str
    created_at: float

@dataclass(frozen=True)
class Checkpoint:
    namespace: str
    mission_id: str
    checkpoint_id: str
    event_sequence: int
    state: Any
    state_sha256: str
    created_at: float

class MissionStore:
    """Transactional mission journal; SQLite BEGIN IMMEDIATE serializes writers.

    Mission.version is the last committed event sequence. Reusing an
    idempotency key is safe only when event type and canonical payload match.
    """
    def __init__(self, database: str | Path, *, busy_timeout_ms: int = 5000) -> None:
        if isinstance(busy_timeout_ms, bool) or not isinstance(busy_timeout_ms, int) or not 0 <= busy_timeout_ms <= 120_000:
            raise ValueError("busy_timeout_ms must be an integer from 0 to 120000")
        self.database = str(database)
        self.conn = sqlite3.connect(self.database, timeout=busy_timeout_ms / 1000, isolation_level=None, check_same_thread=True)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms}")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self._migrate()

    def close(self) -> None:
        self.conn.close()
    def __enter__(self) -> "MissionStore":
        return self
    def __exit__(self, *_: object) -> None:
        self.close()

    def _migrate(self) -> None:
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS schema_meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL);
        INSERT OR IGNORE INTO schema_meta(singleton, version) VALUES(1, 1);
        CREATE TABLE IF NOT EXISTS missions(
          namespace TEXT NOT NULL, mission_id TEXT NOT NULL, status TEXT NOT NULL,
          version INTEGER NOT NULL CHECK(version >= 0), created_at REAL NOT NULL,
          updated_at REAL NOT NULL, PRIMARY KEY(namespace, mission_id));
        CREATE TABLE IF NOT EXISTS mission_events(
          namespace TEXT NOT NULL, mission_id TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence > 0),
          event_id TEXT NOT NULL UNIQUE, event_type TEXT NOT NULL, status_after TEXT NOT NULL, payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL, idempotency_key TEXT NOT NULL, created_at REAL NOT NULL,
          PRIMARY KEY(namespace, mission_id, sequence),
          UNIQUE(namespace, mission_id, idempotency_key),
          FOREIGN KEY(namespace, mission_id) REFERENCES missions(namespace, mission_id) ON DELETE RESTRICT);
        CREATE TABLE IF NOT EXISTS mission_checkpoints(
          namespace TEXT NOT NULL, mission_id TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
          event_sequence INTEGER NOT NULL CHECK(event_sequence >= 0), state_json TEXT NOT NULL,
          state_sha256 TEXT NOT NULL, created_at REAL NOT NULL,
          PRIMARY KEY(namespace, mission_id, checkpoint_id),
          FOREIGN KEY(namespace, mission_id) REFERENCES missions(namespace, mission_id) ON DELETE RESTRICT);
        CREATE INDEX IF NOT EXISTS idx_events_recent ON mission_events(namespace, mission_id, sequence DESC);
        CREATE INDEX IF NOT EXISTS idx_checkpoints_recent ON mission_checkpoints(namespace, mission_id, event_sequence DESC);
        """)
        version = self.conn.execute("SELECT version FROM schema_meta WHERE singleton=1").fetchone()[0]
        if version == 1:
            columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(mission_events)")}
            if "status_after" not in columns:
                self.conn.execute("ALTER TABLE mission_events ADD COLUMN status_after TEXT NOT NULL DEFAULT 'queued'")
            self.conn.execute("UPDATE schema_meta SET version=2 WHERE singleton=1")
            version = 2
        if version != _SCHEMA:
            raise RuntimeError(f"unsupported mission database schema version: {version}")

    def create_mission(self, namespace: str, *, mission_id: str | None = None, initial_state: Mapping[str, Any] | None = None) -> Mission:
        namespace = _text(namespace, "namespace", _MAX_NAMESPACE)
        mission_id = _text(str(uuid.uuid4()) if mission_id is None else mission_id, "mission_id", _MAX_ID)
        if initial_state is not None and not isinstance(initial_state, Mapping):
            raise ValueError("initial_state must be a mapping or None")
        payload = canonical_json({} if initial_state is None else dict(initial_state))
        if len(payload.encode()) > _MAX_PAYLOAD_BYTES:
            raise ValueError("initial_state exceeds payload limit")
        now = time.time()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute("INSERT INTO missions VALUES(?, ?, 'queued', 0, ?, ?)", (namespace, mission_id, now, now))
            self.conn.execute("""INSERT INTO mission_events
                (namespace, mission_id, sequence, event_id, event_type, status_after,
                 payload_json, payload_sha256, idempotency_key, created_at)
                VALUES(?, ?, 1, ?, 'mission.created', 'queued', ?, ?, 'mission.created', ?)""",
                (namespace, mission_id, str(uuid.uuid4()), payload, _digest(payload), now))
            self.conn.execute("UPDATE missions SET version=1 WHERE namespace=? AND mission_id=?", (namespace, mission_id))
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return self.get_mission(namespace, mission_id)

    def get_mission(self, namespace: str, mission_id: str) -> Mission:
        namespace = _text(namespace, "namespace", _MAX_NAMESPACE)
        mission_id = _text(mission_id, "mission_id", _MAX_ID)
        row = self.conn.execute("SELECT * FROM missions WHERE namespace=? AND mission_id=?", (namespace, mission_id)).fetchone()
        if row is None:
            raise MissionNotFoundError("mission not found in namespace")
        return Mission(**dict(row))

    def append_event(self, namespace: str, mission_id: str, event_type: str, payload: Any, *,
                     expected_version: int, idempotency_key: str, status: str | None = None) -> MissionEvent:
        namespace = _text(namespace, "namespace", _MAX_NAMESPACE)
        mission_id = _text(mission_id, "mission_id", _MAX_ID)
        event_type = _text(event_type, "event_type", _MAX_EVENT_TYPE)
        idempotency_key = _text(idempotency_key, "idempotency_key", _MAX_ID)
        if isinstance(expected_version, bool) or not isinstance(expected_version, int) or expected_version < 0:
            raise ValueError("expected_version must be a non-negative integer")
        if status is not None and status not in _ALLOWED_STATUS:
            raise ValueError("invalid mission status")
        payload_json = canonical_json(payload)
        if len(payload_json.encode()) > _MAX_PAYLOAD_BYTES:
            raise ValueError("event payload exceeds payload limit")
        payload_hash = _digest(payload_json)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            existing = self.conn.execute("SELECT * FROM mission_events WHERE namespace=? AND mission_id=? AND idempotency_key=?",
                                         (namespace, mission_id, idempotency_key)).fetchone()
            if existing is not None:
                if (existing["event_type"] != event_type or existing["payload_json"] != payload_json
                        or (status is not None and existing["status_after"] != status)):
                    raise MissionConflictError("idempotency key reused with different event data")
                self.conn.commit()
                return self._event(existing)
            mission = self.conn.execute("SELECT * FROM missions WHERE namespace=? AND mission_id=?",
                                        (namespace, mission_id)).fetchone()
            if mission is None:
                raise MissionNotFoundError("mission not found in namespace")
            if mission["version"] != expected_version:
                raise MissionConflictError("mission version changed; reload before appending")
            if mission["status"] not in _ALLOWED_STATUS:
                raise MissionIntegrityError("persisted mission status is invalid")
            if mission["status"] in _TERMINAL:
                raise MissionConflictError("terminal mission cannot accept new events")
            next_status = status or mission["status"]
            if next_status != mission["status"] and next_status not in _ALLOWED_TRANSITIONS[mission["status"]]:
                raise MissionConflictError(f"invalid mission status transition: {mission['status']} -> {next_status}")
            sequence, now, event_id = mission["version"] + 1, time.time(), str(uuid.uuid4())
            self.conn.execute("""INSERT INTO mission_events
                (namespace, mission_id, sequence, event_id, event_type, status_after,
                 payload_json, payload_sha256, idempotency_key, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (namespace, mission_id, sequence, event_id, event_type, next_status,
                 payload_json, payload_hash, idempotency_key, now))
            self.conn.execute("UPDATE missions SET status=?, version=?, updated_at=? WHERE namespace=? AND mission_id=? AND version=?",
                              (next_status, sequence, now, namespace, mission_id, expected_version))
            if self.conn.execute("SELECT changes()").fetchone()[0] != 1:
                raise MissionConflictError("concurrent mission update")
            self.conn.commit()
            return MissionEvent(namespace, mission_id, sequence, event_id, event_type, next_status,
                                json.loads(payload_json), payload_hash, idempotency_key, now)
        except Exception:
            self.conn.rollback()
            raise

    def events(self, namespace: str, mission_id: str, *, after_sequence: int = 0, limit: int = 100) -> list[MissionEvent]:
        namespace = _text(namespace, "namespace", _MAX_NAMESPACE)
        mission_id = _text(mission_id, "mission_id", _MAX_ID)
        if isinstance(after_sequence, bool) or not isinstance(after_sequence, int) or after_sequence < 0:
            raise ValueError("after_sequence must be a non-negative integer")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("limit must be an integer from 1 to 1000")
        rows = self.conn.execute("SELECT * FROM mission_events WHERE namespace=? AND mission_id=? AND sequence>? ORDER BY sequence LIMIT ?",
                                 (namespace, mission_id, after_sequence, limit)).fetchall()
        return [self._event(row) for row in rows]

    @staticmethod
    def _event(row: sqlite3.Row) -> MissionEvent:
        raw = row["payload_json"]
        if not _SHA256_RE.fullmatch(row["payload_sha256"]) or _digest(raw) != row["payload_sha256"]:
            raise MissionIntegrityError("event payload digest mismatch")
        return MissionEvent(row["namespace"], row["mission_id"], row["sequence"], row["event_id"],
                            row["event_type"], row["status_after"], json.loads(raw), row["payload_sha256"],
                            row["idempotency_key"], row["created_at"])

    def checkpoint(self, namespace: str, mission_id: str, state: Any, *, expected_version: int,
                   checkpoint_id: str | None = None) -> Checkpoint:
        namespace = _text(namespace, "namespace", _MAX_NAMESPACE)
        mission_id = _text(mission_id, "mission_id", _MAX_ID)
        if isinstance(expected_version, bool) or not isinstance(expected_version, int) or expected_version < 0:
            raise ValueError("expected_version must be a non-negative integer")
        raw = canonical_json(state)
        if len(raw.encode()) > _MAX_PAYLOAD_BYTES:
            raise ValueError("checkpoint state exceeds payload limit")
        checkpoint_id = _text(checkpoint_id or str(uuid.uuid4()), "checkpoint_id", _MAX_ID)
        digest, now = _digest(raw), time.time()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            mission = self.conn.execute("SELECT version FROM missions WHERE namespace=? AND mission_id=?",
                                        (namespace, mission_id)).fetchone()
            if mission is None:
                raise MissionNotFoundError("mission not found in namespace")
            if mission["version"] != expected_version:
                raise MissionConflictError("checkpoint version is stale")
            self.conn.execute("INSERT INTO mission_checkpoints VALUES(?, ?, ?, ?, ?, ?, ?)",
                              (namespace, mission_id, checkpoint_id, expected_version, raw, digest, now))
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return Checkpoint(namespace, mission_id, checkpoint_id, expected_version, json.loads(raw), digest, now)

    def restore_checkpoint(self, namespace: str, mission_id: str, checkpoint_id: str) -> Checkpoint:
        namespace = _text(namespace, "namespace", _MAX_NAMESPACE)
        mission_id = _text(mission_id, "mission_id", _MAX_ID)
        checkpoint_id = _text(checkpoint_id, "checkpoint_id", _MAX_ID)
        row = self.conn.execute("SELECT * FROM mission_checkpoints WHERE namespace=? AND mission_id=? AND checkpoint_id=?",
                                (namespace, mission_id, checkpoint_id)).fetchone()
        if row is None:
            raise MissionNotFoundError("checkpoint not found in namespace")
        raw = row["state_json"]
        if not _SHA256_RE.fullmatch(row["state_sha256"]) or _digest(raw) != row["state_sha256"]:
            raise MissionIntegrityError("checkpoint state digest mismatch")
        return Checkpoint(row["namespace"], row["mission_id"], row["checkpoint_id"], row["event_sequence"],
                          json.loads(raw), row["state_sha256"], row["created_at"])
