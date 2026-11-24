from __future__ import annotations
import tempfile
import unittest
from pathlib import Path
from synergy_agent_long_term_memory.runtime import MissionConflictError, MissionIntegrityError, MissionNotFoundError, MissionStore

class MissionRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = MissionStore(Path(self.tmp.name) / "missions.sqlite")
        self.store.create_mission("tenant-a", mission_id="mission-1")
    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()
    def test_create_persists_initial_event_and_version(self):
        mission = self.store.get_mission("tenant-a", "mission-1")
        self.assertEqual((mission.status, mission.version), ("queued", 1))
        self.assertEqual([e.event_type for e in self.store.events("tenant-a", "mission-1")], ["mission.created"])
    def test_append_optimistic_version_and_sequence(self):
        event = self.store.append_event("tenant-a", "mission-1", "task.started", {"task_id":"t1"},
                                        expected_version=1, idempotency_key="start-t1", status="running")
        self.assertEqual(event.sequence, 2)
        self.assertEqual(self.store.get_mission("tenant-a", "mission-1").status, "running")
        with self.assertRaises(MissionConflictError):
            self.store.append_event("tenant-a", "mission-1", "task.done", {}, expected_version=1, idempotency_key="done")
    def test_idempotent_retry_returns_same_event(self):
        kw = dict(namespace="tenant-a", mission_id="mission-1", event_type="tool.completed",
                  payload={"tool":"pytest","exit_code":0}, expected_version=1, idempotency_key="tool-1")
        first = self.store.append_event(**kw)
        retry = self.store.append_event(**kw)
        self.assertEqual((first.event_id, first.sequence), (retry.event_id, retry.sequence))
        self.assertEqual(retry.status_after, "queued")
    def test_idempotency_key_cannot_change_payload(self):
        self.store.append_event("tenant-a","mission-1","tool.completed",{"exit_code":0},expected_version=1,idempotency_key="tool")
        with self.assertRaises(MissionConflictError):
            self.store.append_event("tenant-a","mission-1","tool.completed",{"exit_code":1},expected_version=1,idempotency_key="tool")
    def test_namespace_isolation(self):
        with self.assertRaises(MissionNotFoundError):
            self.store.get_mission("tenant-b", "mission-1")
        self.store.checkpoint("tenant-a","mission-1",{"cursor":7},expected_version=1,checkpoint_id="cp")
        with self.assertRaises(MissionNotFoundError):
            self.store.restore_checkpoint("tenant-b","mission-1","cp")
        self.assertEqual(self.store.restore_checkpoint("tenant-a","mission-1","cp").state, {"cursor":7})
    def test_stale_checkpoint_rejected(self):
        self.store.append_event("tenant-a","mission-1","task.started",{},expected_version=1,idempotency_key="start")
        with self.assertRaises(MissionConflictError):
            self.store.checkpoint("tenant-a","mission-1",{},expected_version=1)
    def test_terminal_mission_rejects_writes(self):
        self.store.append_event("tenant-a","mission-1","mission.started",{},expected_version=1,idempotency_key="start",status="running")
        completed = self.store.append_event("tenant-a","mission-1","mission.completed",{},expected_version=2,idempotency_key="done",status="succeeded")
        self.assertEqual(completed.status_after, "succeeded")
        with self.assertRaises(MissionConflictError):
            self.store.append_event("tenant-a","mission-1","late.event",{},expected_version=3,idempotency_key="late")
    def test_invalid_status_transition_is_rejected(self):
        with self.assertRaises(MissionConflictError):
            self.store.append_event("tenant-a","mission-1","mission.completed",{},expected_version=1,idempotency_key="skip",status="succeeded")
    def test_event_tampering_detected(self):
        self.store.append_event("tenant-a","mission-1","task.started",{"task":"t"},expected_version=1,idempotency_key="start")
        self.store.conn.execute("UPDATE mission_events SET payload_json='{}' WHERE sequence=2")
        with self.assertRaises(MissionIntegrityError):
            self.store.events("tenant-a","mission-1")
    def test_checkpoint_tampering_detected(self):
        self.store.checkpoint("tenant-a","mission-1",{"safe":True},expected_version=1,checkpoint_id="cp")
        self.store.conn.execute("UPDATE mission_checkpoints SET state_json='{}' WHERE checkpoint_id='cp'")
        with self.assertRaises(MissionIntegrityError):
            self.store.restore_checkpoint("tenant-a","mission-1","cp")
    def test_invalid_values_rejected(self):
        with self.assertRaises(ValueError):
            self.store.append_event("tenant-a","mission-1","bad",{},expected_version=True,idempotency_key="bad")
        with self.assertRaises(ValueError):
            self.store.append_event("tenant-a","mission-1","bad",{"nan":float("nan")},expected_version=1,idempotency_key="nan")

if __name__ == "__main__":
    unittest.main()
