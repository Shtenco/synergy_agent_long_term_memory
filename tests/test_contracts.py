from __future__ import annotations

import unittest

from synergy_agent_long_term_memory.contracts import EngineeringStateTransition, SCHEMA_VERSION


class EngineeringTransitionContractTests(unittest.TestCase):
    def valid(self):
        return EngineeringStateTransition(
            event_id="evt-1", namespace="tenant-a", mission_id="mission-1",
            agent_id="coder-1", repository_id="Shtenco/synergy_ai_coder",
            task_id="task-1", sequence=12, transition="commit.created",
            occurred_at=1795540000.0, memory_version=7, checkpoint_id="cp-7",
            commit_sha="a" * 40, evidence_refs=("ci://run/123", "sha256:" + "b" * 64),
        )

    def test_round_trip_is_stable(self):
        item = self.valid()
        decoded = EngineeringStateTransition.from_dict(item.to_dict())
        self.assertEqual(decoded, item)
        self.assertEqual(decoded.schema_version, SCHEMA_VERSION)
        self.assertEqual(decoded.to_json(), item.to_json())

    def test_unknown_schema_and_extra_fields_fail_closed(self):
        data = self.valid().to_dict()
        data["schema_version"] = "synergy.engineering-state-transition/v2"
        with self.assertRaises(ValueError):
            EngineeringStateTransition.from_dict(data)
        data = self.valid().to_dict()
        data["unrecognized"] = True
        with self.assertRaises(ValueError):
            EngineeringStateTransition.from_dict(data)

    def test_namespace_and_sequence_validation(self):
        data = self.valid().to_dict()
        data["namespace"] = "tenant a"
        with self.assertRaises(ValueError):
            EngineeringStateTransition.from_dict(data)
        data = self.valid().to_dict()
        data["sequence"] = True
        with self.assertRaises(ValueError):
            EngineeringStateTransition.from_dict(data)

    def test_commit_sha_and_duplicate_evidence_validation(self):
        data = self.valid().to_dict()
        data["commit_sha"] = "deadbeef"
        with self.assertRaises(ValueError):
            EngineeringStateTransition.from_dict(data)
        data = self.valid().to_dict()
        data["evidence_refs"] = ["same", "same"]
        with self.assertRaises(ValueError):
            EngineeringStateTransition.from_dict(data)


if __name__ == "__main__":
    unittest.main()
