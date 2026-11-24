from __future__ import annotations

import threading
import time
import unittest

from synergy_agent_long_term_memory.ring_buffer import (
    AgentRingBuffer, BufferClosedError, BufferFullError,
)


class AgentRingBufferTests(unittest.TestCase):
    def test_fifo_wraparound_and_snapshot(self):
        ring = AgentRingBuffer[int](3)
        ring.put(1)
        ring.put(2)
        self.assertEqual(ring.get(), 1)
        ring.put(3)
        ring.put(4)
        self.assertEqual(ring.snapshot(), (2, 3, 4))
        self.assertEqual(ring.drain(2), [2, 3])
        self.assertEqual(ring.get(), 4)

    def test_full_buffer_never_overwrites_unconsumed_events(self):
        ring = AgentRingBuffer[str](1)
        ring.put("durable-event-ref")
        with self.assertRaises(BufferFullError):
            ring.put("new-event", block=False)
        self.assertEqual(ring.get(), "durable-event-ref")

    def test_timeout_and_empty_nonblocking(self):
        ring = AgentRingBuffer[int](1)
        with self.assertRaises(BufferFullError):
            ring.get(block=False)
        with self.assertRaises(BufferFullError):
            ring.get(timeout=0.001)

    def test_blocked_producer_resumes_after_consumer_drains(self):
        ring = AgentRingBuffer[int](1)
        ring.put(1)
        result = []
        producer = threading.Thread(target=lambda: (ring.put(2, timeout=1), result.append("put")))
        producer.start()
        time.sleep(0.01)
        self.assertEqual(ring.get(), 1)
        producer.join(1)
        self.assertFalse(producer.is_alive())
        self.assertEqual(result, ["put"])
        self.assertEqual(ring.get(), 2)

    def test_close_allows_drain_then_rejects(self):
        ring = AgentRingBuffer[int](2)
        ring.put(1)
        ring.close()
        self.assertEqual(ring.get(), 1)
        with self.assertRaises(BufferClosedError):
            ring.get(block=False)
        with self.assertRaises(BufferClosedError):
            ring.put(2)

    def test_validation(self):
        with self.assertRaises(ValueError):
            AgentRingBuffer(True)
        ring = AgentRingBuffer(1)
        with self.assertRaises(ValueError):
            ring.put(None)
        with self.assertRaises(ValueError):
            ring.drain(-1)


if __name__ == "__main__":
    unittest.main()
