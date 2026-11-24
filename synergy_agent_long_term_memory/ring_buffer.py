"""Thread-safe bounded ring buffer for recent agent events.

The buffer is a low-latency working set only. It never silently overwrites
unconsumed entries; producers receive backpressure when capacity is exhausted.
Durable recovery must use MissionStore checkpoints and the event journal.
"""
from __future__ import annotations

import threading
import time
from typing import Generic, TypeVar

T = TypeVar("T")


class BufferFullError(RuntimeError):
    """The bounded buffer is full and the caller selected non-blocking behavior."""


class BufferClosedError(RuntimeError):
    """The buffer has been closed and cannot accept or provide more entries."""


class AgentRingBuffer(Generic[T]):
    """Fixed-capacity FIFO ring with O(1) put/get and condition-based backpressure.

    A single instance represents one explicitly scoped stream. Callers should
    create separate instances for each namespace/mission/agent scope rather
    than multiplexing security domains into one queue.
    """

    def __init__(self, capacity: int) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        self.capacity = capacity
        self._slots: list[T | None] = [None] * capacity
        self._head = 0
        self._size = 0
        self._closed = False
        self._condition = threading.Condition()

    def __len__(self) -> int:
        with self._condition:
            return self._size

    @property
    def closed(self) -> bool:
        with self._condition:
            return self._closed

    def put(self, item: T, *, block: bool = True, timeout: float | None = None) -> None:
        if item is None:
            raise ValueError("None is reserved and cannot be buffered")
        if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout < 0):
            raise ValueError("timeout must be a non-negative number or None")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._size == self.capacity and not self._closed:
                if not block:
                    raise BufferFullError("agent ring buffer is full")
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise BufferFullError("timed out waiting for ring buffer capacity")
                self._condition.wait(remaining)
            if self._closed:
                raise BufferClosedError("agent ring buffer is closed")
            tail = (self._head + self._size) % self.capacity
            self._slots[tail] = item
            self._size += 1
            self._condition.notify_all()

    def get(self, *, block: bool = True, timeout: float | None = None) -> T:
        if timeout is not None and (isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout < 0):
            raise ValueError("timeout must be a non-negative number or None")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while self._size == 0 and not self._closed:
                if not block:
                    raise BufferFullError("agent ring buffer is empty")
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise BufferFullError("timed out waiting for a ring buffer item")
                self._condition.wait(remaining)
            if self._size == 0:
                raise BufferClosedError("agent ring buffer is closed and drained")
            item = self._slots[self._head]
            self._slots[self._head] = None
            self._head = (self._head + 1) % self.capacity
            self._size -= 1
            self._condition.notify_all()
            return item  # type: ignore[return-value]

    def drain(self, limit: int | None = None) -> list[T]:
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0):
            raise ValueError("limit must be a non-negative integer or None")
        with self._condition:
            count = self._size if limit is None else min(self._size, limit)
            result: list[T] = []
            for _ in range(count):
                item = self._slots[self._head]
                self._slots[self._head] = None
                self._head = (self._head + 1) % self.capacity
                self._size -= 1
                result.append(item)  # type: ignore[arg-type]
            if count:
                self._condition.notify_all()
            return result

    def snapshot(self) -> tuple[T, ...]:
        """Return an ordered, non-destructive snapshot of buffered entries."""
        with self._condition:
            return tuple(
                self._slots[(self._head + offset) % self.capacity]  # type: ignore[misc]
                for offset in range(self._size)
            )

    def close(self) -> None:
        """Reject future puts; allow consumers to drain already-buffered items."""
        with self._condition:
            self._closed = True
            self._condition.notify_all()
