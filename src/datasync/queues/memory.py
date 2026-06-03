"""In-memory queue with visibility-delay semantics for local runs and tests."""
from __future__ import annotations

import threading
import time
from collections import deque

from .base import Queue, QueueMessage
from ..models import new_id


class InMemoryQueue(Queue):
    """A minimal queue that mimics the parts of Azure Storage Queues we rely on:

    - at-least-once delivery
    - per-message dequeue count
    - visibility delay (used to honor retry backoff)
    """

    def __init__(self, name: str = "in-memory") -> None:
        self.name = name
        self._items: deque[tuple[float, QueueMessage]] = deque()
        self._lock = threading.Lock()

    def send(self, body: str, *, delay_seconds: float = 0.0) -> None:
        visible_at = time.monotonic() + max(0.0, delay_seconds)
        msg = QueueMessage(body=body, id=new_id(), dequeue_count=0)
        with self._lock:
            self._items.append((visible_at, msg))

    def receive(self, max_messages: int = 1) -> list[QueueMessage]:
        now = time.monotonic()
        out: list[QueueMessage] = []
        with self._lock:
            remaining: deque[tuple[float, QueueMessage]] = deque()
            while self._items and len(out) < max_messages:
                visible_at, msg = self._items.popleft()
                if visible_at <= now:
                    msg.dequeue_count += 1
                    out.append(msg)
                else:
                    remaining.append((visible_at, msg))
            remaining.extend(self._items)
            self._items = remaining
        return out

    def complete(self, message: QueueMessage) -> None:
        # Received messages are already removed from the deque; nothing to do.
        return None

    def approximate_count(self) -> int:
        with self._lock:
            return len(self._items)
