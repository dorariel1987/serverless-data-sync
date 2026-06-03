"""Queue abstraction.

The pipeline talks to this interface so that the same orchestration code runs
against an in-memory queue (tests/demo) and Azure Storage Queues (production).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class QueueMessage:
    body: str
    id: str | None = None
    dequeue_count: int = 1


class Queue(ABC):
    @abstractmethod
    def send(self, body: str, *, delay_seconds: float = 0.0) -> None:
        ...

    @abstractmethod
    def receive(self, max_messages: int = 1) -> list[QueueMessage]:
        ...

    @abstractmethod
    def complete(self, message: QueueMessage) -> None:
        """Remove a successfully processed message from the queue."""

    @abstractmethod
    def approximate_count(self) -> int:
        ...
