"""Core domain models for the sync layer.

These models are intentionally plain dataclasses with explicit (de)serialization
so they can travel through Azure Storage Queues (JSON strings) and be indexed in
OpenSearch without coupling to any SDK.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id() -> str:
    return uuid.uuid4().hex


def checksum(payload: dict[str, Any]) -> str:
    """Stable content hash used to detect drift during reconciliation."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class Operation(str, Enum):
    UPSERT = "upsert"
    DELETE = "delete"


class SyncStatus(str, Enum):
    SUCCESS = "success"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"
    SKIPPED = "skipped"


@dataclass
class Record:
    """A single record as it exists in a source or target SaaS system."""

    id: str
    payload: dict[str, Any] = field(default_factory=dict)
    updated_at: str = field(default_factory=utcnow)

    @property
    def checksum(self) -> str:
        return checksum(self.payload)

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.id, "payload": self.payload, "updated_at": self.updated_at}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Record":
        return cls(
            id=data["id"],
            payload=data.get("payload", {}),
            updated_at=data.get("updated_at", utcnow()),
        )


@dataclass
class SyncEvent:
    """A unit of work flowing through the queue.

    `attempt` is incremented every time the message is re-enqueued after a
    retriable failure. Once it crosses the configured max it is dead-lettered.
    """

    record_id: str
    source: str
    target: str
    operation: Operation = Operation.UPSERT
    event_id: str = field(default_factory=new_id)
    attempt: int = 1
    enqueued_at: str = field(default_factory=utcnow)
    correlation_id: str = field(default_factory=new_id)
    # Optional inline payload for systems that push full records via webhook.
    payload: dict[str, Any] | None = None

    def to_json(self) -> str:
        data = asdict(self)
        data["operation"] = self.operation.value
        return json.dumps(data, default=str)

    @classmethod
    def from_json(cls, raw: str) -> "SyncEvent":
        data = json.loads(raw)
        return cls(
            record_id=data["record_id"],
            source=data["source"],
            target=data["target"],
            operation=Operation(data.get("operation", "upsert")),
            event_id=data.get("event_id", new_id()),
            attempt=int(data.get("attempt", 1)),
            enqueued_at=data.get("enqueued_at", utcnow()),
            correlation_id=data.get("correlation_id", new_id()),
            payload=data.get("payload"),
        )

    def next_attempt(self) -> "SyncEvent":
        return SyncEvent(
            record_id=self.record_id,
            source=self.source,
            target=self.target,
            operation=self.operation,
            event_id=self.event_id,
            attempt=self.attempt + 1,
            enqueued_at=utcnow(),
            correlation_id=self.correlation_id,
            payload=self.payload,
        )


@dataclass
class SyncResult:
    event: SyncEvent
    status: SyncStatus
    error: str | None = None
    duration_ms: float = 0.0
    target_checksum: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event.event_id,
            "correlation_id": self.event.correlation_id,
            "record_id": self.event.record_id,
            "source": self.event.source,
            "target": self.event.target,
            "operation": self.event.operation.value,
            "attempt": self.event.attempt,
            "status": self.status.value,
            "error": self.error,
            "duration_ms": round(self.duration_ms, 3),
            "target_checksum": self.target_checksum,
            "ts": utcnow(),
        }


class RetriableError(Exception):
    """Raised by connectors for transient failures that should be retried."""


class FatalError(Exception):
    """Raised for non-retriable failures that should go straight to the DLQ."""
