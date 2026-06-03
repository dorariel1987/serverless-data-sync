"""The sync engine: process one SyncEvent against source/target connectors.

Pure business logic - no queue/Azure knowledge. It returns a SyncResult and lets
the pipeline decide whether to re-enqueue (retry) or dead-letter.
"""
from __future__ import annotations

import time
from typing import Callable

from ..connectors.base import Connector
from ..models import (
    FatalError,
    Operation,
    Record,
    RetriableError,
    SyncEvent,
    SyncResult,
    SyncStatus,
)

# A transform maps a source record payload to the target's shape.
Transform = Callable[[Record], Record]


def identity_transform(record: Record) -> Record:
    return record


class SyncEngine:
    def __init__(
        self,
        connectors: dict[str, Connector],
        *,
        transform: Transform = identity_transform,
    ) -> None:
        self._connectors = connectors
        self._transform = transform

    def _connector(self, name: str) -> Connector:
        try:
            return self._connectors[name]
        except KeyError as exc:
            raise FatalError(f"unknown system '{name}'") from exc

    def process(self, event: SyncEvent) -> SyncResult:
        started = time.perf_counter()
        try:
            result = self._do_process(event)
            result.duration_ms = (time.perf_counter() - started) * 1000
            return result
        except FatalError as exc:
            return SyncResult(
                event=event,
                status=SyncStatus.DEAD_LETTER,
                error=f"fatal: {exc}",
                duration_ms=(time.perf_counter() - started) * 1000,
            )
        except RetriableError as exc:
            return SyncResult(
                event=event,
                status=SyncStatus.RETRY,
                error=f"retriable: {exc}",
                duration_ms=(time.perf_counter() - started) * 1000,
            )
        except Exception as exc:  # unexpected -> treat as retriable once
            return SyncResult(
                event=event,
                status=SyncStatus.RETRY,
                error=f"unexpected: {exc}",
                duration_ms=(time.perf_counter() - started) * 1000,
            )

    def _do_process(self, event: SyncEvent) -> SyncResult:
        source = self._connector(event.source)
        target = self._connector(event.target)

        if event.operation is Operation.DELETE:
            target.delete_record(event.record_id)
            return SyncResult(event=event, status=SyncStatus.SUCCESS)

        # UPSERT: prefer the inline payload (webhook push), else pull from source.
        if event.payload is not None:
            record = Record(id=event.record_id, payload=event.payload)
        else:
            record = source.get_record(event.record_id)

        if record is None:
            # Source no longer has the record -> nothing to sync. Idempotent skip.
            return SyncResult(event=event, status=SyncStatus.SKIPPED)

        transformed = self._transform(record)
        written = target.upsert_record(transformed)
        return SyncResult(
            event=event,
            status=SyncStatus.SUCCESS,
            target_checksum=written.checksum,
        )
