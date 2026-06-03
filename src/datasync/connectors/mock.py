"""In-memory connector used for local development and tests.

Supports deterministic failure injection so we can exercise the retry,
dead-letter and reconciliation paths without any external SaaS dependency.
"""
from __future__ import annotations

import copy
from typing import Iterable

from ..models import FatalError, Record, RetriableError, utcnow


class MockConnector:
    """A fake SaaS system backed by a dict.

    Failure injection:
        fail_upsert_ids:  ids that raise RetriableError on every upsert
        fatal_ids:        ids that raise FatalError on upsert
        flaky_ids:        ids that fail `flaky_times` then succeed (transient)
    """

    name = "mock"

    def __init__(
        self,
        name: str = "mock",
        records: dict[str, Record] | None = None,
        *,
        fail_upsert_ids: set[str] | None = None,
        fatal_ids: set[str] | None = None,
        flaky_ids: set[str] | None = None,
        flaky_times: int = 2,
    ) -> None:
        self.name = name
        self._store: dict[str, Record] = records or {}
        self.fail_upsert_ids = fail_upsert_ids or set()
        self.fatal_ids = fatal_ids or set()
        self.flaky_ids = flaky_ids or set()
        self.flaky_times = flaky_times
        self._flaky_counts: dict[str, int] = {}
        self.calls: list[tuple[str, str]] = []

    def seed(self, record: Record) -> None:
        self._store[record.id] = copy.deepcopy(record)

    def get_record(self, record_id: str) -> Record | None:
        self.calls.append(("get", record_id))
        rec = self._store.get(record_id)
        return copy.deepcopy(rec) if rec else None

    def list_records(self) -> Iterable[Record]:
        self.calls.append(("list", "*"))
        return [copy.deepcopy(r) for r in self._store.values()]

    def upsert_record(self, record: Record) -> Record:
        self.calls.append(("upsert", record.id))

        if record.id in self.fatal_ids:
            raise FatalError(f"{self.name}: record {record.id} is permanently invalid")

        if record.id in self.fail_upsert_ids:
            raise RetriableError(f"{self.name}: upstream 503 for {record.id}")

        if record.id in self.flaky_ids:
            seen = self._flaky_counts.get(record.id, 0)
            if seen < self.flaky_times:
                self._flaky_counts[record.id] = seen + 1
                raise RetriableError(
                    f"{self.name}: transient timeout for {record.id} "
                    f"({seen + 1}/{self.flaky_times})"
                )

        stored = copy.deepcopy(record)
        stored.updated_at = utcnow()
        self._store[record.id] = stored
        return copy.deepcopy(stored)

    def delete_record(self, record_id: str) -> None:
        self.calls.append(("delete", record_id))
        self._store.pop(record_id, None)
