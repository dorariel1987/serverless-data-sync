"""Reconciliation: detect and repair drift between source and target.

Runs on a timer. Compares the full record set of source vs target by checksum
and emits corrective SyncEvents:
    - missing in target          -> upsert
    - checksum mismatch (drift)  -> upsert
    - present in target, gone in source -> delete
"""
from __future__ import annotations

from dataclasses import dataclass, field

from ..connectors.base import Connector
from ..models import Operation, SyncEvent


@dataclass
class ReconciliationReport:
    source: str
    target: str
    missing: list[str] = field(default_factory=list)
    drifted: list[str] = field(default_factory=list)
    orphaned: list[str] = field(default_factory=list)
    in_sync: int = 0

    @property
    def total_diff(self) -> int:
        return len(self.missing) + len(self.drifted) + len(self.orphaned)

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "target": self.target,
            "missing": len(self.missing),
            "drifted": len(self.drifted),
            "orphaned": len(self.orphaned),
            "in_sync": self.in_sync,
            "total_diff": self.total_diff,
        }


class Reconciler:
    def __init__(self, connectors: dict[str, Connector]) -> None:
        self._connectors = connectors

    def reconcile(
        self, source_name: str, target_name: str, *, delete_orphans: bool = False
    ) -> tuple[ReconciliationReport, list[SyncEvent]]:
        source = self._connectors[source_name]
        target = self._connectors[target_name]

        source_records = {r.id: r for r in source.list_records()}
        target_records = {r.id: r for r in target.list_records()}

        report = ReconciliationReport(source=source_name, target=target_name)
        events: list[SyncEvent] = []

        for rid, src in source_records.items():
            tgt = target_records.get(rid)
            if tgt is None:
                report.missing.append(rid)
                events.append(self._upsert_event(source_name, target_name, rid))
            elif src.checksum != tgt.checksum:
                report.drifted.append(rid)
                events.append(self._upsert_event(source_name, target_name, rid))
            else:
                report.in_sync += 1

        for rid in target_records:
            if rid not in source_records:
                report.orphaned.append(rid)
                if delete_orphans:
                    events.append(
                        SyncEvent(
                            record_id=rid,
                            source=source_name,
                            target=target_name,
                            operation=Operation.DELETE,
                        )
                    )

        return report, events

    @staticmethod
    def _upsert_event(source: str, target: str, rid: str) -> SyncEvent:
        return SyncEvent(
            record_id=rid,
            source=source,
            target=target,
            operation=Operation.UPSERT,
        )
