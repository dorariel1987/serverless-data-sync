"""End-to-end demo of the sync pipeline - no Azure required.

Run:  python scripts/demo.py

Shows: ingest -> queue -> sync engine, with a transient (retry) record, a fatal
(dead-letter) record, and a scheduled reconciliation that repairs drift.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datasync.config import AppConfig, RetryConfig  # noqa: E402
from datasync.factory import build_local_pipeline  # noqa: E402
from datasync.models import Operation, Record, SyncEvent  # noqa: E402


def banner(text: str) -> None:
    print("\n" + "=" * 64)
    print(text)
    print("=" * 64)


def main() -> None:
    # Small/zero backoff and no jitter so the demo finishes instantly.
    config = AppConfig(
        retry=RetryConfig(
            max_attempts=5, backoff_base_seconds=0.0, backoff_max_seconds=0.0, jitter=False
        )
    )
    pipeline = build_local_pipeline(config)
    crm = pipeline.connectors["crm"]
    billing = pipeline.connectors["billing"]

    banner("1) Ingest webhooks for every CRM customer -> sync to billing")
    for record in crm.list_records():
        pipeline.enqueue(
            SyncEvent(record_id=record.id, source="crm", target="billing",
                      operation=Operation.UPSERT)
        )
    print(f"queued {pipeline.queue.approximate_count()} events")

    banner("2) Drain the queue (cust-2 is flaky -> retries; cust-4 fatal -> DLQ)")
    results = pipeline.drain()
    for r in results:
        print(f"  {r.event.record_id:8} attempt={r.event.attempt} -> {r.status.value}"
              f"{' :: ' + r.error if r.error else ''}")

    banner("3) Metrics snapshot")
    print(json.dumps(pipeline.metrics.snapshot(), indent=2))
    print(f"dlq depth: {pipeline.dlq.approximate_count()}")

    banner("4) Introduce drift, then reconcile")
    # Mutate a synced record directly in billing to simulate out-of-band drift.
    drifted = billing.get_record("cust-1")
    drifted.payload["tier"] = "TAMPERED"
    billing.seed(drifted)
    # Add a brand new CRM record that never got a webhook.
    crm.seed(Record(id="cust-99", payload={"name": "Account 99", "tier": "free"}))

    report = pipeline.run_reconciliation("crm", "billing")
    print("reconciliation report:", json.dumps(report, indent=2))
    print("repairing drift...")
    pipeline.drain()

    # cust-1 should now match source again; cust-99 should now exist in billing.
    print("cust-1 billing tier:", billing.get_record("cust-1").payload.get("tier"))
    print("cust-99 in billing:", billing.get_record("cust-99") is not None)

    banner("Observability events captured (sample)")
    docs = getattr(pipeline.sink, "documents", [])
    for d in docs[:6]:
        print("  -", d.get("kind"), d.get("status", d.get("event", "")))
    print(f"  ... {len(docs)} total events buffered for OpenSearch")


if __name__ == "__main__":
    main()
