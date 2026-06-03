from datasync.config import AppConfig, RetryConfig
from datasync.connectors.mock import MockConnector
from datasync.factory import build_local_pipeline
from datasync.models import Record, SyncEvent, SyncStatus
from datasync.observability.opensearch_sink import NullSink
from datasync.pipeline import SyncPipeline
from datasync.queues.memory import InMemoryQueue


def fast_config(max_attempts=5):
    return AppConfig(
        retry=RetryConfig(
            max_attempts=max_attempts,
            backoff_base_seconds=0.0,
            backoff_max_seconds=0.0,
            jitter=False,
        )
    )


def build(connectors, max_attempts=5):
    config = fast_config(max_attempts)
    return SyncPipeline(
        config=config,
        connectors=connectors,
        queue=InMemoryQueue("q"),
        dlq=InMemoryQueue("dlq"),
        sink=NullSink(),
    )


def test_happy_path_drains_to_success():
    crm = MockConnector(name="crm")
    crm.seed(Record(id="1", payload={"v": 1}))
    billing = MockConnector(name="billing")
    pipe = build({"crm": crm, "billing": billing})

    pipe.enqueue(SyncEvent(record_id="1", source="crm", target="billing"))
    results = pipe.drain()

    assert results[-1].status is SyncStatus.SUCCESS
    assert billing.get_record("1") is not None
    assert pipe.metrics.get("status.success") == 1


def test_flaky_record_eventually_succeeds_via_retries():
    crm = MockConnector(name="crm")
    crm.seed(Record(id="1", payload={"v": 1}))
    billing = MockConnector(name="billing", flaky_ids={"1"}, flaky_times=2)
    pipe = build({"crm": crm, "billing": billing})

    pipe.enqueue(SyncEvent(record_id="1", source="crm", target="billing"))
    pipe.drain()

    assert billing.get_record("1") is not None
    assert pipe.metrics.get("retried") == 2
    assert pipe.metrics.get("status.success") == 1
    assert pipe.dlq.approximate_count() == 0


def test_fatal_record_goes_straight_to_dlq():
    crm = MockConnector(name="crm")
    crm.seed(Record(id="1", payload={"v": 1}))
    billing = MockConnector(name="billing", fatal_ids={"1"})
    pipe = build({"crm": crm, "billing": billing})

    pipe.enqueue(SyncEvent(record_id="1", source="crm", target="billing"))
    pipe.drain()

    assert pipe.dlq.approximate_count() == 1
    assert pipe.metrics.get("dead_lettered") == 1
    assert pipe.metrics.get("retried") == 0


def test_permanent_retriable_failure_exhausts_then_dlq():
    crm = MockConnector(name="crm")
    crm.seed(Record(id="1", payload={"v": 1}))
    billing = MockConnector(name="billing", fail_upsert_ids={"1"})
    pipe = build({"crm": crm, "billing": billing}, max_attempts=3)

    pipe.enqueue(SyncEvent(record_id="1", source="crm", target="billing"))
    pipe.drain()

    # attempts 1 and 2 retry, attempt 3 exhausts -> DLQ
    assert pipe.metrics.get("retried") == 2
    assert pipe.dlq.approximate_count() == 1


def test_reconciliation_repairs_drift_end_to_end():
    pipe = build_local_pipeline(fast_config())
    crm = pipe.connectors["crm"]
    billing = pipe.connectors["billing"]

    # First full sync (cust-4 is fatal in demo connectors -> lands in DLQ).
    for rec in crm.list_records():
        pipe.enqueue(SyncEvent(record_id=rec.id, source="crm", target="billing"))
    pipe.drain()

    # Tamper with a synced record to create drift.
    rec = billing.get_record("cust-1")
    rec.payload["tier"] = "TAMPERED"
    billing.seed(rec)

    report = pipe.run_reconciliation("crm", "billing")
    assert report["total_diff"] >= 1
    pipe.drain()

    assert billing.get_record("cust-1").payload["tier"] == "free"


def test_observability_records_events():
    crm = MockConnector(name="crm")
    crm.seed(Record(id="1", payload={"v": 1}))
    billing = MockConnector(name="billing")
    sink = NullSink()
    config = fast_config()
    pipe = SyncPipeline(
        config=config,
        connectors={"crm": crm, "billing": billing},
        queue=InMemoryQueue("q"),
        dlq=InMemoryQueue("dlq"),
        sink=sink,
    )
    pipe.enqueue(SyncEvent(record_id="1", source="crm", target="billing"))
    pipe.drain()

    assert len(sink.of_kind("enqueue")) == 1
    assert len(sink.of_kind("sync")) == 1
