import pytest

from datasync.connectors.mock import MockConnector
from datasync.engine.sync import SyncEngine
from datasync.models import Operation, Record, SyncEvent, SyncStatus


def build_engine(**target_kwargs):
    source = MockConnector(name="crm")
    source.seed(Record(id="1", payload={"name": "Acme"}))
    target = MockConnector(name="billing", **target_kwargs)
    return SyncEngine({"crm": source, "billing": target}), source, target


def test_upsert_success():
    engine, _, target = build_engine()
    result = engine.process(SyncEvent(record_id="1", source="crm", target="billing"))
    assert result.status is SyncStatus.SUCCESS
    assert target.get_record("1") is not None
    assert result.target_checksum is not None


def test_upsert_missing_source_is_skipped():
    engine, _, _ = build_engine()
    result = engine.process(SyncEvent(record_id="ghost", source="crm", target="billing"))
    assert result.status is SyncStatus.SKIPPED


def test_retriable_error_returns_retry():
    engine, _, _ = build_engine(fail_upsert_ids={"1"})
    result = engine.process(SyncEvent(record_id="1", source="crm", target="billing"))
    assert result.status is SyncStatus.RETRY
    assert "retriable" in result.error


def test_fatal_error_returns_dead_letter():
    engine, _, _ = build_engine(fatal_ids={"1"})
    result = engine.process(SyncEvent(record_id="1", source="crm", target="billing"))
    assert result.status is SyncStatus.DEAD_LETTER
    assert "fatal" in result.error


def test_delete_operation_is_idempotent():
    engine, _, target = build_engine()
    target.seed(Record(id="1", payload={"name": "Acme"}))
    ev = SyncEvent(record_id="1", source="crm", target="billing", operation=Operation.DELETE)
    assert engine.process(ev).status is SyncStatus.SUCCESS
    # second delete still succeeds (record already gone)
    assert engine.process(ev).status is SyncStatus.SUCCESS


def test_inline_payload_used_without_source_fetch():
    engine, source, target = build_engine()
    ev = SyncEvent(
        record_id="2", source="crm", target="billing", payload={"name": "FromWebhook"}
    )
    result = engine.process(ev)
    assert result.status is SyncStatus.SUCCESS
    assert target.get_record("2").payload["name"] == "FromWebhook"
    # source.get_record should not have been called for record 2
    assert ("get", "2") not in source.calls


def test_unknown_system_is_dead_letter():
    engine, _, _ = build_engine()
    result = engine.process(SyncEvent(record_id="1", source="crm", target="nope"))
    assert result.status is SyncStatus.DEAD_LETTER
