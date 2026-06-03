from datasync.models import Operation, Record, SyncEvent, checksum


def test_checksum_is_order_independent():
    assert checksum({"a": 1, "b": 2}) == checksum({"b": 2, "a": 1})


def test_record_checksum_changes_with_payload():
    r1 = Record(id="1", payload={"x": 1})
    r2 = Record(id="1", payload={"x": 2})
    assert r1.checksum != r2.checksum


def test_sync_event_json_round_trip():
    ev = SyncEvent(record_id="r1", source="a", target="b", operation=Operation.DELETE)
    restored = SyncEvent.from_json(ev.to_json())
    assert restored.record_id == "r1"
    assert restored.operation is Operation.DELETE
    assert restored.event_id == ev.event_id
    assert restored.correlation_id == ev.correlation_id


def test_next_attempt_increments_and_keeps_identity():
    ev = SyncEvent(record_id="r1", source="a", target="b")
    nxt = ev.next_attempt()
    assert nxt.attempt == ev.attempt + 1
    assert nxt.event_id == ev.event_id
    assert nxt.correlation_id == ev.correlation_id
