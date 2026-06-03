from datasync.connectors.mock import MockConnector
from datasync.engine.reconcile import Reconciler
from datasync.models import Operation, Record


def make_connectors():
    crm = MockConnector(name="crm")
    billing = MockConnector(name="billing")
    return crm, billing


def test_detects_missing_record():
    crm, billing = make_connectors()
    crm.seed(Record(id="1", payload={"v": 1}))
    report, events = Reconciler({"crm": crm, "billing": billing}).reconcile("crm", "billing")
    assert report.missing == ["1"]
    assert len(events) == 1 and events[0].operation is Operation.UPSERT


def test_detects_drift():
    crm, billing = make_connectors()
    crm.seed(Record(id="1", payload={"v": 1}))
    billing.seed(Record(id="1", payload={"v": 999}))
    report, events = Reconciler({"crm": crm, "billing": billing}).reconcile("crm", "billing")
    assert report.drifted == ["1"]
    assert report.missing == []
    assert len(events) == 1


def test_in_sync_produces_no_events():
    crm, billing = make_connectors()
    crm.seed(Record(id="1", payload={"v": 1}))
    billing.seed(Record(id="1", payload={"v": 1}))
    report, events = Reconciler({"crm": crm, "billing": billing}).reconcile("crm", "billing")
    assert report.in_sync == 1
    assert events == []


def test_orphan_delete_only_when_enabled():
    crm, billing = make_connectors()
    billing.seed(Record(id="orphan", payload={"v": 1}))

    r = Reconciler({"crm": crm, "billing": billing})
    report, events = r.reconcile("crm", "billing", delete_orphans=False)
    assert report.orphaned == ["orphan"]
    assert events == []

    report2, events2 = r.reconcile("crm", "billing", delete_orphans=True)
    assert len(events2) == 1 and events2[0].operation is Operation.DELETE
