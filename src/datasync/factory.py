"""Composition root.

Builds a fully-wired SyncPipeline from configuration. Swapping connectors,
queues or the observability sink happens here and nowhere else.
"""
from __future__ import annotations

from .config import AppConfig
from .connectors.base import Connector
from .connectors.mock import MockConnector
from .models import Record
from .observability.metrics import Metrics
from .observability.opensearch_sink import build_sink
from .pipeline import SyncPipeline
from .queues.base import Queue
from .queues.memory import InMemoryQueue


def build_local_pipeline(
    config: AppConfig | None = None,
    *,
    connectors: dict[str, Connector] | None = None,
) -> SyncPipeline:
    """In-memory pipeline for the demo and tests."""
    config = config or AppConfig()
    connectors = connectors or _default_demo_connectors()
    return SyncPipeline(
        config=config,
        connectors=connectors,
        queue=InMemoryQueue(config.queue_name),
        dlq=InMemoryQueue(config.dlq_name),
        sink=build_sink(config.opensearch),
        metrics=Metrics(),
    )


def build_azure_pipeline(config: AppConfig, connectors: dict[str, Connector]):
    """Pipeline backed by Azure Storage Queues for the Functions host."""
    from .queues.azure import AzureStorageQueue

    queue: Queue = AzureStorageQueue(config.storage_connection, config.queue_name)
    dlq: Queue = AzureStorageQueue(config.storage_connection, config.dlq_name)
    return SyncPipeline(
        config=config,
        connectors=connectors,
        queue=queue,
        dlq=dlq,
        sink=build_sink(config.opensearch),
        metrics=Metrics(),
    )


def _default_demo_connectors() -> dict[str, Connector]:
    """Two SaaS systems: 'crm' (source) seeded with data, 'billing' (target)."""
    crm = MockConnector(name="crm")
    for i in range(1, 6):
        crm.seed(Record(id=f"cust-{i}", payload={"name": f"Account {i}", "tier": "free"}))

    # 'billing' is the target. Inject a flaky record and a permanently bad one
    # so the demo exercises retry and dead-letter paths.
    billing = MockConnector(
        name="billing",
        flaky_ids={"cust-2"},
        flaky_times=2,
        fatal_ids={"cust-4"},
    )
    return {"crm": crm, "billing": billing}
