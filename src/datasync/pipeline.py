"""Orchestration pipeline.

Glues the queue, sync engine, retry policy, dead-letter handling and
observability together. The Azure Functions triggers and the local demo both
drive this same object, so behavior is identical in every environment.
"""
from __future__ import annotations

import logging

from .config import AppConfig
from .connectors.base import Connector
from .engine.reconcile import Reconciler
from .engine.retry import RetryPolicy
from .engine.sync import SyncEngine, Transform, identity_transform
from .models import SyncEvent, SyncResult, SyncStatus
from .observability.metrics import Metrics
from .observability.opensearch_sink import ObservabilitySink, NullSink
from .queues.base import Queue, QueueMessage

logger = logging.getLogger("datasync.pipeline")


class SyncPipeline:
    def __init__(
        self,
        *,
        config: AppConfig,
        connectors: dict[str, Connector],
        queue: Queue,
        dlq: Queue,
        sink: ObservabilitySink | None = None,
        metrics: Metrics | None = None,
        transform: Transform = identity_transform,
    ) -> None:
        self.config = config
        self.connectors = connectors
        self.queue = queue
        self.dlq = dlq
        self.sink = sink or NullSink()
        self.metrics = metrics or Metrics()
        self.engine = SyncEngine(connectors, transform=transform)
        self.reconciler = Reconciler(connectors)
        self.retry = RetryPolicy(config.retry)

    # ------------------------------------------------------------------ enqueue
    def enqueue(self, event: SyncEvent) -> None:
        self.queue.send(event.to_json())
        self.metrics.incr("enqueued")
        self.sink.emit("enqueue", {"event": "enqueue", **_event_summary(event)})

    # ------------------------------------------------------------------ process
    def handle_event(self, event: SyncEvent, *, dequeue_count: int = 1) -> SyncResult:
        """Process a single event and apply retry / dead-letter routing."""
        result = self.engine.process(event)
        self.metrics.incr(f"status.{result.status.value}")

        if result.status in (SyncStatus.SUCCESS, SyncStatus.SKIPPED):
            self.sink.emit("sync", result.to_dict())
            return result

        if result.status is SyncStatus.RETRY:
            return self._route_retry(event, result, dequeue_count)

        # DEAD_LETTER (fatal error)
        self._dead_letter(event, result)
        return result

    def _route_retry(
        self, event: SyncEvent, result: SyncResult, dequeue_count: int
    ) -> SyncResult:
        attempt = max(event.attempt, dequeue_count)
        if self.retry.should_retry(attempt):
            delay = self.retry.backoff_seconds(attempt)
            self.queue.send(event.next_attempt().to_json(), delay_seconds=delay)
            self.metrics.incr("retried")
            doc = result.to_dict()
            doc["backoff_seconds"] = round(delay, 3)
            self.sink.emit("retry", doc)
            logger.info(
                "retry record=%s attempt=%s delay=%.2fs",
                event.record_id,
                attempt,
                delay,
            )
            return result

        # Exhausted retries -> dead-letter.
        exhausted = SyncResult(
            event=event,
            status=SyncStatus.DEAD_LETTER,
            error=f"max attempts reached ({attempt}): {result.error}",
            duration_ms=result.duration_ms,
        )
        self._dead_letter(event, exhausted)
        return exhausted

    def _dead_letter(self, event: SyncEvent, result: SyncResult) -> None:
        self.dlq.send(event.to_json())
        self.metrics.incr("dead_lettered")
        self.sink.emit("dead_letter", result.to_dict())
        logger.warning(
            "dead-letter record=%s reason=%s", event.record_id, result.error
        )

    # ------------------------------------------------------- local drive helpers
    def process_message(self, message: QueueMessage) -> SyncResult:
        event = SyncEvent.from_json(message.body)
        result = self.handle_event(event, dequeue_count=message.dequeue_count)
        self.queue.complete(message)
        return result

    def drain(self, max_iterations: int = 1000) -> list[SyncResult]:
        """Process the queue until empty (used by the demo and tests)."""
        results: list[SyncResult] = []
        for _ in range(max_iterations):
            batch = self.queue.receive(max_messages=16)
            if not batch:
                break
            for msg in batch:
                results.append(self.process_message(msg))
        return results

    # ---------------------------------------------------------------- reconcile
    def run_reconciliation(
        self, source: str, target: str, *, delete_orphans: bool = False
    ) -> dict:
        report, events = self.reconciler.reconcile(
            source, target, delete_orphans=delete_orphans
        )
        for ev in events:
            self.enqueue(ev)
        self.metrics.incr("reconcile.diffs", report.total_diff)
        self.sink.emit("reconcile", report.to_dict())
        logger.info("reconciliation %s->%s diffs=%s", source, target, report.total_diff)
        return report.to_dict()


def _event_summary(event: SyncEvent) -> dict:
    return {
        "event_id": event.event_id,
        "correlation_id": event.correlation_id,
        "record_id": event.record_id,
        "source": event.source,
        "target": event.target,
        "operation": event.operation.value,
        "attempt": event.attempt,
    }
