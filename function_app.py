"""Azure Functions entry point (Python v2 programming model).

Triggers
--------
ingest        HTTP  POST /api/ingest      -> validate + enqueue a SyncEvent
process       Queue sync-events           -> run the sync engine (retry/DLQ)
dead_letter   Queue sync-events-poison    -> record poison messages
reconcile     Timer every 15 minutes      -> drift detection source<->target
metrics       HTTP  GET  /api/metrics     -> counters + observability buffer

All triggers delegate to the same SyncPipeline used by the local demo/tests.
"""
from __future__ import annotations

import json
import logging
import os
import sys

import azure.functions as func

# Make the `src` layout importable inside the Functions host.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from datasync.config import AppConfig  # noqa: E402
from datasync.factory import build_azure_pipeline, _default_demo_connectors  # noqa: E402
from datasync.models import Operation, SyncEvent  # noqa: E402

logger = logging.getLogger("datasync")

app = func.FunctionApp()

_CONFIG = AppConfig.from_env()
# NOTE: replace _default_demo_connectors() with real SaaS connectors
# (Salesforce/HubSpot/etc.) for production. They only need to implement the
# Connector interface in datasync.connectors.base.
_CONNECTORS = _default_demo_connectors()


def _pipeline():
    return build_azure_pipeline(_CONFIG, _CONNECTORS)


@app.function_name(name="ingest")
@app.route(route="ingest", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def ingest(req: func.HttpRequest) -> func.HttpResponse:
    """Receive a webhook from a SaaS system and enqueue a sync event."""
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("invalid JSON", status_code=400)

    record_id = body.get("record_id")
    source = body.get("source")
    target = body.get("target")
    if not record_id or not source or not target:
        return func.HttpResponse(
            "record_id, source and target are required", status_code=400
        )

    event = SyncEvent(
        record_id=record_id,
        source=source,
        target=target,
        operation=Operation(body.get("operation", "upsert")),
        payload=body.get("payload"),
    )
    _pipeline().enqueue(event)
    return func.HttpResponse(
        json.dumps({"accepted": True, "event_id": event.event_id}),
        status_code=202,
        mimetype="application/json",
    )


@app.function_name(name="process")
@app.queue_trigger(
    arg_name="msg",
    queue_name="%SYNC_QUEUE_NAME%",
    connection="AzureWebJobsStorage",
)
def process(msg: func.QueueMessage) -> None:
    """Process one queued sync event. Re-enqueues retries / routes to DLQ."""
    event = SyncEvent.from_json(msg.get_body().decode("utf-8"))
    result = _pipeline().handle_event(
        event, dequeue_count=msg.dequeue_count or event.attempt
    )
    logger.info(
        "processed record=%s status=%s attempt=%s",
        event.record_id,
        result.status.value,
        event.attempt,
    )


@app.function_name(name="dead_letter")
@app.queue_trigger(
    arg_name="msg",
    queue_name="%SYNC_DLQ_NAME%",
    connection="AzureWebJobsStorage",
)
def dead_letter(msg: func.QueueMessage) -> None:
    """Persist poison messages for inspection / replay in OpenSearch."""
    body = msg.get_body().decode("utf-8")
    pipeline = _pipeline()
    pipeline.sink.emit(
        "dead_letter_received",
        {"event": "dead_letter_received", "body": body, "dequeue_count": msg.dequeue_count},
    )
    pipeline.metrics.incr("dlq.received")
    logger.warning("dead-letter received: %s", body)


@app.function_name(name="reconcile")
@app.timer_trigger(
    arg_name="timer",
    schedule="0 */15 * * * *",
    run_on_startup=False,
)
def reconcile(timer: func.TimerRequest) -> None:
    """Scheduled drift detection that enqueues corrective sync events."""
    report = _pipeline().run_reconciliation("crm", "billing")
    logger.info("reconciliation report: %s", report)


@app.function_name(name="metrics")
@app.route(route="metrics", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def metrics(req: func.HttpRequest) -> func.HttpResponse:
    pipeline = _pipeline()
    payload = {
        "metrics": pipeline.metrics.snapshot(),
        "queue_depth": pipeline.queue.approximate_count(),
        "dlq_depth": pipeline.dlq.approximate_count(),
    }
    return func.HttpResponse(
        json.dumps(payload), status_code=200, mimetype="application/json"
    )
