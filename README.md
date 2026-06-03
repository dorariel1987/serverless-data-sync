# Serverless Data Sync

Event-driven sync layer on **Azure Functions + Storage Queues** that moves and
reconciles records between SaaS systems, with **retries, dead-letter handling and
OpenSearch observability**.

The core engine is decoupled from Azure, so the whole pipeline runs and is fully
tested on a laptop with no cloud dependencies.

## Architecture

```
                 ┌──────────────┐   webhook    ┌───────────────────────┐
   SaaS source ─▶│ ingest (HTTP)│ ───────────▶ │  queue: sync-events   │
                 └──────────────┘              └──────────┬────────────┘
                                                          │ queue trigger
                                                          ▼
                                              ┌───────────────────────┐
                                              │   process (SyncEngine) │
                                              │  source.get → transform│
                                              │  → target.upsert       │
                                              └───┬───────────┬────────┘
                          retriable (backoff)     │           │ fatal / exhausted
                          re-enqueue ◀─────────────┘           ▼
                                                   ┌───────────────────────┐
                                                   │ queue: *-poison (DLQ) │
                                                   │ dead_letter trigger   │
                                                   └───────────────────────┘

   reconcile (timer, every 15m): source ⟷ target drift → corrective events
   observability: every enqueue/sync/retry/dead_letter/reconcile → OpenSearch
```

### Components (`src/datasync/`)

| Module | Responsibility |
| --- | --- |
| `models.py` | `Record`, `SyncEvent`, `SyncResult`, checksums, error types |
| `connectors/` | `Connector` interface + in-memory `MockConnector` (failure injection) |
| `queues/` | `Queue` interface, `InMemoryQueue`, `AzureStorageQueue` |
| `engine/sync.py` | Pure sync logic: fetch → transform → upsert/delete |
| `engine/retry.py` | Capped exponential backoff with full jitter |
| `engine/reconcile.py` | Drift detection (missing / drifted / orphaned) |
| `observability/` | Metrics + OpenSearch sink with graceful fallback |
| `pipeline.py` | Orchestration: retry routing, DLQ, observability |
| `factory.py` | Composition root (local vs Azure wiring) |
| `function_app.py` | Azure Functions v2 triggers (HTTP/Queue/Timer) |

## Quick start (local, no Azure)

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows  (source .venv/bin/activate on *nix)
pip install -r requirements-dev.txt

pytest                            # 29 tests
python scripts/demo.py            # end-to-end: retry + DLQ + reconciliation
```

The demo seeds a `crm` source and a `billing` target, where `cust-2` is flaky
(succeeds after 2 retries) and `cust-4` is permanently invalid (dead-lettered),
then introduces drift and repairs it via reconciliation.

## Run on Azure Functions

1. Install [Azure Functions Core Tools v4] and start the emulators:

   ```bash
   docker compose up -d            # Azurite + OpenSearch + Dashboards
   ```

2. Configure settings:

   ```bash
   copy local.settings.json.example local.settings.json
   ```

3. Run the host:

   ```bash
   func start
   ```

4. Ingest an event:

   ```bash
   curl -X POST http://localhost:7071/api/ingest \
        -H "Content-Type: application/json" \
        -d '{"record_id":"cust-1","source":"crm","target":"billing","operation":"upsert"}'
   ```

5. Inspect counters: `GET http://localhost:7071/api/metrics`.
   Observability docs land in OpenSearch under the `datasync-*` indices
   (view in Dashboards at http://localhost:5601).

## Reliability model

- **Retries** — transient failures (`RetriableError`) are re-enqueued with
  exponential backoff + jitter, up to `SYNC_MAX_ATTEMPTS`.
- **Dead-letter** — non-retriable failures (`FatalError`) and exhausted retries
  are routed to the `*-poison` queue and recorded for replay.
- **Idempotency** — upserts/deletes are idempotent; a missing source record is a
  safe skip, so at-least-once queue delivery never corrupts the target.
- **Reconciliation** — a scheduled job catches anything lost between systems by
  comparing checksums and emitting corrective events.

## Extending to real SaaS systems

Implement the `Connector` interface for each system (Salesforce, HubSpot,
Zendesk, …) and register it in `factory.py` / `function_app.py`. No engine,
queue or observability changes are required.

## Configuration

All settings come from environment variables (see `local.settings.json.example`):
`SYNC_QUEUE_NAME`, `SYNC_DLQ_NAME`, `SYNC_MAX_ATTEMPTS`,
`SYNC_BACKOFF_BASE_SECONDS`, `SYNC_BACKOFF_MAX_SECONDS`, `OPENSEARCH_ENABLED`,
`OPENSEARCH_HOSTS`, `OPENSEARCH_USERNAME`, `OPENSEARCH_PASSWORD`,
`OPENSEARCH_INDEX_PREFIX`.

[Azure Functions Core Tools v4]: https://learn.microsoft.com/azure/azure-functions/functions-run-local
