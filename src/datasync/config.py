"""Runtime configuration loaded from environment variables.

Keeping configuration in one typed place makes the Azure Functions host and the
local runner behave identically, which is critical for reproducing production
issues on a laptop.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _get_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _get_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 5
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 300.0
    jitter: bool = True


@dataclass(frozen=True)
class OpenSearchConfig:
    enabled: bool = False
    hosts: tuple[str, ...] = ("https://localhost:9200",)
    username: str | None = None
    password: str | None = None
    verify_certs: bool = False
    index_prefix: str = "datasync"


@dataclass(frozen=True)
class AppConfig:
    queue_name: str = "sync-events"
    dlq_name: str = "sync-events-poison"
    storage_connection: str = "UseDevelopmentStorage=true"
    retry: RetryConfig = field(default_factory=RetryConfig)
    opensearch: OpenSearchConfig = field(default_factory=OpenSearchConfig)

    @classmethod
    def from_env(cls) -> "AppConfig":
        hosts_raw = os.environ.get("OPENSEARCH_HOSTS", "https://localhost:9200")
        hosts = tuple(h.strip() for h in hosts_raw.split(",") if h.strip())

        return cls(
            queue_name=os.environ.get("SYNC_QUEUE_NAME", "sync-events"),
            dlq_name=os.environ.get("SYNC_DLQ_NAME", "sync-events-poison"),
            storage_connection=os.environ.get(
                "AzureWebJobsStorage", "UseDevelopmentStorage=true"
            ),
            retry=RetryConfig(
                max_attempts=_get_int("SYNC_MAX_ATTEMPTS", 5),
                backoff_base_seconds=float(_get_int("SYNC_BACKOFF_BASE_SECONDS", 2)),
                backoff_max_seconds=float(_get_int("SYNC_BACKOFF_MAX_SECONDS", 300)),
            ),
            opensearch=OpenSearchConfig(
                enabled=_get_bool("OPENSEARCH_ENABLED", False),
                hosts=hosts,
                username=os.environ.get("OPENSEARCH_USERNAME"),
                password=os.environ.get("OPENSEARCH_PASSWORD"),
                verify_certs=_get_bool("OPENSEARCH_VERIFY_CERTS", False),
                index_prefix=os.environ.get("OPENSEARCH_INDEX_PREFIX", "datasync"),
            ),
        )
