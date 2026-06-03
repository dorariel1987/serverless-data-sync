"""Observability sink.

Every sync attempt, retry and dead-letter is indexed as a structured document.
This gives full audit/searchability in OpenSearch Dashboards (per-correlation-id
traces, failure rates, lag, etc.).

The OpenSearch dependency is optional and the sink degrades gracefully: if the
client is missing or the cluster is unreachable, events are logged locally and
buffered in-memory instead of crashing the pipeline.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from ..config import OpenSearchConfig

logger = logging.getLogger("datasync.observability")


class ObservabilitySink(ABC):
    @abstractmethod
    def emit(self, kind: str, document: dict[str, Any]) -> None:
        ...


class NullSink(ObservabilitySink):
    """Keeps the last N documents in memory; used for tests and offline runs."""

    def __init__(self, capacity: int = 1000) -> None:
        self.capacity = capacity
        self.documents: list[dict[str, Any]] = []

    def emit(self, kind: str, document: dict[str, Any]) -> None:
        doc = {"kind": kind, **document}
        self.documents.append(doc)
        if len(self.documents) > self.capacity:
            self.documents.pop(0)
        logger.info("event %s %s", kind, document.get("status", document.get("event")))

    def of_kind(self, kind: str) -> list[dict[str, Any]]:
        return [d for d in self.documents if d.get("kind") == kind]


class OpenSearchSink(ObservabilitySink):
    def __init__(self, config: OpenSearchConfig) -> None:
        self.config = config
        self._fallback = NullSink()
        self._client = None
        self._available = False
        self._connect()

    def _connect(self) -> None:
        try:
            from opensearchpy import OpenSearch  # local import
        except Exception as exc:  # pragma: no cover - depends on env
            logger.warning("opensearch-py not installed (%s); using fallback", exc)
            return

        http_auth = None
        if self.config.username and self.config.password:
            http_auth = (self.config.username, self.config.password)

        try:
            self._client = OpenSearch(
                hosts=list(self.config.hosts),
                http_auth=http_auth,
                use_ssl=any(h.startswith("https") for h in self.config.hosts),
                verify_certs=self.config.verify_certs,
                ssl_show_warn=False,
                timeout=5,
            )
            # Cheap connectivity probe.
            self._client.info()
            self._available = True
            logger.info("connected to OpenSearch at %s", self.config.hosts)
        except Exception as exc:  # pragma: no cover - depends on env
            logger.warning("OpenSearch unavailable (%s); using fallback", exc)
            self._client = None
            self._available = False

    def _index_name(self, kind: str) -> str:
        day = datetime.now(timezone.utc).strftime("%Y.%m.%d")
        return f"{self.config.index_prefix}-{kind}-{day}"

    def emit(self, kind: str, document: dict[str, Any]) -> None:
        # Always keep a local copy so observability survives an OpenSearch outage.
        self._fallback.emit(kind, document)
        if not self._available or self._client is None:
            return
        try:
            self._client.index(index=self._index_name(kind), body=document)
        except Exception as exc:  # pragma: no cover - depends on env
            logger.warning("failed to index %s event: %s", kind, exc)
            self._available = False

    @property
    def buffered(self) -> list[dict[str, Any]]:
        return self._fallback.documents


def build_sink(config: OpenSearchConfig) -> ObservabilitySink:
    if config.enabled:
        return OpenSearchSink(config)
    return NullSink()
