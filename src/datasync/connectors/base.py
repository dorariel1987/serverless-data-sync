"""SaaS connector abstraction.

A connector is the only place that knows how to talk to a specific SaaS API.
The sync engine depends solely on this interface, so adding Salesforce, HubSpot,
Zendesk, etc. is just a new subclass - no engine changes required.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

from ..models import Record


class Connector(ABC):
    name: str = "connector"

    @abstractmethod
    def get_record(self, record_id: str) -> Record | None:
        """Return a single record or None if it does not exist."""

    @abstractmethod
    def list_records(self) -> Iterable[Record]:
        """Return all records. Used by the reconciliation job."""

    @abstractmethod
    def upsert_record(self, record: Record) -> Record:
        """Create or update a record in the target system."""

    @abstractmethod
    def delete_record(self, record_id: str) -> None:
        """Delete a record. Should be idempotent (no error if missing)."""
