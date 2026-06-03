"""Azure Storage Queue implementation.

Imported lazily by the Functions host. Local tests never need azure-storage-queue
to be installed because they use the in-memory queue instead.
"""
from __future__ import annotations

import base64

from .base import Queue, QueueMessage


class AzureStorageQueue(Queue):
    def __init__(self, connection_string: str, queue_name: str) -> None:
        from azure.storage.queue import QueueClient  # local import on purpose

        self.queue_name = queue_name
        self._client = QueueClient.from_connection_string(
            connection_string, queue_name
        )
        try:
            self._client.create_queue()
        except Exception:
            # Queue already exists - safe to ignore.
            pass

    @staticmethod
    def _encode(body: str) -> str:
        # The Functions Python worker base64-encodes queue payloads by default.
        return base64.b64encode(body.encode("utf-8")).decode("ascii")

    def send(self, body: str, *, delay_seconds: float = 0.0) -> None:
        self._client.send_message(
            self._encode(body),
            visibility_timeout=int(delay_seconds) if delay_seconds > 0 else None,
        )

    def receive(self, max_messages: int = 1) -> list[QueueMessage]:
        messages = self._client.receive_messages(max_messages=max_messages)
        out: list[QueueMessage] = []
        for m in messages:
            try:
                body = base64.b64decode(m.content).decode("utf-8")
            except Exception:
                body = m.content
            out.append(
                QueueMessage(
                    body=body,
                    id=m.id,
                    dequeue_count=m.dequeue_count or 1,
                )
            )
            self._receipts[m.id] = m  # type: ignore[attr-defined]
        return out

    _receipts: dict = {}

    def complete(self, message: QueueMessage) -> None:
        m = self._receipts.pop(message.id, None)
        if m is not None:
            self._client.delete_message(m)

    def approximate_count(self) -> int:
        props = self._client.get_queue_properties()
        return props.approximate_message_count or 0
