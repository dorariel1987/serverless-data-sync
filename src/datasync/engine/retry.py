"""Retry policy: capped exponential backoff with optional full jitter."""
from __future__ import annotations

import random

from ..config import RetryConfig


class RetryPolicy:
    def __init__(self, config: RetryConfig) -> None:
        self.config = config

    def should_retry(self, attempt: int) -> bool:
        return attempt < self.config.max_attempts

    def backoff_seconds(self, attempt: int) -> float:
        """Delay before the next attempt (attempt is the one that just failed)."""
        exp = self.config.backoff_base_seconds * (2 ** (attempt - 1))
        capped = min(exp, self.config.backoff_max_seconds)
        if self.config.jitter:
            # Full jitter spreads retries to avoid thundering herds.
            return random.uniform(0, capped)
        return capped
