from datasync.config import RetryConfig
from datasync.engine.retry import RetryPolicy


def test_should_retry_until_max():
    policy = RetryPolicy(RetryConfig(max_attempts=3))
    assert policy.should_retry(1)
    assert policy.should_retry(2)
    assert not policy.should_retry(3)
    assert not policy.should_retry(4)


def test_backoff_is_exponential_without_jitter():
    policy = RetryPolicy(
        RetryConfig(backoff_base_seconds=2, backoff_max_seconds=1000, jitter=False)
    )
    assert policy.backoff_seconds(1) == 2
    assert policy.backoff_seconds(2) == 4
    assert policy.backoff_seconds(3) == 8


def test_backoff_is_capped():
    policy = RetryPolicy(
        RetryConfig(backoff_base_seconds=2, backoff_max_seconds=5, jitter=False)
    )
    assert policy.backoff_seconds(10) == 5


def test_jitter_stays_within_cap():
    policy = RetryPolicy(
        RetryConfig(backoff_base_seconds=2, backoff_max_seconds=50, jitter=True)
    )
    for attempt in range(1, 8):
        delay = policy.backoff_seconds(attempt)
        assert 0 <= delay <= 50
