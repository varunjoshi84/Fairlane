"""Tests for fairlane.retry — backoff calculation and retryable-error check."""

import pytest

from fairlane.retry import calculate_backoff, is_retryable

# ---------------------------------------------------------------------------
# calculate_backoff – deterministic (no jitter)
# ---------------------------------------------------------------------------


class TestCalculateBackoffNoJitter:
    """Without jitter the delay is exactly base_delay * 2^(attempt-1)."""

    def test_attempt_1(self):
        assert calculate_backoff(1, base_delay=1.0, jitter=False) == 1.0

    def test_attempt_2(self):
        assert calculate_backoff(2, base_delay=1.0, jitter=False) == 2.0

    def test_attempt_3(self):
        assert calculate_backoff(3, base_delay=1.0, jitter=False) == 4.0

    def test_attempt_4(self):
        assert calculate_backoff(4, base_delay=1.0, jitter=False) == 8.0

    def test_delays_grow_exponentially(self):
        delays = [
            calculate_backoff(a, base_delay=1.0, jitter=False) for a in range(1, 8)
        ]
        for i in range(1, len(delays)):
            assert delays[i] > delays[i - 1], "delays must strictly increase"

    def test_never_exceeds_max_delay(self):
        for attempt in range(1, 20):
            delay = calculate_backoff(attempt, base_delay=1.0, max_delay=30.0, jitter=False)
            assert delay <= 30.0

    def test_custom_base_delay(self):
        assert calculate_backoff(1, base_delay=0.5, jitter=False) == 0.5
        assert calculate_backoff(2, base_delay=0.5, jitter=False) == 1.0


# ---------------------------------------------------------------------------
# calculate_backoff – with jitter
# ---------------------------------------------------------------------------


class TestCalculateBackoffWithJitter:
    """Full-jitter delays are in [0, deterministic_delay)."""

    @pytest.mark.parametrize("attempt", [1, 2, 3, 5, 10])
    def test_jitter_within_bounds(self, attempt):
        upper = calculate_backoff(attempt, base_delay=1.0, max_delay=60.0, jitter=False)
        for _ in range(200):
            val = calculate_backoff(attempt, base_delay=1.0, max_delay=60.0, jitter=True)
            assert 0 <= val <= upper

    def test_jitter_never_exceeds_max_delay(self):
        for attempt in range(1, 20):
            for _ in range(50):
                val = calculate_backoff(attempt, base_delay=1.0, max_delay=10.0, jitter=True)
                assert val <= 10.0

    def test_jitter_produces_variation(self):
        """Over many samples we expect at least some variance (not all identical)."""
        values = {
            calculate_backoff(3, base_delay=1.0, jitter=True) for _ in range(50)
        }
        assert len(values) > 1, "jitter should produce distinct values"


# ---------------------------------------------------------------------------
# is_retryable
# ---------------------------------------------------------------------------


class TestIsRetryable:
    """Permanent errors → False, transient errors → True."""

    @pytest.mark.parametrize(
        "exc",
        [
            ValueError("bad input"),
            TypeError("wrong type"),
            KeyError("missing"),
            AttributeError("no attr"),
            NotImplementedError(),
            PermissionError("denied"),
        ],
    )
    def test_permanent_errors_are_not_retryable(self, exc):
        assert is_retryable(exc) is False

    @pytest.mark.parametrize(
        "exc",
        [
            TimeoutError("timed out"),
            ConnectionError("refused"),
            ConnectionResetError("reset"),
            OSError("network down"),
            RuntimeError("transient glitch"),
        ],
    )
    def test_transient_errors_are_retryable(self, exc):
        assert is_retryable(exc) is True
