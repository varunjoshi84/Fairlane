"""Tests for fairlane.retry — backoff calculation and retryable-error check."""

import pytest

from fairlane.models import FailureCategory
from fairlane.retry import (
    POISON_PILL_THRESHOLD,
    calculate_backoff,
    classify_failure,
    is_retryable,
)

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


# ---------------------------------------------------------------------------
# classify_failure
# ---------------------------------------------------------------------------


class TestClassifyFailure:
    """classify_failure maps (exception, attempts, max_attempts) to FailureCategory."""

    # -- PERMANENT ----------------------------------------------------------

    @pytest.mark.parametrize(
        "exc",
        [
            ValueError("bad input"),
            KeyError("missing field"),
            TypeError("wrong type"),
            AttributeError("no attr"),
            NotImplementedError(),
            PermissionError("denied"),
        ],
    )
    def test_permanent_errors(self, exc):
        """Non-retryable exceptions → PERMANENT regardless of attempt count."""
        result = classify_failure(exc, attempts=1, max_attempts=5)
        assert result is FailureCategory.PERMANENT

    def test_permanent_even_if_retries_remain(self):
        """PERMANENT takes precedence over remaining retries."""
        result = classify_failure(ValueError("bad"), attempts=1, max_attempts=10)
        assert result is FailureCategory.PERMANENT

    def test_permanent_even_if_retries_exhausted(self):
        """PERMANENT takes precedence over exhausted retries."""
        result = classify_failure(ValueError("bad"), attempts=5, max_attempts=5)
        assert result is FailureCategory.PERMANENT

    # -- TRANSIENT_EXHAUSTED ------------------------------------------------

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
    def test_transient_exhausted(self, exc):
        """Retryable exceptions with attempts >= max_attempts → TRANSIENT_EXHAUSTED."""
        result = classify_failure(exc, attempts=5, max_attempts=5)
        assert result is FailureCategory.TRANSIENT_EXHAUSTED

    def test_transient_exhausted_over_max(self):
        """Attempts exceeding max still yields TRANSIENT_EXHAUSTED."""
        result = classify_failure(TimeoutError("t"), attempts=7, max_attempts=5)
        assert result is FailureCategory.TRANSIENT_EXHAUSTED

    # -- TRANSIENT (retries remaining) --------------------------------------

    def test_transient_with_retries_remaining(self):
        """Retryable exception with retries left → TRANSIENT_EXHAUSTED (conservative).

        In practice the caller schedules a retry rather than dead-lettering.
        """
        result = classify_failure(TimeoutError("t"), attempts=2, max_attempts=5)
        assert result is FailureCategory.TRANSIENT_EXHAUSTED

    # -- POISON_PILL --------------------------------------------------------

    def test_poison_pill_at_threshold(self):
        """consecutive_crashes == POISON_PILL_THRESHOLD → POISON_PILL."""
        result = classify_failure(
            RuntimeError("crash"),
            attempts=1,
            max_attempts=5,
            consecutive_crashes=POISON_PILL_THRESHOLD,
        )
        assert result is FailureCategory.POISON_PILL

    def test_poison_pill_above_threshold(self):
        """consecutive_crashes > POISON_PILL_THRESHOLD → POISON_PILL."""
        result = classify_failure(
            RuntimeError("crash"),
            attempts=1,
            max_attempts=5,
            consecutive_crashes=POISON_PILL_THRESHOLD + 5,
        )
        assert result is FailureCategory.POISON_PILL

    def test_poison_pill_overrides_permanent(self):
        """POISON_PILL takes priority over PERMANENT classification."""
        result = classify_failure(
            ValueError("bad"),
            attempts=1,
            max_attempts=5,
            consecutive_crashes=POISON_PILL_THRESHOLD,
        )
        assert result is FailureCategory.POISON_PILL

    def test_poison_pill_overrides_transient_exhausted(self):
        """POISON_PILL takes priority over TRANSIENT_EXHAUSTED."""
        result = classify_failure(
            TimeoutError("t"),
            attempts=5,
            max_attempts=5,
            consecutive_crashes=POISON_PILL_THRESHOLD,
        )
        assert result is FailureCategory.POISON_PILL

    def test_below_poison_pill_threshold_not_poison(self):
        """consecutive_crashes below threshold does not trigger POISON_PILL."""
        result = classify_failure(
            RuntimeError("crash"),
            attempts=5,
            max_attempts=5,
            consecutive_crashes=POISON_PILL_THRESHOLD - 1,
        )
        assert result is not FailureCategory.POISON_PILL

