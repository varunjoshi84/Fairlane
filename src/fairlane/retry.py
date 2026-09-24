"""Retry utilities: exponential backoff with jitter and retryable-error classification."""

import random

from fairlane.models import FailureCategory


def calculate_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
) -> float:
    """Return a back-off delay for the given *attempt* number (1-based).

    Formula: ``delay = min(max_delay, base_delay * 2 ** (attempt - 1))``

    When *jitter* is ``True`` the returned value is drawn uniformly from
    ``[0, delay)`` ("full jitter"), which helps decorrelate competing
    retriers.
    """
    delay = min(max_delay, base_delay * 2 ** (attempt - 1))
    if jitter:
        delay = random.uniform(0, delay)
    return delay


# ---------------------------------------------------------------------------
# Exception categories
# ---------------------------------------------------------------------------

_PERMANENT_ERRORS: tuple[type[BaseException], ...] = (
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    NotImplementedError,
    PermissionError,
)

# If the same task crashes this many times it is treated as a poison pill.
POISON_PILL_THRESHOLD = 3


def is_retryable(exception: BaseException) -> bool:
    """Return ``True`` for transient errors that are worth retrying.

    Permanent programming / validation errors (``ValueError``,
    ``TypeError``, …) return ``False``; everything else — notably
    ``TimeoutError``, ``ConnectionError``, ``OSError`` — returns ``True``.
    """
    return not isinstance(exception, _PERMANENT_ERRORS)


def classify_failure(
    exception: BaseException,
    attempts: int,
    max_attempts: int,
    *,
    consecutive_crashes: int = 0,
) -> FailureCategory:
    """Classify a task failure into a :class:`FailureCategory`.

    Rules
    -----
    1. If *consecutive_crashes* >= ``POISON_PILL_THRESHOLD`` the task is
       treated as a **POISON_PILL** — it repeatedly crashes the worker
       loop and must be quarantined.
    2. ``ValueError``, ``KeyError``, and other non-retryable exceptions
       are classified as **PERMANENT**.
    3. Transient errors (``TimeoutError``, ``ConnectionError``, …) that
       still have retries left are **not** terminal — the caller should
       schedule a retry instead.  When retries are exhausted
       (*attempts >= max_attempts*) the category is **TRANSIENT_EXHAUSTED**.
    """
    # Poison pill detection takes highest priority.
    if consecutive_crashes >= POISON_PILL_THRESHOLD:
        return FailureCategory.POISON_PILL

    if not is_retryable(exception):
        return FailureCategory.PERMANENT

    # Transient error with retries exhausted.
    if attempts >= max_attempts:
        return FailureCategory.TRANSIENT_EXHAUSTED

    # Transient error with retries remaining — not a terminal failure.
    # Return TRANSIENT_EXHAUSTED as a conservative fallback; in practice
    # the caller should schedule a retry rather than dead-lettering.
    return FailureCategory.TRANSIENT_EXHAUSTED

