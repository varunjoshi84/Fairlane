"""Retry utilities: exponential backoff with jitter and retryable-error classification."""

import random


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


def is_retryable(exception: BaseException) -> bool:
    """Return ``True`` for transient errors that are worth retrying.

    Permanent programming / validation errors (``ValueError``,
    ``TypeError``, …) return ``False``; everything else — notably
    ``TimeoutError``, ``ConnectionError``, ``OSError`` — returns ``True``.
    """
    return not isinstance(exception, _PERMANENT_ERRORS)
