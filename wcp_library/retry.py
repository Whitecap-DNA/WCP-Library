"""Retry policies and tenacity strategy configs for wcp_library.

Every retry policy in the library is defined here. Consumers apply
them via tenacity's decorator/`Retrying`/`AsyncRetrying` surface:

    from tenacity import retry as tenacity_retry
    from wcp_library.retry import postgres_retry_kwargs

    @tenacity_retry(**postgres_retry_kwargs)
    def execute(self, query): ...

Four public policies:
* ``postgres_retry_kwargs`` -- tiered SQL retry for psycopg errors.
* ``oracle_retry_kwargs`` -- tiered SQL retry for oracledb errors.
* ``graph_retry_kwargs`` -- HTTP retry for Microsoft Graph calls.
* ``make_generic_retry(exceptions, ...)`` -- factory for arbitrary
  exception-list retry with exp backoff + jitter.
"""
import logging
import random

import oracledb
import psycopg
from tenacity import retry_if_exception, retry_if_exception_type, stop_after_attempt

logger = logging.getLogger(__name__)


# Module-private sentinel wrapper for Graph retry; kept here so the
# strategy can reference it. graph/__init__.py imports this symbol
# when building retryable exceptions.
class _GraphRetriable(Exception):
    """Raised inside wcp_library.graph._request to signal tenacity to retry.

    Module-private -- graph callers rely on `requests.exceptions.*` and the
    None-on-error contract of public helpers, not this sentinel.
    """
    def __init__(self, response=None, underlying=None):
        self.response = response
        self.underlying = underlying


# Public error-code constants (frozen sets for set-membership checks).
_POSTGRES_CONNECTION_LOSS = frozenset({"08001", "08004"})
_POSTGRES_TRANSIENT       = frozenset({"40P01"})
POSTGRES_RETRY_CODES      = _POSTGRES_CONNECTION_LOSS | _POSTGRES_TRANSIENT

_ORACLE_CONNECTION_LOSS   = frozenset({"ORA-01033", "DPY-6005", "DPY-4011"})
_ORACLE_TRANSIENT         = frozenset({"ORA-08103", "ORA-04021", "ORA-01652"})
ORACLE_RETRY_CODES        = _ORACLE_CONNECTION_LOSS | _ORACLE_TRANSIENT

GRAPH_RETRIABLE_STATUSES  = frozenset({429, 503, 504})


def _extract_full_code(exc: BaseException) -> str | None:
    """Pull ``full_code`` off the driver's error object if present.

    psycopg and oracledb both put a structured error object as
    ``exc.args[0]`` with a ``full_code`` attribute. Returns None when
    the exception doesn't fit that shape.
    """
    try:
        (error_obj,) = exc.args
    except (ValueError, TypeError):
        return None
    if isinstance(error_obj, str):
        return None
    return getattr(error_obj, "full_code", None)


def _before_sleep_log(retry_state) -> None:
    """Shared tenacity before_sleep hook; logs retry attempts at INFO."""
    exc = retry_state.outcome.exception()
    logger.info(
        "retry %d: %s -- %s",
        retry_state.attempt_number,
        type(exc).__name__,
        exc,
    )


def _make_sql_retry(
    catchable: tuple[type, ...],
    connection_loss_codes: frozenset[str],
    transient_codes: frozenset[str],
    name: str,
) -> dict:
    """Build tenacity kwargs for tiered SQL retry.

    * Connection-loss codes: fixed 300s wait (tolerate DB maintenance).
    * Transient codes: exp backoff + jitter (deadlocks / lock-busy
      resolve in milliseconds to seconds).
    """
    retriable_codes = connection_loss_codes | transient_codes

    def _should_retry(exc: BaseException) -> bool:
        if not isinstance(exc, catchable):
            return False
        return _extract_full_code(exc) in retriable_codes

    def _wait(retry_state) -> float:
        code = _extract_full_code(retry_state.outcome.exception())
        if code in connection_loss_codes:
            return 300.0
        return min(2 ** (retry_state.attempt_number - 1), 30) + random.uniform(0, 3)

    def _before_sleep(retry_state) -> None:
        code = _extract_full_code(retry_state.outcome.exception())
        logger.info(
            "%s retry %d: code=%s",
            name, retry_state.attempt_number, code,
        )

    return dict(
        retry=retry_if_exception(_should_retry),
        wait=_wait,
        stop=stop_after_attempt(50),
        before_sleep=_before_sleep,
        reraise=True,
    )


postgres_retry_kwargs = _make_sql_retry(
    catchable=(psycopg.OperationalError, psycopg.DatabaseError),
    connection_loss_codes=_POSTGRES_CONNECTION_LOSS,
    transient_codes=_POSTGRES_TRANSIENT,
    name="postgres",
)

oracle_retry_kwargs = _make_sql_retry(
    catchable=(oracledb.OperationalError, oracledb.DatabaseError),
    connection_loss_codes=_ORACLE_CONNECTION_LOSS,
    transient_codes=_ORACLE_TRANSIENT,
    name="oracle",
)


def _graph_wait(retry_state) -> float:
    exc = retry_state.outcome.exception()
    if isinstance(exc, _GraphRetriable) and exc.response is not None:
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return float(retry_after)
    return min(2 ** (retry_state.attempt_number - 1), 60) + random.uniform(0, 3)


graph_retry_kwargs = dict(
    retry=retry_if_exception_type(_GraphRetriable),
    wait=_graph_wait,
    stop=stop_after_attempt(5),
    before_sleep=_before_sleep_log,
    reraise=True,
)


def make_generic_retry(
    exceptions: type[BaseException] | tuple[type[BaseException], ...],
    max_attempts: int = 5,
    delay: int = 2,
    backoff: int = 2,
    jitter: int = 3,
) -> dict:
    """Build tenacity kwargs for arbitrary-exception retry with exp backoff + jitter.

    Policy-identical to the pre-1.12 ``wcp_library.retry`` decorator.

    :param exceptions: exception class or tuple to catch.
    :param max_attempts: total attempts before giving up.
    :param delay: initial delay (seconds).
    :param backoff: multiplier applied to delay each attempt.
    :param jitter: max random seconds added per retry.
    """
    def _wait(retry_state) -> float:
        return delay * (backoff ** (retry_state.attempt_number - 1)) + random.uniform(0, jitter)

    return dict(
        retry=retry_if_exception_type(exceptions),
        wait=_wait,
        stop=stop_after_attempt(max_attempts),
        before_sleep=_before_sleep_log,
        reraise=True,
    )
