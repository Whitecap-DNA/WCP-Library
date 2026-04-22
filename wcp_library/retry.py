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
