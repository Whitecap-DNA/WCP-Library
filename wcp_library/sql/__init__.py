import asyncio
import logging
from functools import wraps
from time import sleep

import oracledb
import psycopg

logger = logging.getLogger(__name__)


_RETRIABLE_DB_ERRORS = (
    oracledb.OperationalError,
    oracledb.DatabaseError,
    psycopg.OperationalError,
    psycopg.DatabaseError,
)

_RETRY_SLEEP_SECONDS = 300


def classify_db_exception(exc: BaseException, retry_error_codes) -> tuple[str, str | None, str | None]:
    """Classify a database exception for retry-loop logic.

    Inspects the exception's ``args`` for the driver-specific error object
    that carries a ``full_code`` attribute.

    :return: ``(action, full_code, message)`` where ``action`` is one of:

        * ``"retry"`` — error has a ``full_code`` in ``retry_error_codes``;
          the caller should sleep and retry (subject to its own attempt limit).
        * ``"non_retriable"`` — error has a ``full_code`` NOT in
          ``retry_error_codes``; the caller should log and re-raise.
        * ``"raise"`` — exception doesn't fit the expected shape
          (e.g., ``args[0]`` is a plain string, or ``full_code`` is absent);
          the caller should re-raise without logging.
    """
    try:
        (error_obj,) = exc.args
    except (ValueError, TypeError):
        return ("raise", None, None)
    if isinstance(error_obj, str):
        return ("raise", None, None)
    full_code = getattr(error_obj, "full_code", None)
    if full_code is None:
        return ("raise", None, None)
    message = getattr(error_obj, "message", None)
    if full_code in retry_error_codes:
        return ("retry", full_code, message)
    return ("non_retriable", full_code, message)


def _log_retry(attempt: int, limit: int, full_code: str, message: str | None, context: str) -> None:
    logger.debug("Database error: %s", full_code)
    if message:
        logger.debug("Error message: %s", message)
    logger.info(
        "Retry attempt %d/%d. Waiting 5 minutes before retrying %s",
        attempt,
        limit,
        context,
    )


def _log_giveup(full_code: str, limit: int, action: str) -> None:
    if action == "non_retriable":
        logger.error("Non-retryable database error: %s", full_code)
    else:
        logger.error("Retry limit (%d) reached for error: %s", limit, full_code)


def retry(func: callable) -> callable:
    """
    Decorator to retry a function on retriable database errors.

    Retries any call that raises a DB error whose ``full_code`` is listed in
    ``self.retry_error_codes``, waiting 5 minutes between attempts, up to
    ``self.retry_limit`` times. Other DB errors propagate immediately.
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self._retry_count = 0
        while True:
            try:
                return func(self, *args, **kwargs)
            except _RETRIABLE_DB_ERRORS as e:
                action, full_code, message = classify_db_exception(e, self.retry_error_codes)
                if action == "raise":
                    raise
                if action == "non_retriable" or self._retry_count >= self.retry_limit:
                    _log_giveup(full_code, self.retry_limit, action)
                    raise
                self._retry_count += 1
                _log_retry(self._retry_count, self.retry_limit, full_code, message, "connection")
                sleep(_RETRY_SLEEP_SECONDS)
    return wrapper


def async_retry(func: callable) -> callable:
    """
    Async decorator to retry a coroutine on retriable database errors.

    See :func:`retry` for policy details.
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        self._retry_count = 0
        while True:
            try:
                return await func(self, *args, **kwargs)
            except _RETRIABLE_DB_ERRORS as e:
                action, full_code, message = classify_db_exception(e, self.retry_error_codes)
                if action == "raise":
                    raise
                if action == "non_retriable" or self._retry_count >= self.retry_limit:
                    _log_giveup(full_code, self.retry_limit, action)
                    raise
                self._retry_count += 1
                _log_retry(self._retry_count, self.retry_limit, full_code, message, "connection")
                await asyncio.sleep(_RETRY_SLEEP_SECONDS)
    return wrapper
