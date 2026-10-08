"""Tests for wcp_library.retry (tenacity strategy configs)."""
from unittest.mock import MagicMock

import pytest

from wcp_library.retry import _extract_full_code


class TestExtractFullCode:
    def test_returns_code_when_error_obj_has_full_code(self):
        err_obj = MagicMock()
        err_obj.full_code = "08001"
        exc = Exception(err_obj)
        assert _extract_full_code(exc) == "08001"

    def test_returns_none_when_args_is_a_string(self):
        assert _extract_full_code(Exception("plain string")) is None

    def test_returns_none_when_args_is_empty(self):
        assert _extract_full_code(Exception()) is None

    def test_returns_none_when_args_has_multiple_items(self):
        assert _extract_full_code(Exception("a", "b")) is None

    def test_returns_none_when_error_obj_has_no_full_code(self):
        err_obj = object()  # no full_code attr
        assert _extract_full_code(Exception(err_obj)) is None


import psycopg
import oracledb

from wcp_library.retry import (
    postgres_retry_kwargs,
    oracle_retry_kwargs,
    POSTGRES_RETRY_CODES,
    ORACLE_RETRY_CODES,
)


def _mk_error(driver_exc_cls, full_code: str):
    obj = MagicMock()
    obj.full_code = full_code
    obj.message = "simulated"
    return driver_exc_cls(obj)


class TestPostgresRetryStrategy:
    def test_kwargs_are_complete(self):
        for key in ("retry", "wait", "stop", "before_sleep", "reraise"):
            assert key in postgres_retry_kwargs, key
        assert postgres_retry_kwargs["reraise"] is True

    def test_connection_loss_waits_300_seconds(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(psycopg.OperationalError, "08001")
        retry_state.attempt_number = 1
        wait = postgres_retry_kwargs["wait"](retry_state)
        assert wait == 300.0

    def test_transient_conflict_waits_sub_minute(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(psycopg.OperationalError, "40P01")
        retry_state.attempt_number = 1
        wait = postgres_retry_kwargs["wait"](retry_state)
        # Exp backoff at attempt 1 = 2^0 = 1 + up to 3s jitter
        assert 1.0 <= wait <= 4.0

    def test_retry_predicate_matches_retriable_codes(self):
        for code in POSTGRES_RETRY_CODES:
            retry_state = MagicMock()
            retry_state.outcome.exception.return_value = _mk_error(psycopg.OperationalError, code)
            assert postgres_retry_kwargs["retry"](retry_state), code

    def test_retry_predicate_rejects_non_retriable_code(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(psycopg.OperationalError, "99999")
        assert not postgres_retry_kwargs["retry"](retry_state)

    def test_retry_predicate_rejects_wrong_exception_type(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = ValueError("not a db error")
        assert not postgres_retry_kwargs["retry"](retry_state)

    def test_before_sleep_logs_wait_time(self, caplog):
        import logging
        caplog.set_level(logging.INFO, logger="wcp_library.retry")

        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(psycopg.OperationalError, "08001")
        retry_state.attempt_number = 1
        retry_state.next_action = MagicMock()
        retry_state.next_action.sleep = 300.0

        postgres_retry_kwargs["before_sleep"](retry_state)

        # At least one INFO-level record from wcp_library.retry mentions waiting 300.0s
        matching = [r for r in caplog.records if "waiting 300.0s" in r.getMessage()]
        assert matching, f"Expected log record mentioning 'waiting 300.0s'; got {[r.getMessage() for r in caplog.records]}"


class TestOracleRetryStrategy:
    def test_connection_loss_waits_300_seconds(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(oracledb.OperationalError, "ORA-01033")
        retry_state.attempt_number = 1
        wait = oracle_retry_kwargs["wait"](retry_state)
        assert wait == 300.0

    def test_transient_exp_backoff(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(oracledb.OperationalError, "ORA-08103")
        retry_state.attempt_number = 1
        wait = oracle_retry_kwargs["wait"](retry_state)
        assert 1.0 <= wait <= 4.0

    def test_all_retry_codes_match(self):
        for code in ORACLE_RETRY_CODES:
            retry_state = MagicMock()
            retry_state.outcome.exception.return_value = _mk_error(oracledb.OperationalError, code)
            assert oracle_retry_kwargs["retry"](retry_state), code


import requests

from wcp_library.retry import graph_retry_kwargs, _GraphRetriable


def _mk_graph_retry_response(status_code: int, retry_after: str | None = None):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.headers = {"Retry-After": retry_after} if retry_after else {}
    return resp


class TestGraphRetryStrategy:
    def test_kwargs_are_complete(self):
        for key in ("retry", "wait", "stop", "before_sleep", "reraise"):
            assert key in graph_retry_kwargs, key

    def test_retry_after_header_honored(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _GraphRetriable(
            response=_mk_graph_retry_response(429, retry_after="42")
        )
        retry_state.attempt_number = 1
        wait = graph_retry_kwargs["wait"](retry_state)
        assert wait == 42.0

    def test_exp_backoff_when_no_retry_after(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _GraphRetriable(
            response=_mk_graph_retry_response(503)
        )
        retry_state.attempt_number = 1
        wait = graph_retry_kwargs["wait"](retry_state)
        assert 1.0 <= wait <= 4.0

    def test_exp_backoff_on_network_error(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _GraphRetriable(
            underlying=requests.ConnectionError("boom")
        )
        retry_state.attempt_number = 2
        wait = graph_retry_kwargs["wait"](retry_state)
        assert 2.0 <= wait <= 5.0

    def test_non_numeric_retry_after_falls_back_to_backoff(self):
        """HTTP-date Retry-After falls back to exp backoff."""
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _GraphRetriable(
            response=_mk_graph_retry_response(429, retry_after="Wed, 21 Oct 2015 07:28:00 GMT")
        )
        retry_state.attempt_number = 1
        wait = graph_retry_kwargs["wait"](retry_state)
        assert 1.0 <= wait <= 4.0

    def test_retry_predicate_matches_graph_retriable(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _GraphRetriable()
        assert graph_retry_kwargs["retry"](retry_state)

    def test_retry_predicate_rejects_other_exceptions(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = requests.HTTPError("500 Internal")
        assert not graph_retry_kwargs["retry"](retry_state)


from wcp_library.retry import make_generic_retry


class TestMakeGenericRetry:
    def test_defaults_produce_valid_kwargs(self):
        kwargs = make_generic_retry(exceptions=ValueError)
        for key in ("retry", "wait", "stop", "before_sleep", "reraise"):
            assert key in kwargs

    def test_exp_backoff_grows_with_attempt(self):
        kwargs = make_generic_retry(exceptions=ValueError, delay=1, backoff=2, jitter=0)

        rs_1 = MagicMock(); rs_1.attempt_number = 1
        rs_2 = MagicMock(); rs_2.attempt_number = 2
        rs_3 = MagicMock(); rs_3.attempt_number = 3

        # delay * backoff**(attempt-1) with jitter=0
        assert kwargs["wait"](rs_1) == 1   # 1 * 2^0
        assert kwargs["wait"](rs_2) == 2   # 1 * 2^1
        assert kwargs["wait"](rs_3) == 4   # 1 * 2^2

    def test_jitter_adds_randomness(self):
        kwargs = make_generic_retry(exceptions=ValueError, delay=10, backoff=1, jitter=5)
        rs = MagicMock(); rs.attempt_number = 1
        waits = {kwargs["wait"](rs) for _ in range(20)}
        # Expect multiple distinct values due to jitter
        assert len(waits) > 1

    def test_retries_specified_exception(self):
        kwargs = make_generic_retry(exceptions=(ValueError, KeyError))
        rs_val = MagicMock(); rs_val.outcome.exception.return_value = ValueError()
        rs_key = MagicMock(); rs_key.outcome.exception.return_value = KeyError()
        rs_type = MagicMock(); rs_type.outcome.exception.return_value = TypeError()

        assert kwargs["retry"](rs_val)
        assert kwargs["retry"](rs_key)
        assert not kwargs["retry"](rs_type)
