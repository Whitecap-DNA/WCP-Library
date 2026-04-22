"""Mock tests for sync PostgresConnection.retry_transaction."""
from unittest.mock import MagicMock, patch

import psycopg
import pytest

from wcp_library.sql.postgres import (
    PostgresConnection,
    postgres_retry_codes,
)


class _FakeErrorObj:
    def __init__(self, full_code, message="simulated"):
        self.full_code = full_code
        self.message = message


def _retriable_error():
    err_obj = _FakeErrorObj(full_code=postgres_retry_codes[0])
    return psycopg.OperationalError(err_obj)


def _non_retriable_error():
    err_obj = _FakeErrorObj(full_code="99999")
    return psycopg.OperationalError(err_obj)


@pytest.fixture
def conn_with_stub_transaction():
    conn = PostgresConnection(use_pool=False)

    class _DummyCtx:
        def __init__(self):
            self.tx = MagicMock(name="Transaction")

        def __enter__(self):
            return self.tx

        def __exit__(self, *a):
            return False

    conn.transaction = lambda: _DummyCtx()
    return conn


class TestSyncRetryTransaction:
    def test_succeeds_first_attempt(self, conn_with_stub_transaction):
        calls = 0

        def fn(tx):
            nonlocal calls
            calls += 1
            return "ok"

        assert conn_with_stub_transaction.retry_transaction(fn) == "ok"
        assert calls == 1

    def test_non_retriable_error_propagates(self, conn_with_stub_transaction):
        def fn(tx):
            raise _non_retriable_error()

        with pytest.raises(psycopg.OperationalError):
            conn_with_stub_transaction.retry_transaction(fn)

    def test_string_error_propagates(self, conn_with_stub_transaction):
        def fn(tx):
            raise psycopg.OperationalError("just a string")

        with pytest.raises(psycopg.OperationalError):
            conn_with_stub_transaction.retry_transaction(fn)

    def test_retries_then_succeeds(self, conn_with_stub_transaction):
        attempts = []

        def fn(tx):
            attempts.append(1)
            if len(attempts) == 1:
                raise _retriable_error()
            return "ok"

        with patch("time.sleep") as mock_sleep:
            result = conn_with_stub_transaction.retry_transaction(fn)

        assert result == "ok"
        assert len(attempts) == 2
        mock_sleep.assert_called_once_with(300)

    def test_retry_limit_respected(self, conn_with_stub_transaction):
        conn_with_stub_transaction.retry_limit = 2
        attempts = []

        def fn(tx):
            attempts.append(1)
            raise _retriable_error()

        with patch("time.sleep"):
            with pytest.raises(psycopg.OperationalError):
                conn_with_stub_transaction.retry_transaction(fn)

        assert len(attempts) == 3
