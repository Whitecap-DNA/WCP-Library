"""Mock tests for AsyncPostgresConnection.retry_transaction.

No live DB. The transaction() context manager is monkeypatched with a
lightweight async context, and asyncio.sleep is stubbed so retries
don't actually sleep 5 minutes.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import psycopg
import pytest
from tenacity import stop_after_attempt

from wcp_library.retry import postgres_retry_kwargs
from wcp_library.sql.postgres import AsyncPostgresConnection


class _FakeErrorObj:
    """Simulates the psycopg error.args[0] object that carries full_code/message."""
    def __init__(self, full_code, message="simulated"):
        self.full_code = full_code
        self.message = message


def _retriable_error():
    """Build a psycopg.OperationalError whose args[0].full_code is retriable."""
    err_obj = _FakeErrorObj(full_code="08001")
    e = psycopg.OperationalError(err_obj)
    return e


def _non_retriable_error():
    """Build a psycopg.OperationalError whose args[0].full_code is NOT retriable."""
    err_obj = _FakeErrorObj(full_code="99999")
    e = psycopg.OperationalError(err_obj)
    return e


@pytest.fixture
def conn_with_stub_transaction():
    """An AsyncPostgresConnection whose transaction() yields an AsyncMock tx."""
    conn = AsyncPostgresConnection(use_pool=False)

    # Replace transaction() with a dummy async context manager that yields an AsyncMock.
    class _DummyCtx:
        def __init__(self):
            self.tx = MagicMock(name="AsyncTransaction")

        async def __aenter__(self):
            return self.tx

        async def __aexit__(self, *a):
            return False

    conn.transaction = lambda: _DummyCtx()
    return conn


class TestRetryTransactionSuccess:
    async def test_succeeds_on_first_attempt(self, conn_with_stub_transaction):
        call_count = 0

        async def fn(tx):
            nonlocal call_count
            call_count += 1
            return "ok"

        result = await conn_with_stub_transaction.retry_transaction(fn)
        assert result == "ok"
        assert call_count == 1

    async def test_no_sleep_on_first_attempt_success(
        self, conn_with_stub_transaction
    ):
        async def fn(tx):
            return None

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await conn_with_stub_transaction.retry_transaction(fn)
        assert mock_sleep.await_count == 0


class TestRetryTransactionNonRetriable:
    async def test_non_retriable_error_propagates(self, conn_with_stub_transaction):
        async def fn(tx):
            raise _non_retriable_error()

        with pytest.raises(psycopg.OperationalError):
            await conn_with_stub_transaction.retry_transaction(fn)

    async def test_string_error_propagates(self, conn_with_stub_transaction):
        # If error.args[0] is a plain string (no full_code attribute), not retriable.
        async def fn(tx):
            raise psycopg.OperationalError("just a string")

        with pytest.raises(psycopg.OperationalError):
            await conn_with_stub_transaction.retry_transaction(fn)


class TestRetryTransactionRetries:
    async def test_retries_on_retriable_error_then_succeeds(
        self, conn_with_stub_transaction
    ):
        attempts = []

        async def fn(tx):
            attempts.append(len(attempts) + 1)
            if len(attempts) == 1:
                raise _retriable_error()
            return "finally ok"

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await conn_with_stub_transaction.retry_transaction(fn)

        assert result == "finally ok"
        assert len(attempts) == 2
        mock_sleep.assert_awaited_once_with(300.0)

    async def test_retry_limit_respected(
        self, conn_with_stub_transaction, monkeypatch
    ):
        # Lower retry_limit to 2 attempts total by patching the stop condition.
        # monkeypatch.setitem ensures the module-level dict is reverted after test.
        monkeypatch.setitem(postgres_retry_kwargs, "stop", stop_after_attempt(2))

        attempts = []

        async def fn(tx):
            attempts.append(1)
            raise _retriable_error()

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(psycopg.OperationalError):
                await conn_with_stub_transaction.retry_transaction(fn)

        assert len(attempts) == 2
