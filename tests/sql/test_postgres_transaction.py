"""Mock tests for AsyncTransaction and AsyncPostgresConnection.transaction().

No live Postgres -- the connection is an AsyncMock. We verify:
  - The context manager checks out a connection, sets autocommit=False,
    enters the psycopg native transaction, yields an AsyncTransaction,
    restores autocommit on exit, and (when pooled) returns the connection.
  - AsyncTransaction primitives call through to the held connection
    without committing.
  - Manual tx.commit() / tx.rollback() set _completed_manually.
  - Composite methods (inherited from AsyncExecutor) dispatch to
    AsyncTransaction primitives.
"""
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest

from wcp_library.sql.postgres import (
    AsyncExecutor,
    AsyncPostgresConnection,
    AsyncTransaction,
)


def _make_mock_cursor(rowcount=1):
    """Cursor-like MagicMock with a settable ``rowcount``."""
    c = MagicMock(name="Cursor")
    c.rowcount = rowcount
    c.executemany = AsyncMock()
    c.execute = AsyncMock()
    c.fetchall = AsyncMock(return_value=[(1, "a"), (2, "b")])
    return c


def _make_mock_connection():
    """Build a MagicMock mimicking a psycopg3 AsyncConnection."""
    conn = MagicMock(name="AsyncConnection")
    conn.autocommit = True  # snapshot of prior state

    # set_autocommit is an async method in psycopg3
    conn.set_autocommit = AsyncMock()
    # connection.execute returns a cursor-like with .rowcount
    conn.execute = AsyncMock(return_value=_make_mock_cursor(rowcount=1))
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock()

    # psycopg's connection.transaction() is a native async context manager
    tx_ctx = MagicMock(name="NativeTxCtx")
    tx_ctx.__aenter__ = AsyncMock(return_value=tx_ctx)
    tx_ctx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=tx_ctx)

    # cursor() is sync-returning; executemany/execute on the cursor are async
    cursor = _make_mock_cursor(rowcount=2)
    conn.cursor = MagicMock(return_value=cursor)
    return conn, cursor, tx_ctx


class TestAsyncTransactionPrimitives:
    async def test_execute_uses_held_connection(self):
        conn, _, _ = _make_mock_connection()
        tx = AsyncTransaction(parent=None, connection=conn)
        await tx.execute("SELECT 1")
        conn.execute.assert_awaited_once_with("SELECT 1")
        conn.commit.assert_not_awaited()  # no per-call commit

    async def test_safe_execute_passes_values(self):
        conn, _, _ = _make_mock_connection()
        tx = AsyncTransaction(parent=None, connection=conn)
        await tx.safe_execute("INSERT INTO t VALUES (%s)", ("a",))
        conn.execute.assert_awaited_once_with("INSERT INTO t VALUES (%s)", ("a",))

    async def test_execute_many_uses_cursor(self):
        conn, cursor, _ = _make_mock_connection()
        tx = AsyncTransaction(parent=None, connection=conn)
        records = [{"x": 1}, {"x": 2}]
        await tx.execute_many("INSERT INTO t VALUES (%(x)s)", records)
        cursor.executemany.assert_awaited_once_with(
            "INSERT INTO t VALUES (%(x)s)", records, returning=False
        )

    async def test_fetch_data_returns_cursor_fetchall(self):
        conn, cursor, _ = _make_mock_connection()
        tx = AsyncTransaction(parent=None, connection=conn)
        rows = await tx.fetch_data("SELECT * FROM t")
        assert rows == [(1, "a"), (2, "b")]
        cursor.execute.assert_awaited_once_with("SELECT * FROM t")

    async def test_manual_commit_sets_flag(self):
        conn, _, _ = _make_mock_connection()
        tx = AsyncTransaction(parent=None, connection=conn)
        await tx.commit()
        assert tx._completed_manually is True
        conn.commit.assert_awaited_once()

    async def test_manual_rollback_sets_flag(self):
        conn, _, _ = _make_mock_connection()
        tx = AsyncTransaction(parent=None, connection=conn)
        await tx.rollback()
        assert tx._completed_manually is True
        conn.rollback.assert_awaited_once()

    def test_inherits_from_executor(self):
        assert issubclass(AsyncTransaction, AsyncExecutor)

    def test_connection_property(self):
        conn, _, _ = _make_mock_connection()
        tx = AsyncTransaction(parent=None, connection=conn)
        assert tx.connection is conn


class TestAsyncTransactionContextManager:
    async def test_normal_exit_restores_autocommit(self):
        parent = AsyncPostgresConnection(use_pool=False)
        conn, _, tx_ctx = _make_mock_connection()
        conn.autocommit = True
        parent._get_connection = AsyncMock(return_value=conn)

        async with parent.transaction() as tx:
            assert isinstance(tx, AsyncTransaction)
            assert tx.connection is conn

        # Entered psycopg native tx
        tx_ctx.__aenter__.assert_awaited_once()
        tx_ctx.__aexit__.assert_awaited_once()

        # Autocommit: toggled to False, then restored to True
        set_ac_calls = [c.args for c in conn.set_autocommit.await_args_list]
        assert set_ac_calls == [(False,), (True,)]

    async def test_exception_propagates_and_autocommit_restored(self):
        parent = AsyncPostgresConnection(use_pool=False)
        conn, _, tx_ctx = _make_mock_connection()
        conn.autocommit = True
        parent._get_connection = AsyncMock(return_value=conn)

        # Simulate psycopg's tx context raising on exit (propagating the inner error)
        tx_ctx.__aexit__ = AsyncMock(side_effect=lambda *a: False)

        with pytest.raises(RuntimeError, match="boom"):
            async with parent.transaction() as tx:
                raise RuntimeError("boom")

        # Autocommit still restored despite the exception
        set_ac_calls = [c.args for c in conn.set_autocommit.await_args_list]
        assert set_ac_calls == [(False,), (True,)]

    async def test_pool_mode_returns_connection_on_exit(self):
        parent = AsyncPostgresConnection(use_pool=True)
        conn, _, _ = _make_mock_connection()
        conn.autocommit = True
        parent._get_connection = AsyncMock(return_value=conn)
        parent._session_pool = MagicMock()
        parent._session_pool.putconn = AsyncMock()

        async with parent.transaction():
            pass

        parent._session_pool.putconn.assert_awaited_once_with(conn)

    async def test_nopool_mode_does_not_call_putconn(self):
        parent = AsyncPostgresConnection(use_pool=False)
        conn, _, _ = _make_mock_connection()
        conn.autocommit = True
        parent._get_connection = AsyncMock(return_value=conn)
        # _session_pool shouldn't even be touched; set to a sentinel
        parent._session_pool = None

        async with parent.transaction():
            pass
        # No AttributeError means we never tried to putconn


class TestCompositesThroughTransaction:
    async def test_truncate_table_via_transaction_calls_execute(self):
        conn, _, _ = _make_mock_connection()
        tx = AsyncTransaction(parent=None, connection=conn)

        await tx.truncate_table("public.t")
        # Composite lives on AsyncExecutor, calls self.execute -> AsyncTransaction.execute.
        # The query is a psycopg Composed: Composed([SQL('TRUNCATE TABLE '), Identifier('public','t')])
        conn.execute.assert_awaited_once()
        called_query = conn.execute.await_args.args[0]
        s = str(called_query)
        assert "TRUNCATE TABLE" in s
        assert "'public'" in s and "'t'" in s

    async def test_empty_table_via_transaction_calls_execute(self):
        conn, _, _ = _make_mock_connection()
        tx = AsyncTransaction(parent=None, connection=conn)

        await tx.empty_table("public.t")
        conn.execute.assert_awaited_once()
        called_query = conn.execute.await_args.args[0]
        assert "DELETE FROM" in str(called_query)

    async def test_export_df_via_transaction_uses_execute_many(self):
        conn, cursor, _ = _make_mock_connection()
        tx = AsyncTransaction(parent=None, connection=conn)

        df = pd.DataFrame([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
        await tx.export_df_to_warehouse(df, "t", columns=["id", "v"])

        cursor.executemany.assert_awaited_once()
        query = cursor.executemany.await_args.args[0]
        # Query is a psycopg Composed object, not a plain string
        assert "INSERT INTO" in str(query)
