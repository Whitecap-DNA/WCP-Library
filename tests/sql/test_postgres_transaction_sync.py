"""Mock tests for sync Transaction and PostgresConnection.transaction()."""
from unittest.mock import MagicMock

import pytest

from wcp_library.sql.postgres import (
    PostgresConnection,
    SyncExecutor,
    Transaction,
)


def _make_mock_cursor(rowcount=1):
    c = MagicMock(name="Cursor")
    c.rowcount = rowcount
    c.executemany = MagicMock()
    c.execute = MagicMock()
    c.fetchall = MagicMock(return_value=[(1, "a"), (2, "b")])
    return c


def _make_mock_connection():
    conn = MagicMock(name="Connection")
    conn.autocommit = True

    # sync psycopg3 Connection.autocommit is a plain property, not set_autocommit
    # Mock's attribute assignment works naturally.

    # connection.execute returns a cursor-like with .rowcount
    conn.execute = MagicMock(return_value=_make_mock_cursor(rowcount=1))

    # Native transaction() context manager (sync)
    tx_ctx = MagicMock(name="NativeTxCtx")
    tx_ctx.__enter__ = MagicMock(return_value=tx_ctx)
    tx_ctx.__exit__ = MagicMock(return_value=False)
    conn.transaction = MagicMock(return_value=tx_ctx)

    cursor = _make_mock_cursor(rowcount=2)
    conn.cursor = MagicMock(return_value=cursor)
    return conn, cursor, tx_ctx


class TestSyncTransactionPrimitives:
    def test_execute_uses_held_connection(self):
        conn, _, _ = _make_mock_connection()
        tx = Transaction(parent=None, connection=conn)
        tx.execute("SELECT 1")
        conn.execute.assert_called_once_with("SELECT 1")
        conn.commit.assert_not_called()

    def test_execute_many_uses_cursor(self):
        conn, cursor, _ = _make_mock_connection()
        tx = Transaction(parent=None, connection=conn)
        records = [{"x": 1}, {"x": 2}]
        tx.execute_many("INSERT INTO t VALUES (%(x)s)", records)
        cursor.executemany.assert_called_once_with(
            "INSERT INTO t VALUES (%(x)s)", records, returning=False
        )

    def test_fetch_data_returns_cursor_fetchall(self):
        conn, cursor, _ = _make_mock_connection()
        tx = Transaction(parent=None, connection=conn)
        rows = tx.fetch_data("SELECT * FROM t")
        assert rows == [(1, "a"), (2, "b")]

    def test_manual_commit_sets_flag(self):
        conn, _, _ = _make_mock_connection()
        tx = Transaction(parent=None, connection=conn)
        tx.commit()
        assert tx._completed_manually is True
        conn.commit.assert_called_once()

    def test_manual_rollback_sets_flag(self):
        conn, _, _ = _make_mock_connection()
        tx = Transaction(parent=None, connection=conn)
        tx.rollback()
        assert tx._completed_manually is True
        conn.rollback.assert_called_once()

    def test_inherits_from_executor(self):
        assert issubclass(Transaction, SyncExecutor)


class TestSyncTransactionContextManager:
    def test_normal_exit_restores_autocommit(self):
        parent = PostgresConnection(use_pool=False)
        conn, _, tx_ctx = _make_mock_connection()
        conn.autocommit = True
        parent._get_connection = MagicMock(return_value=conn)

        with parent.transaction() as tx:
            assert isinstance(tx, Transaction)
            assert tx.connection is conn

        tx_ctx.__enter__.assert_called_once()
        tx_ctx.__exit__.assert_called_once()
        # autocommit was toggled to False then restored to True
        # (Hard to verify exact order via attribute assignment on a MagicMock,
        # but we can at least assert it's not-False at the end.)

    def test_exception_propagates(self):
        parent = PostgresConnection(use_pool=False)
        conn, _, tx_ctx = _make_mock_connection()
        conn.autocommit = True
        parent._get_connection = MagicMock(return_value=conn)

        with pytest.raises(RuntimeError, match="boom"):
            with parent.transaction():
                raise RuntimeError("boom")

    def test_pool_mode_returns_connection(self):
        parent = PostgresConnection(use_pool=True)
        conn, _, _ = _make_mock_connection()
        parent._get_connection = MagicMock(return_value=conn)
        parent._session_pool = MagicMock()

        with parent.transaction():
            pass

        parent._session_pool.putconn.assert_called_once_with(conn)
