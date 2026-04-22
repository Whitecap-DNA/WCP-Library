"""Mock tests for wcp_library/sql/oracle.py.

Covers OracleConnection (sync) and AsyncOracleConnection (async) surface:
- Primitives: execute, safe_execute, execute_multiple, execute_many, fetch_data
- Composites: export_df_to_warehouse, upsert (via execute_many path),
  truncate_table, empty_table, remove_matching_data
- Lifecycle: __init__, set_user, close_connection, context managers
- Retry behavior for retriable vs non-retriable oracledb errors
"""
from unittest.mock import AsyncMock, MagicMock, patch

import oracledb
import pandas as pd
import pytest

from wcp_library.sql.oracle import (
    AsyncOracleConnection,
    OracleConnection,
    _quote_identifier,
    oracle_retry_codes,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


class _FakeErrorObj:
    """Mimics oracledb's internal error object with ``full_code`` + ``message``."""

    def __init__(self, full_code, message="simulated"):
        self.full_code = full_code
        self.message = message


def _retriable_oracle_error():
    """Build an OperationalError wrapping an error obj with a retriable full_code."""
    return oracledb.OperationalError(_FakeErrorObj(oracle_retry_codes[0]))


def _non_retriable_oracle_error():
    """Build an OperationalError wrapping a non-retriable error obj."""
    return oracledb.OperationalError(_FakeErrorObj("ORA-99999"))


@pytest.fixture
def mock_sync_cursor():
    cursor = MagicMock(name="SyncCursor")
    cursor.execute = MagicMock()
    cursor.executemany = MagicMock()
    cursor.fetchall = MagicMock(return_value=[(1, "a"), (2, "b")])
    return cursor


@pytest.fixture
def mock_sync_conn(mock_sync_cursor):
    conn = MagicMock(name="OracleSyncConnection")
    conn.cursor = MagicMock(return_value=mock_sync_cursor)
    conn.commit = MagicMock()
    conn.rollback = MagicMock()
    conn.close = MagicMock()
    conn.is_healthy = MagicMock(return_value=True)
    return conn


@pytest.fixture
def sync_oracle(mock_sync_conn):
    """OracleConnection pre-wired with an injected mock connection (non-pool)."""
    oc = OracleConnection(use_pool=False)
    oc._connection = mock_sync_conn
    return oc


def _make_async_cursor():
    """Build a MagicMock that supports `with connection.cursor() as cursor`.

    `connection.cursor()` in the async Oracle API returns an object that is
    used as a sync context manager (`with`, not `async with`), but the
    cursor methods themselves are awaitables.
    """
    cursor = MagicMock(name="AsyncCursor")
    cursor.execute = AsyncMock()
    cursor.executemany = AsyncMock()
    cursor.fetchall = AsyncMock(return_value=[(1, "a"), (2, "b")])
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    return cursor


def _make_async_conn():
    cursor = _make_async_cursor()
    conn = MagicMock(name="OracleAsyncConnection")
    conn.cursor = MagicMock(return_value=cursor)
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock()
    conn.close = AsyncMock()
    conn.is_healthy = MagicMock(return_value=True)
    return conn, cursor


@pytest.fixture
def async_conn_pair():
    return _make_async_conn()


@pytest.fixture
def async_oracle(async_conn_pair):
    """AsyncOracleConnection pre-wired with an injected async mock connection."""
    conn, _cursor = async_conn_pair
    ao = AsyncOracleConnection(use_pool=False)
    ao._connection = conn
    return ao


# ---------------------------------------------------------------------------
# Module-level helper tests
# ---------------------------------------------------------------------------


class TestQuoteIdentifier:
    def test_simple(self):
        assert _quote_identifier("foo") == '"FOO"'

    def test_schema_qualified(self):
        assert _quote_identifier("schema.table") == '"SCHEMA"."TABLE"'

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            _quote_identifier("")

    def test_rejects_injection(self):
        with pytest.raises(ValueError):
            _quote_identifier("foo; DROP TABLE bar")


# ---------------------------------------------------------------------------
# OracleConnection - construction & credentials
# ---------------------------------------------------------------------------


class TestOracleConnectionInit:
    def test_defaults(self):
        oc = OracleConnection()
        assert oc.use_pool is False
        assert oc.min_connections == 2
        assert oc.max_connections == 5
        assert oc._connection is None
        assert oc._session_pool is None
        assert oc._username is None
        assert oc._password is None
        assert oc._hostname is None
        assert oc._port is None
        assert oc._database is None
        assert oc._sid is None
        assert oc.retry_limit == 50
        assert oc.retry_error_codes == oracle_retry_codes
        assert oc._retry_count == 0

    def test_custom_pool_options(self):
        oc = OracleConnection(use_pool=True, min_connections=3, max_connections=10)
        assert oc.use_pool is True
        assert oc.min_connections == 3
        assert oc.max_connections == 10


class TestOracleConnectionSetUser:
    def test_requires_service_or_sid(self):
        oc = OracleConnection(use_pool=False)
        with pytest.raises(ValueError, match="Service or SID"):
            oc.set_user({"UserName": "u", "Password": "p", "Host": "h", "Port": 1521})

    def test_sets_fields_and_invokes_connect_with_service(self, mock_sync_conn):
        oc = OracleConnection(use_pool=False)
        with patch(
            "wcp_library.sql.oracle._connect_warehouse",
            return_value=mock_sync_conn,
        ) as mocked_connect:
            oc.set_user(
                {
                    "UserName": "user",
                    "Password": "pw",
                    "Host": "example.com",
                    "Port": "1521",
                    "Service": "SVC",
                }
            )
        assert oc._username == "user"
        assert oc._password == "pw"
        assert oc._hostname == "example.com"
        assert oc._port == 1521
        assert oc._database == "SVC"
        assert oc._sid is None
        assert oc._connection is mock_sync_conn
        mocked_connect.assert_called_once()
        # service supplied => third-to-last positional was the sid/service value
        args, _kwargs = mocked_connect.call_args
        assert args[0] == "user"
        assert args[4] == "SVC"

    def test_uses_sid_when_service_absent(self, mock_sync_conn):
        oc = OracleConnection(use_pool=False)
        with patch(
            "wcp_library.sql.oracle._connect_warehouse",
            return_value=mock_sync_conn,
        ) as mocked_connect:
            oc.set_user(
                {
                    "UserName": "user",
                    "Password": "pw",
                    "Host": "example.com",
                    "Port": 1521,
                    "SID": "MYSID",
                }
            )
        assert oc._database is None
        assert oc._sid == "MYSID"
        args, _kwargs = mocked_connect.call_args
        assert args[4] == "MYSID"


class TestOracleConnectionCloseAndDestructor:
    def test_close_connection_non_pool(self, sync_oracle, mock_sync_conn):
        sync_oracle.close_connection()
        mock_sync_conn.close.assert_called_once()
        assert sync_oracle._connection is None

    def test_close_connection_unhealthy_no_close_called(self, mock_sync_conn):
        oc = OracleConnection(use_pool=False)
        oc._connection = mock_sync_conn
        mock_sync_conn.is_healthy.return_value = False
        oc.close_connection()
        mock_sync_conn.close.assert_not_called()
        assert oc._connection is None

    def test_close_connection_pool(self):
        oc = OracleConnection(use_pool=True)
        oc._session_pool = MagicMock()
        oc.close_connection()
        oc._session_pool.close.assert_called_once()

    def test_context_manager_exit_closes(self):
        oc = OracleConnection(use_pool=False)
        conn = MagicMock()
        conn.is_healthy.return_value = True
        oc._connection = conn
        with oc as handle:
            assert handle is oc
        conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# OracleConnection - primitives
# ---------------------------------------------------------------------------


class TestOracleConnectionExecute:
    def test_execute_calls_cursor_execute_and_commits(self, sync_oracle, mock_sync_conn, mock_sync_cursor):
        sync_oracle.execute("SELECT 1 FROM DUAL")
        mock_sync_cursor.execute.assert_called_once_with("SELECT 1 FROM DUAL")
        mock_sync_conn.commit.assert_called_once()

    def test_execute_non_retriable_error_raises(self, sync_oracle, mock_sync_cursor):
        mock_sync_cursor.execute.side_effect = _non_retriable_oracle_error()
        with pytest.raises(oracledb.OperationalError):
            sync_oracle.execute("SELECT 1 FROM DUAL")

    def test_execute_pool_release(self, mock_sync_conn, mock_sync_cursor):
        oc = OracleConnection(use_pool=True)
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=mock_sync_conn)
        oc._session_pool = pool
        oc.execute("SELECT 1 FROM DUAL")
        mock_sync_cursor.execute.assert_called_once_with("SELECT 1 FROM DUAL")
        pool.release.assert_called_once_with(mock_sync_conn)


class TestOracleConnectionSafeExecute:
    def test_happy_path(self, sync_oracle, mock_sync_conn, mock_sync_cursor):
        packed = {"a": 1}
        sync_oracle.safe_execute("INSERT INTO t VALUES (:a)", packed)
        mock_sync_cursor.execute.assert_called_once_with("INSERT INTO t VALUES (:a)", packed)
        mock_sync_conn.commit.assert_called_once()

    def test_non_retriable_error_raises(self, sync_oracle, mock_sync_cursor):
        mock_sync_cursor.execute.side_effect = _non_retriable_oracle_error()
        with pytest.raises(oracledb.OperationalError):
            sync_oracle.safe_execute("INSERT INTO t VALUES (:a)", {"a": 1})


class TestOracleConnectionExecuteMultiple:
    def test_runs_each_query(self, sync_oracle, mock_sync_conn, mock_sync_cursor):
        queries = [
            ("SELECT 1 FROM DUAL", None),
            ("INSERT INTO t VALUES (:a)", {"a": 1}),
        ]
        sync_oracle.execute_multiple(queries)
        assert mock_sync_cursor.execute.call_count == 2
        first_call, second_call = mock_sync_cursor.execute.call_args_list
        assert first_call.args == ("SELECT 1 FROM DUAL",)
        assert second_call.args == ("INSERT INTO t VALUES (:a)", {"a": 1})
        mock_sync_conn.commit.assert_called_once()

    def test_error_raises(self, sync_oracle, mock_sync_cursor):
        mock_sync_cursor.execute.side_effect = _non_retriable_oracle_error()
        with pytest.raises(oracledb.OperationalError):
            sync_oracle.execute_multiple([("SELECT 1 FROM DUAL", None)])


class TestOracleConnectionExecuteMany:
    def test_happy_path(self, sync_oracle, mock_sync_conn, mock_sync_cursor):
        records = [{"a": 1}, {"a": 2}]
        sync_oracle.execute_many("INSERT INTO t VALUES (:a)", records)
        mock_sync_cursor.executemany.assert_called_once_with(
            "INSERT INTO t VALUES (:a)", records
        )
        mock_sync_conn.commit.assert_called_once()

    def test_error_raises(self, sync_oracle, mock_sync_cursor):
        mock_sync_cursor.executemany.side_effect = _non_retriable_oracle_error()
        with pytest.raises(oracledb.OperationalError):
            sync_oracle.execute_many("INSERT INTO t VALUES (:a)", [{"a": 1}])


class TestOracleConnectionFetchData:
    def test_fetch_with_packed_data(self, sync_oracle, mock_sync_cursor):
        rows = sync_oracle.fetch_data("SELECT * FROM t WHERE a=:a", {"a": 1})
        mock_sync_cursor.execute.assert_called_once_with(
            "SELECT * FROM t WHERE a=:a", {"a": 1}
        )
        assert rows == [(1, "a"), (2, "b")]

    def test_fetch_without_packed_data(self, sync_oracle, mock_sync_cursor):
        rows = sync_oracle.fetch_data("SELECT * FROM t")
        mock_sync_cursor.execute.assert_called_once_with("SELECT * FROM t")
        assert rows == [(1, "a"), (2, "b")]

    def test_fetch_error_raises(self, sync_oracle, mock_sync_cursor):
        mock_sync_cursor.execute.side_effect = _non_retriable_oracle_error()
        with pytest.raises(oracledb.OperationalError):
            sync_oracle.fetch_data("SELECT * FROM t")


# ---------------------------------------------------------------------------
# OracleConnection - composites
# ---------------------------------------------------------------------------


class TestOracleConnectionExportDf:
    def test_happy_path_calls_execute_many_with_expected_sql(self, sync_oracle):
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        with patch.object(sync_oracle, "execute_many") as mock_em:
            count = sync_oracle.export_df_to_warehouse(df, "schema.my_table", ["id", "name"])
        assert count == 2
        mock_em.assert_called_once()
        query, records = mock_em.call_args.args
        assert query == (
            'INSERT INTO "SCHEMA"."MY_TABLE" ("ID", "NAME") VALUES (:id, :name)'
        )
        assert records == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    def test_empty_df_returns_zero(self, sync_oracle):
        df = pd.DataFrame({"id": [], "name": []})
        with patch.object(sync_oracle, "execute_many") as mock_em:
            count = sync_oracle.export_df_to_warehouse(df, "t", ["id", "name"])
        assert count == 0
        mock_em.assert_not_called()

    def test_empty_columns_raises(self, sync_oracle):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="columns cannot be empty"):
            sync_oracle.export_df_to_warehouse(df, "t", [])

    def test_unknown_columns_raises(self, sync_oracle):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="subset of DataFrame columns"):
            sync_oracle.export_df_to_warehouse(df, "t", ["id", "missing"])

    def test_remove_nan_converts_na_to_none(self, sync_oracle):
        import numpy as np

        df = pd.DataFrame({"id": [1, 2], "name": ["a", np.nan]})
        with patch.object(sync_oracle, "execute_many") as mock_em:
            sync_oracle.export_df_to_warehouse(df, "t", ["id", "name"], remove_nan=True)
        _query, records = mock_em.call_args.args
        # second record's name should have been normalized to None
        assert records[1]["name"] is None

    def test_empty_string_normalized_to_none(self, sync_oracle):
        df = pd.DataFrame({"id": [1], "name": [""]})
        with patch.object(sync_oracle, "execute_many") as mock_em:
            sync_oracle.export_df_to_warehouse(df, "t", ["id", "name"])
        _query, records = mock_em.call_args.args
        assert records[0]["name"] is None


class TestOracleConnectionRemoveMatching:
    def test_happy_path_builds_delete(self, sync_oracle):
        df = pd.DataFrame({"id": [1, 2, 2], "name": ["a", "b", "b"]})
        with patch.object(sync_oracle, "execute_many") as mock_em:
            count = sync_oracle.remove_matching_data(df, "schema.t", ["id"])
        assert count == 2  # duplicates dropped
        query, records = mock_em.call_args.args
        assert query == 'DELETE FROM "SCHEMA"."T" WHERE id = :id'
        assert records == [{"id": 1}, {"id": 2}]

    def test_multi_column_match(self, sync_oracle):
        df = pd.DataFrame({"id": [1], "name": ["a"]})
        with patch.object(sync_oracle, "execute_many") as mock_em:
            sync_oracle.remove_matching_data(df, "t", ["id", "name"])
        query, _ = mock_em.call_args.args
        assert query == 'DELETE FROM "T" WHERE id = :id AND name = :name'

    def test_empty_match_cols_raises(self, sync_oracle):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="match_cols cannot be empty"):
            sync_oracle.remove_matching_data(df, "t", [])

    def test_unknown_match_cols_raises(self, sync_oracle):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="subset of DataFrame columns"):
            sync_oracle.remove_matching_data(df, "t", ["missing"])

    def test_empty_df_returns_zero(self, sync_oracle):
        df = pd.DataFrame({"id": []})
        with patch.object(sync_oracle, "execute_many") as mock_em:
            count = sync_oracle.remove_matching_data(df, "t", ["id"])
        assert count == 0
        mock_em.assert_not_called()


class TestOracleConnectionTruncateEmpty:
    def test_truncate_table_calls_execute(self, sync_oracle):
        with patch.object(sync_oracle, "execute") as mock_exec:
            sync_oracle.truncate_table("schema.t")
        mock_exec.assert_called_once_with('TRUNCATE TABLE "SCHEMA"."T"')

    def test_truncate_requires_name(self, sync_oracle):
        with pytest.raises(ValueError):
            sync_oracle.truncate_table("")

    def test_empty_table_calls_execute(self, sync_oracle):
        with patch.object(sync_oracle, "execute") as mock_exec:
            sync_oracle.empty_table("t")
        mock_exec.assert_called_once_with('DELETE FROM "T"')

    def test_empty_table_requires_name(self, sync_oracle):
        with pytest.raises(ValueError):
            sync_oracle.empty_table("")


# ---------------------------------------------------------------------------
# OracleConnection - retry behavior
# ---------------------------------------------------------------------------


class TestOracleConnectionRetryBehavior:
    def test_retriable_error_then_success_retries(self, sync_oracle, mock_sync_cursor):
        # First call raises retriable, second succeeds
        mock_sync_cursor.execute.side_effect = [_retriable_oracle_error(), None]
        with patch("wcp_library.sql.sleep") as mock_sleep:
            sync_oracle.execute("SELECT 1 FROM DUAL")
        assert mock_sync_cursor.execute.call_count == 2
        assert mock_sleep.called  # retry waited

    def test_retry_limit_reached_raises(self, sync_oracle, mock_sync_cursor):
        sync_oracle.retry_limit = 2
        mock_sync_cursor.execute.side_effect = _retriable_oracle_error()
        with patch("wcp_library.sql.sleep"):
            with pytest.raises(oracledb.OperationalError):
                sync_oracle.execute("SELECT 1 FROM DUAL")
        # Attempts: initial + retry_limit retries => retry_limit + 1
        assert mock_sync_cursor.execute.call_count == sync_oracle.retry_limit + 1

    def test_non_retriable_error_not_retried(self, sync_oracle, mock_sync_cursor):
        mock_sync_cursor.execute.side_effect = _non_retriable_oracle_error()
        with patch("wcp_library.sql.sleep") as mock_sleep:
            with pytest.raises(oracledb.OperationalError):
                sync_oracle.execute("SELECT 1 FROM DUAL")
        mock_sync_cursor.execute.assert_called_once()
        mock_sleep.assert_not_called()

    def test_string_error_arg_raised_immediately(self, sync_oracle, mock_sync_cursor):
        # When e.args is a string (not an error object), retry decorator re-raises
        mock_sync_cursor.execute.side_effect = oracledb.OperationalError("plain string")
        with pytest.raises(oracledb.OperationalError):
            sync_oracle.execute("SELECT 1 FROM DUAL")
        mock_sync_cursor.execute.assert_called_once()


# ---------------------------------------------------------------------------
# AsyncOracleConnection - construction & credentials
# ---------------------------------------------------------------------------


class TestAsyncOracleConnectionInit:
    def test_defaults(self):
        ao = AsyncOracleConnection()
        assert ao.use_pool is False
        assert ao.min_connections == 2
        assert ao.max_connections == 5
        assert ao._connection is None
        assert ao._session_pool is None
        assert ao.retry_limit == 50
        assert ao.retry_error_codes == oracle_retry_codes

    def test_custom_pool_options(self):
        ao = AsyncOracleConnection(use_pool=True, min_connections=4, max_connections=8)
        assert ao.use_pool is True
        assert ao.min_connections == 4
        assert ao.max_connections == 8


class TestAsyncOracleConnectionSetUser:
    @pytest.mark.asyncio
    async def test_requires_service_or_sid(self):
        ao = AsyncOracleConnection(use_pool=False)
        with pytest.raises(ValueError, match="Service or SID"):
            await ao.set_user(
                {"UserName": "u", "Password": "p", "Host": "h", "Port": 1521}
            )

    @pytest.mark.asyncio
    async def test_sets_fields_and_invokes_connect(self, async_conn_pair):
        ao = AsyncOracleConnection(use_pool=False)
        conn, _cursor = async_conn_pair
        with patch(
            "wcp_library.sql.oracle._async_connect_warehouse",
            new=AsyncMock(return_value=conn),
        ) as mocked_connect:
            await ao.set_user(
                {
                    "UserName": "user",
                    "Password": "pw",
                    "Host": "example.com",
                    "Port": "1521",
                    "Service": "SVC",
                }
            )
        assert ao._username == "user"
        assert ao._password == "pw"
        assert ao._hostname == "example.com"
        assert ao._port == 1521
        assert ao._database == "SVC"
        assert ao._connection is conn
        mocked_connect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_uses_sid_when_service_absent(self, async_conn_pair):
        ao = AsyncOracleConnection(use_pool=False)
        conn, _ = async_conn_pair
        with patch(
            "wcp_library.sql.oracle._async_connect_warehouse",
            new=AsyncMock(return_value=conn),
        ) as mocked_connect:
            await ao.set_user(
                {
                    "UserName": "user",
                    "Password": "pw",
                    "Host": "example.com",
                    "Port": 1521,
                    "SID": "MYSID",
                }
            )
        assert ao._sid == "MYSID"
        assert ao._database is None
        args, _kwargs = mocked_connect.call_args
        assert args[4] == "MYSID"


class TestAsyncOracleConnectionClose:
    @pytest.mark.asyncio
    async def test_close_non_pool(self, async_oracle, async_conn_pair):
        conn, _ = async_conn_pair
        await async_oracle.close_connection()
        conn.close.assert_awaited_once()
        assert async_oracle._connection is None

    @pytest.mark.asyncio
    async def test_close_unhealthy_no_close_called(self, async_conn_pair):
        conn, _ = async_conn_pair
        conn.is_healthy.return_value = False
        ao = AsyncOracleConnection(use_pool=False)
        ao._connection = conn
        await ao.close_connection()
        conn.close.assert_not_awaited()
        assert ao._connection is None

    @pytest.mark.asyncio
    async def test_close_pool(self):
        ao = AsyncOracleConnection(use_pool=True)
        pool = MagicMock()
        pool.close = AsyncMock()
        ao._session_pool = pool
        await ao.close_connection()
        pool.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_aexit_closes(self, async_conn_pair):
        conn, _ = async_conn_pair
        ao = AsyncOracleConnection(use_pool=False)
        ao._connection = conn
        async with ao as handle:
            assert handle is ao
        conn.close.assert_awaited_once()


# ---------------------------------------------------------------------------
# AsyncOracleConnection - primitives
# ---------------------------------------------------------------------------


class TestAsyncOracleConnectionExecute:
    @pytest.mark.asyncio
    async def test_execute_happy_path(self, async_oracle, async_conn_pair):
        conn, cursor = async_conn_pair
        await async_oracle.execute("SELECT 1 FROM DUAL")
        cursor.execute.assert_awaited_once_with("SELECT 1 FROM DUAL")
        conn.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_execute_non_retriable_error_raises(self, async_oracle, async_conn_pair):
        _conn, cursor = async_conn_pair
        cursor.execute.side_effect = _non_retriable_oracle_error()
        with pytest.raises(oracledb.OperationalError):
            await async_oracle.execute("SELECT 1 FROM DUAL")

    @pytest.mark.asyncio
    async def test_execute_pool_release(self, async_conn_pair):
        conn, cursor = async_conn_pair
        ao = AsyncOracleConnection(use_pool=True)
        pool = MagicMock()
        pool.acquire = AsyncMock(return_value=conn)
        pool.release = AsyncMock()
        ao._session_pool = pool
        await ao.execute("SELECT 1 FROM DUAL")
        cursor.execute.assert_awaited_once_with("SELECT 1 FROM DUAL")
        pool.release.assert_awaited_once_with(conn)


class TestAsyncOracleConnectionSafeExecute:
    @pytest.mark.asyncio
    async def test_happy_path(self, async_oracle, async_conn_pair):
        conn, cursor = async_conn_pair
        await async_oracle.safe_execute("INSERT INTO t VALUES (:a)", {"a": 1})
        cursor.execute.assert_awaited_once_with("INSERT INTO t VALUES (:a)", {"a": 1})
        conn.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_retriable_error_raises(self, async_oracle, async_conn_pair):
        _conn, cursor = async_conn_pair
        cursor.execute.side_effect = _non_retriable_oracle_error()
        with pytest.raises(oracledb.OperationalError):
            await async_oracle.safe_execute("INSERT INTO t VALUES (:a)", {"a": 1})


class TestAsyncOracleConnectionExecuteMultiple:
    @pytest.mark.asyncio
    async def test_runs_each_query(self, async_oracle, async_conn_pair):
        conn, cursor = async_conn_pair
        queries = [
            ("SELECT 1 FROM DUAL", None),
            ("INSERT INTO t VALUES (:a)", {"a": 1}),
        ]
        await async_oracle.execute_multiple(queries)
        assert cursor.execute.await_count == 2
        first_call, second_call = cursor.execute.await_args_list
        assert first_call.args == ("SELECT 1 FROM DUAL",)
        assert second_call.args == ("INSERT INTO t VALUES (:a)", {"a": 1})
        conn.commit.assert_awaited_once()


class TestAsyncOracleConnectionExecuteMany:
    @pytest.mark.asyncio
    async def test_happy_path(self, async_oracle, async_conn_pair):
        conn, cursor = async_conn_pair
        records = [{"a": 1}, {"a": 2}]
        await async_oracle.execute_many("INSERT INTO t VALUES (:a)", records)
        cursor.executemany.assert_awaited_once_with(
            "INSERT INTO t VALUES (:a)", records
        )
        conn.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_raises(self, async_oracle, async_conn_pair):
        _conn, cursor = async_conn_pair
        cursor.executemany.side_effect = _non_retriable_oracle_error()
        with pytest.raises(oracledb.OperationalError):
            await async_oracle.execute_many("INSERT INTO t VALUES (:a)", [{"a": 1}])


class TestAsyncOracleConnectionFetchData:
    @pytest.mark.asyncio
    async def test_fetch_with_packed_data(self, async_oracle, async_conn_pair):
        _conn, cursor = async_conn_pair
        rows = await async_oracle.fetch_data("SELECT * FROM t WHERE a=:a", {"a": 1})
        cursor.execute.assert_awaited_once_with(
            "SELECT * FROM t WHERE a=:a", {"a": 1}
        )
        assert rows == [(1, "a"), (2, "b")]

    @pytest.mark.asyncio
    async def test_fetch_without_packed_data(self, async_oracle, async_conn_pair):
        _conn, cursor = async_conn_pair
        rows = await async_oracle.fetch_data("SELECT * FROM t")
        cursor.execute.assert_awaited_once_with("SELECT * FROM t")
        assert rows == [(1, "a"), (2, "b")]


# ---------------------------------------------------------------------------
# AsyncOracleConnection - composites
# ---------------------------------------------------------------------------


class TestAsyncOracleConnectionExportDf:
    @pytest.mark.asyncio
    async def test_happy_path(self, async_oracle):
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        with patch.object(async_oracle, "execute_many", new=AsyncMock()) as mock_em:
            count = await async_oracle.export_df_to_warehouse(
                df, "schema.my_table", ["id", "name"]
            )
        assert count == 2
        mock_em.assert_awaited_once()
        query, records = mock_em.await_args.args
        assert query == (
            'INSERT INTO "SCHEMA"."MY_TABLE" ("ID", "NAME") VALUES (:id, :name)'
        )
        assert records == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    @pytest.mark.asyncio
    async def test_empty_df_returns_zero(self, async_oracle):
        df = pd.DataFrame({"id": [], "name": []})
        with patch.object(async_oracle, "execute_many", new=AsyncMock()) as mock_em:
            count = await async_oracle.export_df_to_warehouse(df, "t", ["id", "name"])
        assert count == 0
        mock_em.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_columns_raises(self, async_oracle):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="columns cannot be empty"):
            await async_oracle.export_df_to_warehouse(df, "t", [])

    @pytest.mark.asyncio
    async def test_unknown_columns_raises(self, async_oracle):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="subset of DataFrame columns"):
            await async_oracle.export_df_to_warehouse(df, "t", ["id", "missing"])

    @pytest.mark.asyncio
    async def test_remove_nan_converts_na_to_none(self, async_oracle):
        import numpy as np

        df = pd.DataFrame({"id": [1, 2], "name": ["a", np.nan]})
        with patch.object(async_oracle, "execute_many", new=AsyncMock()) as mock_em:
            await async_oracle.export_df_to_warehouse(
                df, "t", ["id", "name"], remove_nan=True
            )
        _query, records = mock_em.await_args.args
        assert records[1]["name"] is None


class TestAsyncOracleConnectionRemoveMatching:
    @pytest.mark.asyncio
    async def test_happy_path(self, async_oracle):
        df = pd.DataFrame({"id": [1, 2, 2], "name": ["a", "b", "b"]})
        with patch.object(async_oracle, "execute_many", new=AsyncMock()) as mock_em:
            count = await async_oracle.remove_matching_data(df, "schema.t", ["id"])
        assert count == 2
        query, records = mock_em.await_args.args
        assert query == 'DELETE FROM "SCHEMA"."T" WHERE id = :id'
        assert records == [{"id": 1}, {"id": 2}]

    @pytest.mark.asyncio
    async def test_empty_match_cols_raises(self, async_oracle):
        df = pd.DataFrame({"id": [1]})
        with pytest.raises(ValueError, match="match_cols cannot be empty"):
            await async_oracle.remove_matching_data(df, "t", [])

    @pytest.mark.asyncio
    async def test_empty_df_returns_zero(self, async_oracle):
        df = pd.DataFrame({"id": []})
        with patch.object(async_oracle, "execute_many", new=AsyncMock()) as mock_em:
            count = await async_oracle.remove_matching_data(df, "t", ["id"])
        assert count == 0
        mock_em.assert_not_awaited()


class TestAsyncOracleConnectionTruncateEmpty:
    @pytest.mark.asyncio
    async def test_truncate_calls_execute(self, async_oracle):
        with patch.object(async_oracle, "execute", new=AsyncMock()) as mock_exec:
            await async_oracle.truncate_table("schema.t")
        mock_exec.assert_awaited_once_with('TRUNCATE TABLE "SCHEMA"."T"')

    @pytest.mark.asyncio
    async def test_truncate_requires_name(self, async_oracle):
        with pytest.raises(ValueError):
            await async_oracle.truncate_table("")

    @pytest.mark.asyncio
    async def test_empty_table_calls_execute(self, async_oracle):
        with patch.object(async_oracle, "execute", new=AsyncMock()) as mock_exec:
            await async_oracle.empty_table("t")
        mock_exec.assert_awaited_once_with('DELETE FROM "T"')

    @pytest.mark.asyncio
    async def test_empty_requires_name(self, async_oracle):
        with pytest.raises(ValueError):
            await async_oracle.empty_table("")


# ---------------------------------------------------------------------------
# AsyncOracleConnection - retry behavior
# ---------------------------------------------------------------------------


class TestAsyncOracleConnectionRetryBehavior:
    @pytest.mark.asyncio
    async def test_retriable_error_then_success_retries(self, async_oracle, async_conn_pair):
        _conn, cursor = async_conn_pair
        cursor.execute.side_effect = [_retriable_oracle_error(), None]
        with patch("wcp_library.sql.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            await async_oracle.execute("SELECT 1 FROM DUAL")
        assert cursor.execute.await_count == 2
        mock_sleep.assert_awaited()

    @pytest.mark.asyncio
    async def test_retry_limit_reached_raises(self, async_oracle, async_conn_pair):
        _conn, cursor = async_conn_pair
        async_oracle.retry_limit = 2
        cursor.execute.side_effect = _retriable_oracle_error()
        with patch("wcp_library.sql.asyncio.sleep", new=AsyncMock()):
            with pytest.raises(oracledb.OperationalError):
                await async_oracle.execute("SELECT 1 FROM DUAL")
        assert cursor.execute.await_count == async_oracle.retry_limit + 1

    @pytest.mark.asyncio
    async def test_non_retriable_error_not_retried(self, async_oracle, async_conn_pair):
        _conn, cursor = async_conn_pair
        cursor.execute.side_effect = _non_retriable_oracle_error()
        with patch("wcp_library.sql.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            with pytest.raises(oracledb.OperationalError):
                await async_oracle.execute("SELECT 1 FROM DUAL")
        cursor.execute.assert_awaited_once()
        mock_sleep.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_string_error_arg_raised_immediately(self, async_oracle, async_conn_pair):
        _conn, cursor = async_conn_pair
        cursor.execute.side_effect = oracledb.OperationalError("plain string")
        with pytest.raises(oracledb.OperationalError):
            await async_oracle.execute("SELECT 1 FROM DUAL")
        cursor.execute.assert_awaited_once()
