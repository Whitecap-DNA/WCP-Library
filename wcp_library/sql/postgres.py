import asyncio
import logging
import time
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Awaitable, Callable, TypeVar

import numpy as np
import pandas as pd
import psycopg
from psycopg import AsyncConnection, Connection
from psycopg.conninfo import make_conninfo
from psycopg.sql import Composed, Identifier, Placeholder, SQL
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from wcp_library.sql import (
    _RETRY_SLEEP_SECONDS,
    _log_giveup,
    _log_retry,
    async_retry,
    classify_db_exception,
    retry,
)

logger = logging.getLogger(__name__)
postgres_retry_codes = ['08001', '08004', '40P01']

_T = TypeVar("_T")


# ---------------------------------------------------------------------------
# Query-building helpers (shared by sync and async composites)
# ---------------------------------------------------------------------------


def _table_identifier(table_name: str) -> Identifier:
    """Build a psycopg ``Identifier`` from a possibly schema-qualified name."""
    return Identifier(*table_name.split("."))


def _prepare_df_records(
    df: pd.DataFrame, columns: list[str], remove_nan: bool
) -> list[tuple]:
    """Project ``df`` onto ``columns``, normalize NaN/NaT/empty-string to None,
    and return row tuples suitable for ``execute_many``."""
    df_copy = df[columns].copy()
    if remove_nan:
        df_copy = df_copy.replace({np.nan: None, pd.NaT: None})
    df_copy = df_copy.replace({"": None})
    return list(df_copy.itertuples(index=False, name=None))


def _build_insert_for_df(
    df: pd.DataFrame, table_name: str, columns: list[str], remove_nan: bool
) -> tuple[Composed, list[tuple]]:
    """Build the INSERT query + records for ``export_df_to_warehouse``.

    Caller is responsible for upstream validation (non-empty columns,
    subset-of-df-columns, non-empty df).
    """
    col_ids = SQL(", ").join(Identifier(c) for c in columns)
    placeholders = SQL(", ").join(Placeholder() for _ in columns)
    query = SQL("INSERT INTO {} ({}) VALUES ({})").format(
        _table_identifier(table_name), col_ids, placeholders
    )
    return query, _prepare_df_records(df, columns, remove_nan)


def _build_upsert_for_df(
    df: pd.DataFrame,
    table_name: str,
    columns: list[str],
    match_cols: list[str],
    remove_nan: bool,
) -> tuple[Composed, list[tuple]]:
    """Build the INSERT...ON CONFLICT query + records for
    ``upsert_df_to_warehouse``."""
    update_cols = [c for c in columns if c not in match_cols]
    col_ids = SQL(", ").join(Identifier(c) for c in columns)
    match_ids = SQL(", ").join(Identifier(c) for c in match_cols)
    placeholders = SQL(", ").join(Placeholder() for _ in columns)
    if update_cols:
        updates = SQL(", ").join(
            SQL("{} = EXCLUDED.{}").format(Identifier(c), Identifier(c))
            for c in update_cols
        )
        conflict_action = SQL("DO UPDATE SET {}").format(updates)
    else:
        conflict_action = SQL("DO NOTHING")
    query = SQL(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {}"
    ).format(_table_identifier(table_name), col_ids, placeholders, match_ids, conflict_action)
    return query, _prepare_df_records(df, columns, remove_nan)


def _build_delete_matching_for_df(
    df: pd.DataFrame, table_name: str, match_cols: list[str]
) -> tuple[Composed, list[dict]]:
    """Build the DELETE query + dict-records for ``remove_matching_data``."""
    df_subset = df[match_cols].drop_duplicates(keep='first')
    conditions = SQL(" AND ").join(
        SQL("{} = {}").format(Identifier(c), Placeholder(c)) for c in match_cols
    )
    query = SQL("DELETE FROM {} WHERE {}").format(
        _table_identifier(table_name), conditions
    )
    return query, df_subset.to_dict('records')


# ---------------------------------------------------------------------------


def _connect_warehouse(username: str, password: str, hostname: str, port: int, database: str, min_connections: int,
                       max_connections: int, use_pool: bool) -> Connection | ConnectionPool:
    """
    Create Warehouse Connection

    :param username: username
    :param password: password
    :param hostname: hostname
    :param port: port
    :param database: database
    :param min_connections:
    :param max_connections:
    :return: session_pool
    """

    keepalive_kwargs = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 5,
        "keepalives_count": 5,
    }

    conn_string = f"dbname={database} user={username} password={password} host={hostname} port={port}"
    conninfo = make_conninfo(conn_string)

    if use_pool:
        logger.debug(f"Creating connection pool with min size {min_connections} and max size {max_connections}")
        session_pool = ConnectionPool(
            conninfo=conninfo,
            min_size=min_connections,
            max_size=max_connections,
            kwargs={'options': '-c datestyle=ISO,YMD'} | keepalive_kwargs,
            open=True
        )
        return session_pool
    else:
        logger.debug("Creating single connection")
        connection = psycopg.connect(conninfo=conninfo, options='-c datestyle=ISO,YMD', **keepalive_kwargs)
        return connection


async def _async_connect_warehouse(username: str, password: str, hostname: str, port: int, database: str, min_connections: int,
                                   max_connections: int, use_pool: bool) -> AsyncConnection | AsyncConnectionPool:
    """
    Create Warehouse Connection

    :param username: username
    :param password: password
    :param hostname: hostname
    :param port: port
    :param database: database
    :param min_connections:
    :param max_connections:
    :return: session_pool
    """

    keepalive_kwargs = {
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 5,
        "keepalives_count": 5,
    }

    conn_string = f"dbname={database} user={username} password={password} host={hostname} port={port}"
    conninfo = make_conninfo(conn_string)

    if use_pool:
        logger.debug(f"Creating async connection pool with min size {min_connections} and max size {max_connections}")
        session_pool = AsyncConnectionPool(
            conninfo=conninfo,
            min_size=min_connections,
            max_size=max_connections,
            kwargs={"options": "-c datestyle=ISO,YMD"} | keepalive_kwargs,
            open=False
        )
        return session_pool
    else:
        logger.debug("Creating single async connection")
        connection = await AsyncConnection.connect(conninfo=conninfo, options='-c datestyle=ISO,YMD', **keepalive_kwargs)
        return connection


"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""


class SyncExecutor(ABC):
    """Sync mirror of :class:`AsyncExecutor`. Same surface, synchronous signatures.

    Concrete subclasses implement the five primitives (execute,
    safe_execute, execute_many, execute_multiple, fetch_data). The
    composite data-manipulation methods live here and call
    self.<primitive> -- dispatch is correct on both PostgresConnection
    (per-call pool checkout) and Transaction (single held connection)
    because both implement the same primitive surface.
    """

    @abstractmethod
    def execute(self, query: SQL | Composed | str) -> int: ...

    @abstractmethod
    def safe_execute(self, query: SQL | Composed | str, packed_values: dict) -> int: ...

    @abstractmethod
    def execute_many(
        self, query: SQL | Composed | str, dictionary: list[dict] | list[tuple]
    ) -> int: ...

    @abstractmethod
    def execute_multiple(
        self, queries: list[tuple[SQL | Composed | str, dict]]
    ) -> int: ...

    @abstractmethod
    def fetch_data(
        self, query: SQL | Composed | str, packed_data: dict | None = None
    ) -> list[tuple]: ...

    def export_df_to_warehouse(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: list[str],
        remove_nan: bool = False,
    ) -> int:
        """Insert every row of ``df[columns]`` into ``table_name``.

        :param df: source DataFrame
        :param table_name: destination table (may be schema-qualified)
        :param columns: columns to insert; must be a subset of ``df.columns``
        :param remove_nan: convert NaN/NaT values to NULL before insert
        :return: number of records inserted (``0`` if ``df`` is empty)
        """
        columns = list(columns)
        if not columns:
            raise ValueError("columns cannot be empty")
        if not set(columns).issubset(set(df.columns)):
            raise ValueError("columns must be a subset of DataFrame columns")
        if df.empty:
            return 0
        query, records = _build_insert_for_df(df, table_name, columns, remove_nan)
        self.execute_many(query, records)
        return len(records)

    def upsert_df_to_warehouse(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: list[str],
        match_cols: list[str],
        remove_nan: bool = False,
    ) -> int:
        """Upsert every row of ``df[columns]`` into ``table_name`` using
        ``ON CONFLICT (match_cols)``.

        :param df: source DataFrame
        :param table_name: destination table (may be schema-qualified)
        :param columns: columns to write (must be a superset of ``match_cols``)
        :param match_cols: conflict columns (must have a matching index/PK
            on the destination)
        :param remove_nan: convert NaN/NaT values to NULL before upsert
        :return: number of records sent to the server (``0`` if ``df`` is empty)
        """
        columns = list(columns)
        match_cols = list(match_cols)
        if not columns:
            raise ValueError("columns cannot be empty")
        if not match_cols:
            raise ValueError("match_cols cannot be empty")
        if not set(match_cols).issubset(set(columns)):
            raise ValueError("match_cols must be a subset of columns")
        if df.empty:
            return 0
        query, records = _build_upsert_for_df(df, table_name, columns, match_cols, remove_nan)
        self.execute_many(query, records)
        return len(records)

    def truncate_table(self, table_name: str) -> None:
        """Truncate ``table_name`` (schema-qualified names supported).

        :raises ValueError: if ``table_name`` is empty
        """
        if not table_name:
            raise ValueError("table_name cannot be empty")
        query = SQL("TRUNCATE TABLE {}").format(_table_identifier(table_name))
        self.execute(query)

    def empty_table(self, table_name: str) -> None:
        """Delete every row from ``table_name`` (retains the table).

        :raises ValueError: if ``table_name`` is empty
        """
        if not table_name:
            raise ValueError("table_name cannot be empty")
        query = SQL("DELETE FROM {}").format(_table_identifier(table_name))
        self.execute(query)


class Transaction(SyncExecutor):
    """Handle yielded by ``PostgresConnection.transaction()``.

    Sync mirror of :class:`AsyncTransaction`. All primitives run on
    the single held psycopg3 Connection and do NOT commit per call.
    Committed on normal context-manager exit, rolled back on
    exception (by psycopg3's native transaction context). Manual
    :meth:`commit` / :meth:`rollback` are available for early
    termination; after either is called, the outer context manager's
    commit/rollback becomes a no-op because no transaction remains
    open.

    Composite methods (``upsert_df_to_warehouse``, ``truncate_table``,
    etc.) are inherited from :class:`SyncExecutor` and dispatch to the
    primitives on this class -- so they run transactionally too.

    No retry decoration on the primitives here: retry belongs at the
    transaction boundary via :meth:`PostgresConnection.retry_transaction`.
    """

    def __init__(self, parent: "PostgresConnection", connection: Connection) -> None:
        self._parent = parent
        self._connection = connection
        self._completed_manually = False

    @property
    def connection(self) -> Connection:
        """The underlying psycopg3 ``Connection`` (escape hatch)."""
        return self._connection

    def execute(self, query: SQL | Composed | str) -> int:
        cursor = self._connection.execute(query)
        return max(cursor.rowcount, 0)

    def safe_execute(
        self, query: SQL | Composed | str, packed_values: dict
    ) -> int:
        cursor = self._connection.execute(query, packed_values)
        return max(cursor.rowcount, 0)

    def execute_many(
        self, query: SQL | Composed | str, dictionary: list[dict] | list[tuple]
    ) -> int:
        self._connection.prepare_threshold = None
        cursor = self._connection.cursor()
        cursor.executemany(query, dictionary, returning=False)
        return max(cursor.rowcount, 0)

    def execute_multiple(
        self, queries: list[tuple[SQL | Composed | str, dict]]
    ) -> int:
        total = 0
        for item in queries:
            query = item[0]
            packed_values = item[1] if len(item) > 1 else None
            if packed_values:
                cursor = self._connection.execute(query, packed_values)
            else:
                cursor = self._connection.execute(query)
            total += max(cursor.rowcount, 0)
        return total

    def fetch_data(
        self, query: SQL | Composed | str, packed_data: dict | None = None
    ) -> list[tuple]:
        cursor = self._connection.cursor()
        if packed_data:
            cursor.execute(query, packed_data)
        else:
            cursor.execute(query)
        return cursor.fetchall()

    def commit(self) -> None:
        """Commit the transaction early.

        After this call, the outer ``with conn.transaction():`` block's
        commit-on-exit becomes a no-op (no active transaction).
        """
        self._connection.commit()
        self._completed_manually = True

    def rollback(self) -> None:
        """Rollback the transaction early.

        After this call, the outer ``with conn.transaction():`` block's
        rollback-on-exception becomes a no-op (no active transaction).
        """
        self._connection.rollback()
        self._completed_manually = True


class PostgresConnection(SyncExecutor):
    """Synchronous Postgres connection manager.

    Construct, then call :meth:`set_user` with a credentials dict to
    establish the connection (or pool). All five primitives and every
    inherited composite honor the retry policy defined in
    :mod:`wcp_library.sql`.

    **Autocommit mode (default)** — each primitive commits after the
    statement and, when pooled, returns the connection on the same call.

    **Caller-driven transactions** — either:

    * construct with ``autocommit=False`` for the whole instance and
      drive commits manually via :meth:`commit` / :meth:`rollback`, *or*
    * use :meth:`transaction` (recommended) to open a transactional
      block scoped to one held connection.

    .. warning::
        When ``autocommit=False``, statements accumulate in an open
        transaction on ``self._connection``. If the instance is
        garbage-collected (or the process exits) without a prior
        :meth:`commit`, psycopg3 rolls back on close — *uncommitted
        statements are silently dropped*. Always pair
        ``autocommit=False`` with an explicit commit path, or prefer
        :meth:`transaction` which handles this automatically.

    Use as a context manager (``with PostgresConnection() as conn:``)
    to guarantee :meth:`close_connection` runs.

    :param use_pool: back the instance with a pool instead of a single
        connection
    :param min_connections: pool minimum size (when ``use_pool=True``)
    :param max_connections: pool maximum size (when ``use_pool=True``)
    :param autocommit: per-primitive autocommit (default ``True``)
    :raises ValueError: if ``use_pool=True`` is combined with
        ``autocommit=False`` (unsupported combination)
    """

    def __init__(
        self,
        use_pool: bool = False,
        min_connections: int = 2,
        max_connections: int = 5,
        autocommit: bool = True,
    ):
        if use_pool and not autocommit:
            raise ValueError(
                "use_pool=True with autocommit=False is unsupported — "
                "pool-per-call checkout combined with caller-driven commit "
                "leaves held connections dangling across statement "
                "boundaries. Use conn.transaction() for transactional "
                "work on a pooled connection."
            )
        self._autocommit = autocommit

        self._username: str | None = None
        self._password: str | None = None
        self._hostname: str | None = None
        self._port: int | None = None
        self._database: str | None = None
        self._connection: Connection | None = None
        self._session_pool: ConnectionPool | None = None

        self.use_pool = use_pool
        self.min_connections = min_connections
        self.max_connections = max_connections

        self._retry_count = 0
        self.retry_limit = 50
        self.retry_error_codes = postgres_retry_codes

    @retry
    def _connect(self) -> None:
        """
        Connect to the warehouse

        :return: None
        """
        connection = _connect_warehouse(self._username, self._password, self._hostname, self._port,
                                        self._database, self.min_connections, self.max_connections, self.use_pool)

        if self.use_pool:
            self._session_pool = connection
            self._session_pool.open()
        else:
            self._connection = connection

    def _get_connection(self) -> Connection:
        """
        Get the connection object

        :return: connection
        """

        if self.use_pool:
            connection = self._session_pool.getconn()
            return connection
        else:
            if self._connection is None or self._connection.closed:
                self._connect()
            return self._connection

    def set_user(self, credentials_dict: dict) -> None:
        """Store credentials and open the connection (or pool).

        :param credentials_dict: must contain keys ``UserName``, ``Password``,
            ``Host``, ``Port``, ``Database``
        """
        self._username = credentials_dict['UserName']
        self._password = credentials_dict['Password']
        self._hostname = credentials_dict['Host']
        self._port = int(credentials_dict['Port'])
        self._database = credentials_dict['Database']

        self._connect()

    def close_connection(self) -> None:
        """Close the pool (if pooled) or the single connection."""
        if self.use_pool:
            self._session_pool.close()
        else:
            if self._connection is not None and not self._connection.closed:
                self._connection.close()
            self._connection = None

    @retry
    def execute(self, query: SQL | Composed | str) -> int:
        """Execute a single statement.

        :param query: query (``SQL``, ``Composed``, or raw string)
        :return: rows affected (``0`` for DDL or statements where the
            driver does not report a rowcount)
        """
        connection = self._get_connection()
        try:
            cursor = connection.execute(query)
            rowcount = max(cursor.rowcount, 0)
            if self._autocommit:
                connection.commit()
            return rowcount
        finally:
            if self.use_pool:
                self._session_pool.putconn(connection)

    @retry
    def safe_execute(
        self, query: SQL | Composed | str, packed_values: dict
    ) -> int:
        """Execute a parameterized statement (safe against SQL injection).

        :param query: query (``SQL``, ``Composed``, or raw string with
            ``%s`` / ``%(name)s`` placeholders)
        :param packed_values: values for the placeholders
        :return: rows affected
        """
        connection = self._get_connection()
        try:
            cursor = connection.execute(query, packed_values)
            rowcount = max(cursor.rowcount, 0)
            if self._autocommit:
                connection.commit()
            return rowcount
        finally:
            if self.use_pool:
                self._session_pool.putconn(connection)

    @retry
    def execute_multiple(
        self, queries: list[tuple[SQL | Composed | str, dict]]
    ) -> int:
        """Execute a sequence of ``(query, packed_values)`` pairs in order.

        Each tuple may omit ``packed_values`` (i.e. a one-element tuple is
        treated as "no params").

        :param queries: list of ``(query, packed_values_or_missing)`` tuples
        :return: sum of rows affected across all statements
        """
        connection = self._get_connection()
        try:
            total = 0
            for item in queries:
                query = item[0]
                packed_values = item[1] if len(item) > 1 else None
                if packed_values:
                    cursor = connection.execute(query, packed_values)
                else:
                    cursor = connection.execute(query)
                total += max(cursor.rowcount, 0)
            if self._autocommit:
                connection.commit()
            return total
        finally:
            if self.use_pool:
                self._session_pool.putconn(connection)

    @retry
    def execute_many(
        self, query: SQL | Composed | str, dictionary: list[dict] | list[tuple]
    ) -> int:
        """Execute the same query once per record in ``dictionary``.

        :param query: query with placeholders
        :param dictionary: iterable of parameter dicts or tuples
        :return: rows affected
        """
        connection = self._get_connection()
        try:
            connection.prepare_threshold = None
            cursor = connection.cursor()
            cursor.executemany(query, dictionary, returning=False)
            rowcount = max(cursor.rowcount, 0)
            if self._autocommit:
                connection.commit()
            return rowcount
        finally:
            if self.use_pool:
                self._session_pool.putconn(connection)

    @retry
    def fetch_data(
        self, query: SQL | Composed | str, packed_data: dict | None = None
    ) -> list[tuple]:
        """Execute ``query`` and return every row.

        :param query: SELECT query
        :param packed_data: optional parameter dict
        :return: list of row tuples
        """
        connection = self._get_connection()
        try:
            cursor = connection.cursor()
            if packed_data:
                cursor.execute(query, packed_data)
            else:
                cursor.execute(query)
            rows = cursor.fetchall()
            if self._autocommit:
                connection.commit()
            return rows
        finally:
            if self.use_pool:
                self._session_pool.putconn(connection)

    def commit(self) -> None:
        """Commit the current transaction.

        Only meaningful when the instance was constructed with
        ``autocommit=False``. No-op if no connection has been opened yet.
        """
        if self._connection is None:
            return
        self._connection.commit()

    def rollback(self) -> None:
        """Rollback the current transaction.

        Only meaningful when the instance was constructed with
        ``autocommit=False``. No-op if no connection has been opened yet.
        """
        if self._connection is None:
            return
        self._connection.rollback()

    @contextmanager
    def transaction(self):
        """Enter a transactional context on a single held physical connection.

        Usage:

            with conn.transaction() as tx:
                tx.execute("CREATE TABLE ...")
                tx.upsert_df_to_warehouse(df, "target", cols, match_cols)
            # Commit on normal exit, rollback on exception.

        Autocommit is toggled to False for the block and restored on
        exit. If ``use_pool=True``, the connection is returned to the
        pool at exit. psycopg3's native ``connection.transaction()``
        handles BEGIN/COMMIT/ROLLBACK (nested calls produce SAVEPOINTs).

        :yield: a :class:`Transaction` bound to the held connection.
        """
        connection = self._get_connection()
        try:
            prior_autocommit = connection.autocommit
            try:
                connection.autocommit = False
                tx = Transaction(self, connection)
                with connection.transaction():
                    yield tx
                    # If tx.commit() / tx.rollback() was called, the native
                    # transaction context manager sees no active transaction
                    # and its own commit/rollback becomes a no-op.
            finally:
                connection.autocommit = prior_autocommit
        finally:
            # Outermost finally guarantees the pool gets its connection back
            # even if reading/restoring autocommit raises.
            if self.use_pool:
                self._session_pool.putconn(connection)

    def retry_transaction(
        self,
        fn: Callable[..., _T],
        *args: Any,
        **kwargs: Any,
    ) -> _T:
        """Run ``fn(tx, *args, **kwargs)`` inside a transaction with retry at
        the transaction boundary.

        Sync mirror of :meth:`AsyncPostgresConnection.retry_transaction`.
        On retriable Postgres errors (``full_code`` in
        ``self.retry_error_codes``), the transaction is rolled back,
        the thread sleeps 5 minutes per the standard library policy,
        and a fresh transaction is entered to retry ``fn``.
        Non-retriable errors propagate immediately.

        This is the transaction-scoped equivalent of the ``@retry``
        decorator: same exception filtering, same backoff, same
        attempt limit, just applied at the granularity of the whole
        ``with self.transaction() as tx: fn(tx, *args, **kwargs)`` block.

        :param fn: callable; receives a :class:`Transaction` as its first
            argument, followed by any extra positional/keyword args
        :param args: forwarded to ``fn`` after the transaction handle
        :param kwargs: forwarded to ``fn``
        :return: the return value of the successful ``fn`` invocation
        :raises: the final exception if all attempts fail, or any non-
            retriable error immediately
        """
        self._retry_count = 0
        while True:
            try:
                with self.transaction() as tx:
                    return fn(tx, *args, **kwargs)
            except (psycopg.OperationalError, psycopg.DatabaseError) as e:
                action, full_code, message = classify_db_exception(e, self.retry_error_codes)
                if action == "raise":
                    raise
                if action == "non_retriable" or self._retry_count >= self.retry_limit:
                    _log_giveup(full_code, self.retry_limit, action)
                    raise
                self._retry_count += 1
                _log_retry(self._retry_count, self.retry_limit, full_code, message, "transaction")
                time.sleep(_RETRY_SLEEP_SECONDS)

    @retry
    def remove_matching_data(
        self, df: pd.DataFrame, table_name: str, match_cols: list[str]
    ) -> int:
        """Delete rows from ``table_name`` that match any row in ``df[match_cols]``.

        :param df: source DataFrame whose rows identify targets for deletion
        :param table_name: destination table (may be schema-qualified)
        :param match_cols: columns used to match rows; must be a subset of
            ``df.columns``
        :return: number of distinct match-tuples sent to the server
            (``0`` if ``df`` is empty)
        """
        match_cols = list(match_cols)
        if not match_cols:
            raise ValueError("match_cols cannot be empty")
        if not set(match_cols).issubset(set(df.columns)):
            raise ValueError("match_cols must be a subset of DataFrame columns")
        if df.empty:
            return 0
        query, records = _build_delete_matching_for_df(df, table_name, match_cols)
        self.execute_many(query, records)
        return len(records)

    def __enter__(self) -> "PostgresConnection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Close the connection/pool on exit and propagate any exception."""
        self.close_connection()
        return False


"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""


class AsyncExecutor(ABC):
    """Abstract executor for async Postgres operations.

    Concrete subclasses implement the five primitives (execute,
    safe_execute, execute_many, execute_multiple, fetch_data). The
    composite data-manipulation methods (export_df_to_warehouse,
    upsert_df_to_warehouse, truncate_table, empty_table) live here
    and call self.<primitive> -- dispatch is correct on both
    AsyncPostgresConnection (per-call pool checkout) and
    AsyncTransaction (single held connection) because both implement
    the same primitive surface.
    """

    @abstractmethod
    async def execute(self, query: SQL | Composed | str) -> int: ...

    @abstractmethod
    async def safe_execute(self, query: SQL | Composed | str, packed_values: dict) -> int: ...

    @abstractmethod
    async def execute_many(
        self, query: SQL | Composed | str, dictionary: list[dict] | list[tuple]
    ) -> int: ...

    @abstractmethod
    async def execute_multiple(
        self, queries: list[tuple[SQL | Composed | str, dict]]
    ) -> int: ...

    @abstractmethod
    async def fetch_data(
        self, query: SQL | Composed | str, packed_data: dict | None = None
    ) -> list[tuple]: ...

    async def export_df_to_warehouse(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: list[str],
        remove_nan: bool = False,
    ) -> int:
        """Insert every row of ``df[columns]`` into ``table_name``.

        :param df: source DataFrame
        :param table_name: destination table (may be schema-qualified)
        :param columns: columns to insert; must be a subset of ``df.columns``
        :param remove_nan: convert NaN/NaT values to NULL before insert
        :return: number of records inserted (``0`` if ``df`` is empty)
        """
        columns = list(columns)
        if not columns:
            raise ValueError("columns cannot be empty")
        if not set(columns).issubset(set(df.columns)):
            raise ValueError("columns must be a subset of DataFrame columns")
        if df.empty:
            return 0
        query, records = _build_insert_for_df(df, table_name, columns, remove_nan)
        await self.execute_many(query, records)
        return len(records)

    async def upsert_df_to_warehouse(
        self,
        df: pd.DataFrame,
        table_name: str,
        columns: list[str],
        match_cols: list[str],
        remove_nan: bool = False,
    ) -> int:
        """Upsert every row of ``df[columns]`` into ``table_name`` using
        ``ON CONFLICT (match_cols)``.

        :param df: source DataFrame
        :param table_name: destination table (may be schema-qualified)
        :param columns: columns to write (must be a superset of ``match_cols``)
        :param match_cols: conflict columns (must have a matching index/PK
            on the destination)
        :param remove_nan: convert NaN/NaT values to NULL before upsert
        :return: number of records sent to the server (``0`` if ``df`` is empty)
        """
        columns = list(columns)
        match_cols = list(match_cols)
        if not columns:
            raise ValueError("columns cannot be empty")
        if not match_cols:
            raise ValueError("match_cols cannot be empty")
        if not set(match_cols).issubset(set(columns)):
            raise ValueError("match_cols must be a subset of columns")
        if df.empty:
            return 0
        query, records = _build_upsert_for_df(df, table_name, columns, match_cols, remove_nan)
        await self.execute_many(query, records)
        return len(records)

    async def truncate_table(self, table_name: str) -> None:
        """Truncate ``table_name`` (schema-qualified names supported).

        :raises ValueError: if ``table_name`` is empty
        """
        if not table_name:
            raise ValueError("table_name cannot be empty")
        query = SQL("TRUNCATE TABLE {}").format(_table_identifier(table_name))
        await self.execute(query)

    async def empty_table(self, table_name: str) -> None:
        """Delete every row from ``table_name`` (retains the table).

        :raises ValueError: if ``table_name`` is empty
        """
        if not table_name:
            raise ValueError("table_name cannot be empty")
        query = SQL("DELETE FROM {}").format(_table_identifier(table_name))
        await self.execute(query)


class AsyncTransaction(AsyncExecutor):
    """Handle yielded by ``AsyncPostgresConnection.transaction()``.

    All primitives run on the single held psycopg3 AsyncConnection and
    do NOT commit per call. The transaction is committed on normal
    context-manager exit and rolled back on exception (by psycopg3's
    native transaction context). Manual :meth:`commit` / :meth:`rollback`
    are available for early termination; after either is called, the
    outer context manager's commit/rollback becomes a no-op because no
    transaction remains open.

    Composite methods (``upsert_df_to_warehouse``, ``truncate_table``,
    etc.) are inherited from :class:`AsyncExecutor` and dispatch to the
    primitives on this class -- so they run transactionally too.

    No retry decoration on the primitives here: retry belongs at the
    transaction boundary via :meth:`AsyncPostgresConnection.retry_transaction`.
    """

    def __init__(self, parent: "AsyncPostgresConnection", connection: AsyncConnection) -> None:
        self._parent = parent
        self._connection = connection
        self._completed_manually = False

    @property
    def connection(self) -> AsyncConnection:
        """The underlying psycopg3 ``AsyncConnection`` (escape hatch)."""
        return self._connection

    async def execute(self, query: SQL | Composed | str) -> int:
        cursor = await self._connection.execute(query)
        return max(cursor.rowcount, 0)

    async def safe_execute(
        self, query: SQL | Composed | str, packed_values: dict
    ) -> int:
        cursor = await self._connection.execute(query, packed_values)
        return max(cursor.rowcount, 0)

    async def execute_many(
        self, query: SQL | Composed | str, dictionary: list[dict] | list[tuple]
    ) -> int:
        self._connection.prepare_threshold = None
        cursor = self._connection.cursor()
        await cursor.executemany(query, dictionary, returning=False)
        return max(cursor.rowcount, 0)

    async def execute_multiple(
        self, queries: list[tuple[SQL | Composed | str, dict]]
    ) -> int:
        total = 0
        for item in queries:
            query = item[0]
            packed_values = item[1] if len(item) > 1 else None
            if packed_values:
                cursor = await self._connection.execute(query, packed_values)
            else:
                cursor = await self._connection.execute(query)
            total += max(cursor.rowcount, 0)
        return total

    async def fetch_data(
        self, query: SQL | Composed | str, packed_data: dict | None = None
    ) -> list[tuple]:
        cursor = self._connection.cursor()
        if packed_data:
            await cursor.execute(query, packed_data)
        else:
            await cursor.execute(query)
        return await cursor.fetchall()

    async def commit(self) -> None:
        """Commit the transaction early.

        After this call, the outer ``async with conn.transaction():``
        block's commit-on-exit becomes a no-op (no active transaction).
        """
        await self._connection.commit()
        self._completed_manually = True

    async def rollback(self) -> None:
        """Rollback the transaction early.

        After this call, the outer ``async with conn.transaction():``
        block's rollback-on-exception becomes a no-op (no active
        transaction).
        """
        await self._connection.rollback()
        self._completed_manually = True


class AsyncPostgresConnection(AsyncExecutor):
    """Asynchronous Postgres connection manager.

    Construct, then ``await set_user(credentials)`` to establish the
    connection (or pool). All five primitives and every inherited
    composite honor the async retry policy defined in
    :mod:`wcp_library.sql`.

    **Autocommit mode (default)** — each primitive commits after the
    statement and, when pooled, returns the connection on the same call.

    **Caller-driven transactions** — either:

    * construct with ``autocommit=False`` for the whole instance and
      drive commits manually via :meth:`commit` / :meth:`rollback`, *or*
    * use :meth:`transaction` (recommended) to open a transactional
      block scoped to one held connection.

    .. warning::
        When ``autocommit=False``, statements accumulate in an open
        transaction on ``self._connection``. If the instance is
        garbage-collected (or the process exits) without a prior
        :meth:`commit`, psycopg3 rolls back on close — *uncommitted
        statements are silently dropped*. Always pair
        ``autocommit=False`` with an explicit commit path, or prefer
        :meth:`transaction` which handles this automatically.

    Use as an async context manager
    (``async with AsyncPostgresConnection() as conn:``) to guarantee
    :meth:`close_connection` runs.

    :param use_pool: back the instance with a pool instead of a single
        connection
    :param min_connections: pool minimum size (when ``use_pool=True``)
    :param max_connections: pool maximum size (when ``use_pool=True``)
    :param autocommit: per-primitive autocommit (default ``True``)
    :raises ValueError: if ``use_pool=True`` is combined with
        ``autocommit=False`` (unsupported combination)
    """

    def __init__(
        self,
        use_pool: bool = False,
        min_connections: int = 2,
        max_connections: int = 5,
        autocommit: bool = True,
    ):
        if use_pool and not autocommit:
            raise ValueError(
                "use_pool=True with autocommit=False is unsupported — "
                "pool-per-call checkout combined with caller-driven commit "
                "leaves held connections dangling across statement "
                "boundaries. Use conn.transaction() for transactional "
                "work on a pooled connection."
            )
        self._autocommit = autocommit

        self._username: str | None = None
        self._password: str | None = None
        self._hostname: str | None = None
        self._port: int | None = None
        self._database: str | None = None
        self._connection: AsyncConnection | None = None
        self._session_pool: AsyncConnectionPool | None = None

        self.use_pool = use_pool
        self.min_connections = min_connections
        self.max_connections = max_connections

        self._retry_count = 0
        self.retry_limit = 50
        self.retry_error_codes = postgres_retry_codes

    @async_retry
    async def _connect(self) -> None:
        """
        Connect to the warehouse

        :return: None
        """

        connection = await _async_connect_warehouse(self._username, self._password, self._hostname, self._port,
                                                    self._database, self.min_connections, self.max_connections,
                                                    self.use_pool)
        if self.use_pool:
            self._session_pool = connection
            await self._session_pool.open()
        else:
            self._connection = connection

    async def _get_connection(self) -> AsyncConnection:
        """
        Get the connection object

        :return: connection
        """

        if self.use_pool:
            connection = await self._session_pool.getconn()
            return connection
        else:
            if self._connection is None or self._connection.closed:
                await self._connect()
            return self._connection


    async def set_user(self, credentials_dict: dict) -> None:
        """Store credentials and open the connection (or pool).

        :param credentials_dict: must contain keys ``UserName``, ``Password``,
            ``Host``, ``Port``, ``Database``
        """
        self._username = credentials_dict['UserName']
        self._password = credentials_dict['Password']
        self._hostname = credentials_dict['Host']
        self._port = int(credentials_dict['Port'])
        self._database = credentials_dict['Database']

        await self._connect()

    async def close_connection(self) -> None:
        """Close the pool (if pooled) or the single connection."""
        if self.use_pool:
            await self._session_pool.close()
        else:
            if self._connection is not None and not self._connection.closed:
                await self._connection.close()
            self._connection = None

    @async_retry
    async def execute(self, query: SQL | Composed | str) -> int:
        """Execute a single statement.

        :param query: query (``SQL``, ``Composed``, or raw string)
        :return: rows affected (``0`` for DDL or statements where the
            driver does not report a rowcount)
        """
        connection = await self._get_connection()
        try:
            cursor = await connection.execute(query)
            rowcount = max(cursor.rowcount, 0)
            if self._autocommit:
                await connection.commit()
            return rowcount
        finally:
            if self.use_pool:
                await self._session_pool.putconn(connection)

    @async_retry
    async def safe_execute(
        self, query: SQL | Composed | str, packed_values: dict
    ) -> int:
        """Execute a parameterized statement (safe against SQL injection).

        :param query: query (``SQL``, ``Composed``, or raw string with
            ``%s`` / ``%(name)s`` placeholders)
        :param packed_values: values for the placeholders
        :return: rows affected
        """
        connection = await self._get_connection()
        try:
            cursor = await connection.execute(query, packed_values)
            rowcount = max(cursor.rowcount, 0)
            if self._autocommit:
                await connection.commit()
            return rowcount
        finally:
            if self.use_pool:
                await self._session_pool.putconn(connection)

    @async_retry
    async def execute_multiple(
        self, queries: list[tuple[SQL | Composed | str, dict]]
    ) -> int:
        """Execute a sequence of ``(query, packed_values)`` pairs in order.

        Each tuple may omit ``packed_values`` (i.e. a one-element tuple is
        treated as "no params").

        :param queries: list of ``(query, packed_values_or_missing)`` tuples
        :return: sum of rows affected across all statements
        """
        connection = await self._get_connection()
        try:
            total = 0
            for item in queries:
                query = item[0]
                packed_values = item[1] if len(item) > 1 else None
                if packed_values:
                    cursor = await connection.execute(query, packed_values)
                else:
                    cursor = await connection.execute(query)
                total += max(cursor.rowcount, 0)
            if self._autocommit:
                await connection.commit()
            return total
        finally:
            if self.use_pool:
                await self._session_pool.putconn(connection)

    @async_retry
    async def execute_many(
        self, query: SQL | Composed | str, dictionary: list[dict] | list[tuple]
    ) -> int:
        """Execute the same query once per record in ``dictionary``.

        :param query: query with placeholders
        :param dictionary: iterable of parameter dicts or tuples
        :return: rows affected
        """
        connection = await self._get_connection()
        try:
            connection.prepare_threshold = None
            cursor = connection.cursor()
            await cursor.executemany(query, dictionary, returning=False)
            rowcount = max(cursor.rowcount, 0)
            if self._autocommit:
                await connection.commit()
            return rowcount
        finally:
            if self.use_pool:
                await self._session_pool.putconn(connection)

    @async_retry
    async def fetch_data(
        self, query: SQL | Composed | str, packed_data: dict | None = None
    ) -> list[tuple]:
        """Execute ``query`` and return every row.

        :param query: SELECT query
        :param packed_data: optional parameter dict
        :return: list of row tuples
        """
        connection = await self._get_connection()
        try:
            cursor = connection.cursor()
            if packed_data:
                await cursor.execute(query, packed_data)
            else:
                await cursor.execute(query)
            rows = await cursor.fetchall()
            if self._autocommit:
                await connection.commit()
            return rows
        finally:
            if self.use_pool:
                await self._session_pool.putconn(connection)

    async def commit(self) -> None:
        """Commit the current transaction.

        Only meaningful when the instance was constructed with
        ``autocommit=False``. No-op if no connection has been opened yet.
        """
        if self._connection is None:
            return
        await self._connection.commit()

    async def rollback(self) -> None:
        """Rollback the current transaction.

        Only meaningful when the instance was constructed with
        ``autocommit=False``. No-op if no connection has been opened yet.
        """
        if self._connection is None:
            return
        await self._connection.rollback()

    @asynccontextmanager
    async def transaction(self):
        """Enter a transactional context on a single held physical connection.

        Usage:

            async with conn.transaction() as tx:
                await tx.execute("SET LOCAL ROLE some_role")
                await tx.execute("CREATE TABLE foo (...)")
                await tx.upsert_df_to_warehouse(df, "foo", cols, match_cols)
            # Commit on normal exit, rollback on exception.

        Autocommit is toggled to False for the block and restored on exit.
        If ``use_pool=True``, the connection is returned to the pool at
        exit. psycopg3's native ``connection.transaction()`` handles
        BEGIN/COMMIT/ROLLBACK (nested calls produce SAVEPOINTs).

        :yield: an :class:`AsyncTransaction` bound to the held connection.
        """
        connection = await self._get_connection()
        try:
            prior_autocommit = connection.autocommit
            try:
                await connection.set_autocommit(False)
                tx = AsyncTransaction(self, connection)
                async with connection.transaction():
                    yield tx
                    # If tx.commit() / tx.rollback() was called, the native
                    # transaction context manager sees no active transaction
                    # and its own commit/rollback becomes a no-op.
            finally:
                await connection.set_autocommit(prior_autocommit)
        finally:
            # Outermost finally guarantees the pool gets its connection back
            # even if reading/restoring autocommit raises.
            if self.use_pool:
                await self._session_pool.putconn(connection)

    async def retry_transaction(
        self,
        fn: Callable[..., Awaitable[_T]],
        *args: Any,
        **kwargs: Any,
    ) -> _T:
        """Run ``await fn(tx, *args, **kwargs)`` inside a transaction with
        retry at the transaction boundary.

        On retriable Postgres errors (``full_code`` in ``self.retry_error_codes``),
        the transaction is rolled back, the coroutine waits 5 minutes per the
        standard library policy, and a fresh transaction is entered to retry
        ``fn``. Non-retriable errors propagate immediately.

        This is the transaction-scoped equivalent of the ``@async_retry``
        decorator: same exception filtering, same backoff, same attempt
        limit, just applied at the granularity of the whole ``async with
        self.transaction() as tx: await fn(tx, *args, **kwargs)`` block.

        :param fn: async callable; receives an :class:`AsyncTransaction` as
            its first argument, followed by any extra positional/keyword args
        :param args: forwarded to ``fn`` after the transaction handle
        :param kwargs: forwarded to ``fn``
        :return: the return value of the successful ``fn`` invocation
        :raises: the final exception if all attempts fail, or any non-
            retriable error immediately
        """
        self._retry_count = 0
        while True:
            try:
                async with self.transaction() as tx:
                    return await fn(tx, *args, **kwargs)
            except (psycopg.OperationalError, psycopg.DatabaseError) as e:
                action, full_code, message = classify_db_exception(e, self.retry_error_codes)
                if action == "raise":
                    raise
                if action == "non_retriable" or self._retry_count >= self.retry_limit:
                    _log_giveup(full_code, self.retry_limit, action)
                    raise
                self._retry_count += 1
                _log_retry(self._retry_count, self.retry_limit, full_code, message, "transaction")
                await asyncio.sleep(_RETRY_SLEEP_SECONDS)

    @async_retry
    async def remove_matching_data(
        self, df: pd.DataFrame, table_name: str, match_cols: list[str]
    ) -> int:
        """Delete rows from ``table_name`` that match any row in ``df[match_cols]``.

        :param df: source DataFrame whose rows identify targets for deletion
        :param table_name: destination table (may be schema-qualified)
        :param match_cols: columns used to match rows; must be a subset of
            ``df.columns``
        :return: number of distinct match-tuples sent to the server
            (``0`` if ``df`` is empty)
        """
        match_cols = list(match_cols)
        if not match_cols:
            raise ValueError("match_cols cannot be empty")
        if not set(match_cols).issubset(set(df.columns)):
            raise ValueError("match_cols must be a subset of DataFrame columns")
        if df.empty:
            return 0
        query, records = _build_delete_matching_for_df(df, table_name, match_cols)
        await self.execute_many(query, records)
        return len(records)

    async def __aenter__(self) -> "AsyncPostgresConnection":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Close the connection/pool on exit and propagate any exception."""
        await self.close_connection()
        return False

