import logging

import numpy as np
import pandas as pd
import psycopg
from psycopg import AsyncConnection, Connection
from psycopg.conninfo import make_conninfo
from psycopg.sql import Composed, Identifier, Placeholder, SQL
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from wcp_library.sql import retry, async_retry

logger = logging.getLogger(__name__)
postgres_retry_codes = ['08001', '08004', '40P01']


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


class PostgresConnection(object):
    """
    SQL Connection Class

    :return: None
    """

    def __init__(self, use_pool: bool = False, min_connections: int = 2, max_connections: int = 5):
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


        self.connection = _connect_warehouse(self._username, self._password, self._hostname, self._port,
                                             self._database, self.min_connections, self.max_connections, self.use_pool)

        if self.use_pool:
            self._session_pool = self.connection
            self._session_pool.open()
        else:
            self._connection = self.connection

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
        """
        Set the user credentials and connect

        :param credentials_dict: dictionary of connection details
        :return: None
        """

        self._username = credentials_dict['UserName']
        self._password = credentials_dict['Password']
        self._hostname = credentials_dict['Host']
        self._port = int(credentials_dict['Port'])
        self._database = credentials_dict['Database']

        self._connect()

    def close_connection(self) -> None:
        """
        Close the connection

        :return: None
        """

        if self.use_pool:
            self._session_pool.close()
        else:
            if self._connection is not None and not self._connection.closed:
                self._connection.close()
            self._connection = None

    @retry
    def execute(self, query: SQL | Composed | str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        connection = self._get_connection()
        connection.execute(query)
        connection.commit()

        if self.use_pool:
            self._session_pool.putconn(connection)

    @retry
    def safe_execute(self, query: SQL | Composed | str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        connection = self._get_connection()
        connection.execute(query, packed_values)
        connection.commit()

        if self.use_pool:
            self._session_pool.putconn(connection)

    @retry
    def execute_multiple(self, queries: list[tuple[SQL | Composed | str, dict]]) -> None:
        """
        Execute multiple queries

        :param queries: list of queries
        :return: None
        """

        connection = self._get_connection()
        for item in queries:
            query = item[0]
            packed_values = item[1]
            if packed_values:
                connection.execute(query, packed_values)
            else:
                connection.execute(query)
        connection.commit()

        if self.use_pool:
            self._session_pool.putconn(connection)

    @retry
    def execute_many(self, query: SQL | Composed | str, dictionary: list[dict] | list[tuple]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        connection = self._get_connection()
        connection.prepare_threshold = None
        cursor = connection.cursor()
        cursor.executemany(query, dictionary, returning=False)
        connection.commit()

        if self.use_pool:
            self._session_pool.putconn(connection)

    @retry
    def fetch_data(self, query: SQL | Composed | str, packed_data=None) -> list[tuple]:
        """
        Fetch the data from the query

        :param query: query
        :param packed_data: packed data
        :return: rows
        """

        connection = self._get_connection()
        cursor = connection.cursor()
        if packed_data:
            cursor.execute(query, packed_data)
        else:
            cursor.execute(query)
        rows = cursor.fetchall()
        connection.commit()

        if self.use_pool:
            self._session_pool.putconn(connection)
        return rows

    @retry
    def remove_matching_data(self, df: pd.DataFrame, table_name: str, match_cols: list) -> int:
        """
        Remove matching data from the warehouse

        :param df: DataFrame
        :param table_name: output table name
        :param match_cols: list of columns to match on
        :return: Number of records matched for deletion
        """

        match_cols = list(match_cols) if not isinstance(match_cols, list) else match_cols

        if not match_cols:
            raise ValueError("match_cols cannot be empty")
        if not set(match_cols).issubset(set(df.columns)):
            raise ValueError("match_cols must be a subset of DataFrame columns")
        if df.empty:
            return 0

        df_subset = df[match_cols].drop_duplicates(keep='first')
        param_list = []
        for column in match_cols:
            param_list.append(f"{column} = %({column})s")
        params = ' AND '.join(param_list) if len(param_list) > 1 else param_list[0]

        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)
        query = SQL("DELETE FROM {} WHERE {}").format(table_id, SQL(params))

        main_dict = df_subset.to_dict('records')
        self.execute_many(query, main_dict)
        return len(main_dict)

    @retry
    def export_df_to_warehouse(self, df: pd.DataFrame, table_name: str, columns: list, remove_nan: bool = False) -> int:
        """
        Export the DataFrame to the warehouse

        :param df: DataFrame
        :param table_name: output table name
        :param columns: list of columns to insert
        :param remove_nan: remove NaN values
        :return: Number of records inserted
        """

        columns = list(columns) if not isinstance(columns, list) else columns

        if not columns:
            raise ValueError("columns cannot be empty")
        if not set(columns).issubset(set(df.columns)):
            raise ValueError("columns must be a subset of DataFrame columns")
        if df.empty:
            return 0

        col_ids = SQL(", ").join(Identifier(c) for c in columns)
        placeholders = SQL(", ").join(Placeholder() for _ in columns)

        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)

        df_copy = df[columns].copy()
        if remove_nan:
            df_copy = df_copy.replace({np.nan: None, pd.NaT: None})
        df_copy = df_copy.replace({"": None})

        records = list(df_copy.itertuples(index=False, name=None))
        query = SQL("INSERT INTO {} ({}) VALUES ({})").format(table_id, col_ids, placeholders)
        self.execute_many(query, records)
        return len(records)

    @retry
    def upsert_df_to_warehouse(self, df: pd.DataFrame, table_name: str, columns: list, match_cols: list, remove_nan: bool = False) -> int:
        """
        Upsert the DataFrame to the warehouse

        :param df: DataFrame
        :param table_name: output table name
        :param columns: list of columns
        :param match_cols: list of columns to match on
        :param remove_nan: remove NaN values
        :return: Number of records upserted
        """

        columns = list(columns) if not isinstance(columns, list) else columns
        match_cols = list(match_cols) if not isinstance(match_cols, list) else match_cols

        if not columns:
            raise ValueError("columns cannot be empty")
        if not match_cols:
            raise ValueError("match_cols cannot be empty")
        if not set(match_cols).issubset(set(columns)):
            raise ValueError("match_cols must be a subset of columns")
        if df.empty:
            return 0

        update_cols = [c for c in columns if c not in match_cols]

        col_ids = SQL(", ").join(Identifier(c) for c in columns)
        match_ids = SQL(", ").join(Identifier(c) for c in match_cols)
        placeholders = SQL(", ").join(Placeholder() for _ in columns)

        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)

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
        ).format(table_id, col_ids, placeholders, match_ids, conflict_action)

        df_copy = df[columns].copy()
        if remove_nan:
            df_copy = df_copy.replace({np.nan: None, pd.NaT: None})
        df_copy = df_copy.replace({"": None})

        records = list(df_copy.itertuples(index=False, name=None))
        self.execute_many(query, records)
        return len(records)

    @retry
    def truncate_table(self, table_name: str) -> None:
        """
        Truncate the table

        :param table_name: table name
        :return: None
        """

        if not table_name:
            raise ValueError("table_name cannot be empty")

        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)
        truncate_query = SQL("TRUNCATE TABLE {}").format(table_id)
        self.execute(truncate_query)

    @retry
    def empty_table(self, table_name: str) -> None:
        """
        Empty the table

        :param table_name: table name
        :return: None
        """

        if not table_name:
            raise ValueError("table_name cannot be empty")

        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)
        delete_query = SQL("DELETE FROM {}").format(table_id)
        self.execute(delete_query)

    def __enter__(self):
        """
        Context manager entry

        :return: self
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit

        :return: None
        """
        self.close_connection()
        return False

    def __del__(self) -> None:
        """
        Destructor

        :return: None
        """

        if self._session_pool is not None:
            self._session_pool.close()
        else:
            if self._connection is not None and not self._connection.closed:
                self._connection.close()
            self._connection = None


class AsyncPostgresConnection(object):
    """
    SQL Connection Class

    :return: None
    """

    def __init__(self, use_pool: bool = False, min_connections: int = 2, max_connections: int = 5):
        self._username: str | None = None
        self._password: str | None = None
        self._hostname: str | None = None
        self._port: str | None = None
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
        """
        Set the user credentials and connect

        :param credentials_dict: dictionary of connection details
        :return: None
        """

        self._username = credentials_dict['UserName']
        self._password = credentials_dict['Password']
        self._hostname = credentials_dict['Host']
        self._port = int(credentials_dict['Port'])
        self._database = credentials_dict['Database']

        await self._connect()

    async def close_connection(self) -> None:
        """
        Close the connection

        :return: None
        """

        if self.use_pool:
            await self._session_pool.close()
        else:
            if self._connection is not None and not self._connection.closed:
                await self._connection.close()
            self._connection = None

    @async_retry
    async def execute(self, query: SQL | Composed | str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        connection = await self._get_connection()
        await connection.execute(query)
        await connection.commit()

        if self.use_pool:
            await self._session_pool.putconn(connection)

    @async_retry
    async def safe_execute(self, query: SQL | Composed | str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        connection = await self._get_connection()
        await connection.execute(query, packed_values)
        await connection.commit()

        if self.use_pool:
            await self._session_pool.putconn(connection)

    @async_retry
    async def execute_multiple(self, queries: list[tuple[SQL | Composed | str, dict]]) -> None:
        """
        Execute multiple queries

        :param queries: list of queries
        :return: None
        """

        connection = await self._get_connection()
        for item in queries:
            query = item[0]
            packed_values = item[1]
            if packed_values:
                await connection.execute(query, packed_values)
            else:
                await connection.execute(query)
        await connection.commit()

        if self.use_pool:
            await self._session_pool.putconn(connection)

    @async_retry
    async def execute_many(self, query: SQL | Composed | str, dictionary: list[dict] | list[tuple]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        connection = await self._get_connection()
        connection.prepare_threshold = None
        cursor = connection.cursor()
        await cursor.executemany(query, dictionary, returning=False)
        await connection.commit()

        if self.use_pool:
            await self._session_pool.putconn(connection)

    @async_retry
    async def fetch_data(self, query: SQL | Composed | str, packed_data=None) -> list[tuple]:
        """
        Fetch the data from the query

        :param query: query
        :param packed_data: packed data
        :return: rows
        """

        connection = await self._get_connection()
        cursor = connection.cursor()
        if packed_data:
            await cursor.execute(query, packed_data)
        else:
            await cursor.execute(query)
        rows = await cursor.fetchall()
        await connection.commit()

        if self.use_pool:
            await self._session_pool.putconn(connection)
        return rows

    @async_retry
    async def remove_matching_data(self, df: pd.DataFrame, table_name: str, match_cols: list) -> int:
        """
        Remove matching data from the warehouse

        :param df: DataFrame
        :param table_name: output table name
        :param match_cols: list of columns to match on
        :return: Number of records matched for deletion
        """

        match_cols = list(match_cols) if not isinstance(match_cols, list) else match_cols

        if not match_cols:
            raise ValueError("match_cols cannot be empty")
        if not set(match_cols).issubset(set(df.columns)):
            raise ValueError("match_cols must be a subset of DataFrame columns")
        if df.empty:
            return 0

        df_subset = df[match_cols].drop_duplicates(keep='first')
        param_list = []
        for column in match_cols:
            param_list.append(f"{column} = %({column})s")
        params = ' AND '.join(param_list) if len(param_list) > 1 else param_list[0]

        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)
        query = SQL("DELETE FROM {} WHERE {}").format(table_id, SQL(params))

        main_dict = df_subset.to_dict('records')
        await self.execute_many(query, main_dict)
        return len(main_dict)

    @async_retry
    async def export_df_to_warehouse(self, df: pd.DataFrame, table_name: str, columns: list, remove_nan: bool = False) -> int:
        """
        Export the DataFrame to the warehouse

        :param df: DataFrame
        :param table_name: output table name
        :param columns: list of columns to insert
        :param remove_nan: remove NaN values
        :return: Number of records inserted
        """

        columns = list(columns) if not isinstance(columns, list) else columns

        if not columns:
            raise ValueError("columns cannot be empty")
        if not set(columns).issubset(set(df.columns)):
            raise ValueError("columns must be a subset of DataFrame columns")
        if df.empty:
            return 0

        col_ids = SQL(", ").join(Identifier(c) for c in columns)
        placeholders = SQL(", ").join(Placeholder() for _ in columns)

        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)

        df_copy = df[columns].copy()
        if remove_nan:
            df_copy = df_copy.replace({np.nan: None, pd.NaT: None})
        df_copy = df_copy.replace({"": None})

        records = list(df_copy.itertuples(index=False, name=None))
        query = SQL("INSERT INTO {} ({}) VALUES ({})").format(table_id, col_ids, placeholders)
        await self.execute_many(query, records)
        return len(records)

    @async_retry
    async def upsert_df_to_warehouse(self, df: pd.DataFrame, table_name: str, columns: list, match_cols: list, remove_nan: bool = False) -> int:
        """
        Upsert the DataFrame to the warehouse

        :param df: DataFrame
        :param table_name: output table name
        :param columns: list of columns
        :param match_cols: list of columns to match on
        :param remove_nan: remove NaN values
        :return: Number of records upserted
        """

        columns = list(columns) if not isinstance(columns, list) else columns
        match_cols = list(match_cols) if not isinstance(match_cols, list) else match_cols

        if not columns:
            raise ValueError("columns cannot be empty")
        if not match_cols:
            raise ValueError("match_cols cannot be empty")
        if not set(match_cols).issubset(set(columns)):
            raise ValueError("match_cols must be a subset of columns")
        if df.empty:
            return 0

        update_cols = [c for c in columns if c not in match_cols]

        col_ids = SQL(", ").join(Identifier(c) for c in columns)
        match_ids = SQL(", ").join(Identifier(c) for c in match_cols)
        placeholders = SQL(", ").join(Placeholder() for _ in columns)

        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)

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
        ).format(table_id, col_ids, placeholders, match_ids, conflict_action)

        df_copy = df[columns].copy()
        if remove_nan:
            df_copy = df_copy.replace({np.nan: None, pd.NaT: None})
        df_copy = df_copy.replace({"": None})

        records = list(df_copy.itertuples(index=False, name=None))
        await self.execute_many(query, records)
        return len(records)

    @async_retry
    async def truncate_table(self, table_name: str) -> None:
        """
        Truncate the table

        :param table_name: table name
        :return: None
        """

        if not table_name:
            raise ValueError("table_name cannot be empty")

        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)
        truncate_query = SQL("TRUNCATE TABLE {}").format(table_id)
        await self.execute(truncate_query)

    @async_retry
    async def empty_table(self, table_name: str) -> None:
        """
        Empty the table

        :param table_name: table name
        :return: None
        """

        if not table_name:
            raise ValueError("table_name cannot be empty")

        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)
        delete_query = SQL("DELETE FROM {}").format(table_id)
        await self.execute(delete_query)

    async def __aenter__(self):
        """
        Async context manager entry

        :return: self
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Async context manager exit

        :return: None
        """
        await self.close_connection()
        return False
