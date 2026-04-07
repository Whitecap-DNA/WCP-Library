import logging
import re

import numpy as np
import pandas as pd
import oracledb
from oracledb import ConnectionPool, AsyncConnectionPool, Connection, AsyncConnection

from wcp_library.sql import retry, async_retry

logger = logging.getLogger(__name__)
oracledb.defaults.fetch_lobs = False
oracle_retry_codes = ['ORA-01033', 'DPY-6005', 'DPY-4011', 'ORA-08103', 'ORA-04021', 'ORA-01652', 'ORA-08103']

# Pattern for validating Oracle identifiers (prevents SQL injection)
VALID_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z][A-Za-z0-9_#$]*(\.[A-Za-z][A-Za-z0-9_#$]*)?$')


def _quote_identifier(identifier: str) -> str:
    """
    Quote and validate Oracle identifier to prevent SQL injection.

    Oracle identifiers can be schema.table or just table.
    This validates the identifier and returns it quoted.

    :param identifier: Table or column name
    :return: Quoted identifier
    :raises ValueError: If identifier is invalid
    """

    if not identifier:
        raise ValueError("Identifier cannot be empty")

    if not VALID_IDENTIFIER_PATTERN.match(identifier):
        raise ValueError(f"Invalid Oracle identifier: {identifier}")

    # Quote each part separately if schema.table format
    parts = identifier.split('.')
    quoted_parts = [f'"{part.upper()}"' for part in parts]
    return '.'.join(quoted_parts)


def _connect_warehouse(username: str, password: str, hostname: str, port: int, database: str, min_connections: int,
                       max_connections: int, use_pool: bool) -> ConnectionPool | Connection:
    """
    Create Warehouse Connection

    :param username: username
    :param password: password
    :param hostname: hostname
    :param port: port
    :param database: database
    :param min_connections:
    :param max_connections:
    :param use_pool: use connection pool
    :return: session_pool | connection
    """

    if use_pool:
        logger.debug(f"Creating connection pool with min size {min_connections} and max size {max_connections}")
        dsn = oracledb.makedsn(hostname, port, sid=database)
        session_pool = oracledb.create_pool(
            user=username,
            password=password,
            dsn=dsn,
            min=min_connections,
            max=max_connections,
            increment=1,
        )
        return session_pool
    else:
        logger.debug("Creating single connection")
        connection = oracledb.connect(
            user=username,
            password=password,
            dsn=oracledb.makedsn(hostname, port, service_name=database)
        )
        return connection


async def _async_connect_warehouse(username: str, password: str, hostname: str, port: int, database: str,
                                   min_connections: int, max_connections: int, use_pool: bool) -> AsyncConnectionPool | AsyncConnection:
    """
    Create Warehouse Connection

    :param username: username
    :param password: password
    :param hostname: hostname
    :param port: port
    :param database: database
    :param min_connections:
    :param max_connections:
    :param use_pool: use connection pool
    :return: session_pool | connection
    """

    if use_pool:
        logger.debug(f"Creating async connection pool with min size {min_connections} and max size {max_connections}")
        dsn = oracledb.makedsn(hostname, port, sid=database)
        session_pool = oracledb.create_pool_async(
            user=username,
            password=password,
            dsn=dsn,
            min=min_connections,
            max=max_connections,
            increment=1
        )
        return session_pool
    else:
        logger.debug("Creating single async connection")
        connection = await oracledb.connect_async(
            user=username,
            password=password,
            dsn=oracledb.makedsn(hostname, port, service_name=database)
        )
        return connection


"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""


class OracleConnection(object):
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
        self._sid: str | None = None
        self._connection: Connection | None = None
        self._session_pool: ConnectionPool | None = None

        self.use_pool = use_pool
        self.min_connections = min_connections
        self.max_connections = max_connections

        self._retry_count = 0
        self.retry_limit = 50
        self.retry_error_codes = oracle_retry_codes

    @retry
    def _connect(self) -> None:
        """
        Connect to the warehouse

        :return: None
        """

        sid_or_service = self._database if self._database else self._sid

        connection = _connect_warehouse(self._username, self._password, self._hostname, self._port,
                                        sid_or_service, self.min_connections, self.max_connections, self.use_pool)

        if self.use_pool:
            self._session_pool = connection
        else:
            self._connection = connection

    def _get_connection(self) -> Connection:
        """
        Get the connection, either from the pool or create a new one

        :return: Connection
        """

        if self.use_pool:
            return self._session_pool.acquire()
        else:
            if not self._connection or not self._connection.is_healthy():
                self._connect()
            return self._connection

    def set_user(self, credentials_dict: dict) -> None:
        """
        Set the user credentials and connect

        :param credentials_dict: dictionary of connection details
        :return: None
        """

        if not (credentials_dict.get('Service') or credentials_dict.get('SID')):
            raise ValueError("Either Service or SID must be provided")

        self._username = credentials_dict['UserName']
        self._password = credentials_dict['Password']
        self._hostname = credentials_dict['Host']
        self._port = int(credentials_dict['Port'])
        self._database = credentials_dict.get('Service')
        self._sid = credentials_dict.get('SID')

        self._connect()

    def close_connection(self) -> None:
        """
        Close the connection

        :return: None
        """

        if self.use_pool:
            self._session_pool.close()
        else:
            if self._connection and self._connection.is_healthy():
                self._connection.close()
            self._connection = None

    @retry
    def execute(self, query: str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        connection = self._get_connection()
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()

        if self.use_pool:
            self._session_pool.release(connection)

    @retry
    def safe_execute(self, query: str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        connection = self._get_connection()
        cursor = connection.cursor()
        cursor.execute(query, packed_values)
        connection.commit()

        if self.use_pool:
            self._session_pool.release(connection)

    @retry
    def execute_multiple(self, queries: list[tuple[str, dict]]) -> None:
        """
        Execute multiple queries

        :param queries: list of queries
        :return: None
        """

        connection = self._get_connection()
        cursor = connection.cursor()
        for item in queries:
            query = item[0]
            packed_values = item[1]
            if packed_values:
                cursor.execute(query, packed_values)
            else:
                cursor.execute(query)
        connection.commit()

        if self.use_pool:
            self._session_pool.release(connection)

    @retry
    def execute_many(self, query: str, dictionary: list[dict]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        connection = self._get_connection()
        cursor = connection.cursor()
        cursor.executemany(query, dictionary)
        connection.commit()

        if self.use_pool:
            self._session_pool.release(connection)

    @retry
    def fetch_data(self, query: str, packed_data=None) -> list:
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
            self._session_pool.release(connection)
        return rows

    @retry
    def remove_matching_data(self, df: pd.DataFrame, table_name: str, match_cols: list) -> int:
        """
        Remove matching data from the warehouse

        :param df: DataFrame
        :param table_name: table name
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
        param_list = [f"{column} = :{column}" for column in match_cols]
        params = ' AND '.join(param_list)

        quoted_table = _quote_identifier(table_name)
        query = f"DELETE FROM {quoted_table} WHERE {params}"

        main_dict = df_subset.to_dict('records')
        self.execute_many(query, main_dict)
        return len(main_dict)

    @retry
    def export_df_to_warehouse(self, df: pd.DataFrame, table_name: str, columns: list, remove_nan: bool = False) -> int:
        """
        Export the DataFrame to the warehouse

        :param df: DataFrame
        :param table_name: table name
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

        quoted_table = _quote_identifier(table_name)
        quoted_columns = [_quote_identifier(col) for col in columns]
        col_list = ', '.join(quoted_columns)
        bind_list = ', '.join([f':{column}' for column in columns])

        df_copy = df[columns].copy()
        if remove_nan:
            df_copy = df_copy.replace({np.nan: None, pd.NaT: None})
        df_copy = df_copy.replace({"": None})

        main_dict = df_copy.to_dict('records')
        query = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({bind_list})"
        self.execute_many(query, main_dict)
        return len(main_dict)

    @retry
    def truncate_table(self, table_name: str) -> None:
        """
        Truncate the table

        :param table_name: table name
        :return: None
        """

        if not table_name:
            raise ValueError("table_name cannot be empty")

        quoted_table = _quote_identifier(table_name)
        truncate_query = f"TRUNCATE TABLE {quoted_table}"
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

        quoted_table = _quote_identifier(table_name)
        delete_query = f"DELETE FROM {quoted_table}"
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

        if self.use_pool:
            self._session_pool.close()
        else:
            if self._connection and self._connection.is_healthy():
                self._connection.close()
            self._connection = None


class AsyncOracleConnection(object):
    """
    SQL Connection Class

    :return: None
    """

    def __init__(self, use_pool: bool = False, min_connections: int = 2, max_connections: int = 5):
        self._db_service: str = "Oracle"
        self._username: str | None = None
        self._password: str | None = None
        self._hostname: str | None = None
        self._port: int | None = None
        self._database: str | None = None
        self._sid: str | None = None
        self._connection: AsyncConnection | None = None
        self._session_pool: AsyncConnectionPool | None = None

        self.use_pool = use_pool
        self.min_connections = min_connections
        self.max_connections = max_connections

        self._retry_count = 0
        self.retry_limit = 50
        self.retry_error_codes = oracle_retry_codes

    @async_retry
    async def _connect(self) -> None:
        """
        Connect to the warehouse

        :return: None
        """

        sid_or_service = self._database if self._database else self._sid

        connection = await _async_connect_warehouse(self._username, self._password, self._hostname, self._port,
                                                    sid_or_service, self.min_connections, self.max_connections,
                                                    self.use_pool)

        if self.use_pool:
            self._session_pool = connection
        else:
            self._connection = connection

    async def _get_connection(self) -> AsyncConnection:
        """
        Get the connection, either from the pool or create a new one

        :return: AsyncConnection
        """

        if self.use_pool:
            return await self._session_pool.acquire()
        else:
            if not self._connection or not self._connection.is_healthy():
                await self._connect()
            return self._connection

    async def set_user(self, credentials_dict: dict) -> None:
        """
        Set the user credentials and connect

        :param credentials_dict: dictionary of connection details
        :return: None
        """

        if not (credentials_dict.get('Service') or credentials_dict.get('SID')):
            raise ValueError("Either Service or SID must be provided")

        self._username = credentials_dict['UserName']
        self._password = credentials_dict['Password']
        self._hostname = credentials_dict['Host']
        self._port = int(credentials_dict['Port'])
        self._database = credentials_dict.get('Service')
        self._sid = credentials_dict.get('SID')

        await self._connect()

    async def close_connection(self) -> None:
        """
        Close the connection

        :return: None
        """

        if self.use_pool:
            await self._session_pool.close()
        else:
            if self._connection and self._connection.is_healthy():
                await self._connection.close()
            self._connection = None

    @async_retry
    async def execute(self, query: str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        connection = await self._get_connection()
        with connection.cursor() as cursor:
            await cursor.execute(query)
            await connection.commit()

        if self.use_pool:
            await self._session_pool.release(connection)

    @async_retry
    async def safe_execute(self, query: str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        connection = await self._get_connection()
        with connection.cursor() as cursor:
            await cursor.execute(query, packed_values)
            await connection.commit()

        if self.use_pool:
            await self._session_pool.release(connection)

    @async_retry
    async def execute_multiple(self, queries: list[tuple[str, dict]]) -> None:
        """
        Execute multiple queries

        :param queries: list of queries
        :return: None
        """

        connection = await self._get_connection()
        with connection.cursor() as cursor:
            for item in queries:
                query = item[0]
                packed_values = item[1]
                if packed_values:
                    await cursor.execute(query, packed_values)
                else:
                    await cursor.execute(query)
            await connection.commit()

        if self.use_pool:
            await self._session_pool.release(connection)

    @async_retry
    async def execute_many(self, query: str, dictionary: list[dict]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        connection = await self._get_connection()
        with connection.cursor() as cursor:
            await cursor.executemany(query, dictionary)
            await connection.commit()

        if self.use_pool:
            await self._session_pool.release(connection)

    @async_retry
    async def fetch_data(self, query: str, packed_data=None) -> list:
        """
        Fetch the data from the query

        :param query: query
        :param packed_data: packed data
        :return: rows
        """

        connection = await self._get_connection()
        with connection.cursor() as cursor:
            if packed_data:
                await cursor.execute(query, packed_data)
            else:
                await cursor.execute(query)
            rows = await cursor.fetchall()
        await connection.commit()

        if self.use_pool:
            await self._session_pool.release(connection)
        return rows

    @async_retry
    async def remove_matching_data(self, df: pd.DataFrame, table_name: str, match_cols: list) -> int:
        """
        Remove matching data from the warehouse

        :param df: DataFrame
        :param table_name: table name
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
        param_list = [f"{column} = :{column}" for column in match_cols]
        params = ' AND '.join(param_list)

        quoted_table = _quote_identifier(table_name)
        query = f"DELETE FROM {quoted_table} WHERE {params}"

        main_dict = df_subset.to_dict('records')
        await self.execute_many(query, main_dict)
        return len(main_dict)

    @async_retry
    async def export_df_to_warehouse(self, df: pd.DataFrame, table_name: str, columns: list, remove_nan: bool = False) -> int:
        """
        Export the DataFrame to the warehouse

        :param df: DataFrame
        :param table_name: table name
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

        quoted_table = _quote_identifier(table_name)
        quoted_columns = [_quote_identifier(col) for col in columns]
        col_list = ', '.join(quoted_columns)
        bind_list = ', '.join([f':{column}' for column in columns])

        df_copy = df[columns].copy()
        if remove_nan:
            df_copy = df_copy.replace({np.nan: None, pd.NaT: None})
        df_copy = df_copy.replace({"": None})

        main_dict = df_copy.to_dict('records')
        query = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({bind_list})"
        await self.execute_many(query, main_dict)
        return len(main_dict)

    @async_retry
    async def truncate_table(self, table_name: str) -> None:
        """
        Truncate the table

        :param table_name: table name
        :return: None
        """

        if not table_name:
            raise ValueError("table_name cannot be empty")

        quoted_table = _quote_identifier(table_name)
        truncate_query = f"TRUNCATE TABLE {quoted_table}"
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

        quoted_table = _quote_identifier(table_name)
        delete_query = f"DELETE FROM {quoted_table}"
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
