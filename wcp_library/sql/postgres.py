import logging
from typing import Optional

import numpy as np
import pandas as pd
import psycopg
from psycopg import AsyncConnection, Connection
from psycopg.conninfo import make_conninfo
from psycopg.sql import SQL
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from wcp_library.sql import retry, async_retry

logger = logging.getLogger(__name__)


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

    conn_string = f"dbname={database} user={username} password={password} host={hostname} port={port}"
    conninfo = make_conninfo(conn_string)

    if use_pool:
        logger.debug(f"Creating connection pool with min size {min_connections} and max size {max_connections}")
        session_pool = ConnectionPool(
            conninfo=conninfo,
            min_size=min_connections,
            max_size=max_connections,
            kwargs={'options': '-c datestyle=ISO,YMD'},
            open=True
        )
        return session_pool
    else:
        logger.debug("Creating single connection")
        connection = psycopg.connect(conninfo=conninfo, options='-c datestyle=ISO,YMD')
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

    conn_string = f"dbname={database} user={username} password={password} host={hostname} port={port}"
    conninfo = make_conninfo(conn_string)

    if use_pool:
        logger.debug(f"Creating async connection pool with min size {min_connections} and max size {max_connections}")
        session_pool = AsyncConnectionPool(
            conninfo=conninfo,
            min_size=min_connections,
            max_size=max_connections,
            kwargs={"options": "-c datestyle=ISO,YMD"},
            open=False
        )
        return session_pool
    else:
        logger.debug("Creating single async connection")
        connection = await AsyncConnection.connect(conninfo=conninfo, options='-c datestyle=ISO,YMD')
        return connection


"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""


class PostgresConnection(object):
    """
    SQL Connection Class

    :return: None
    """

    def __init__(self, use_pool: bool = False, min_connections: int = 2, max_connections: int = 5):
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._hostname: Optional[str] = None
        self._port: Optional[int] = None
        self._database: Optional[str] = None
        self._connection: Optional[Connection] = None
        self._session_pool: Optional[ConnectionPool] = None

        self.use_pool = use_pool
        self.min_connections = min_connections
        self.max_connections = max_connections

        self._retry_count = 0
        self.retry_limit = 50
        self.retry_error_codes = ['08001', '08004']

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

        self._username: Optional[str] = credentials_dict['UserName']
        self._password: Optional[str] = credentials_dict['Password']
        self._hostname: Optional[str] = credentials_dict['Host']
        self._port: Optional[int] = int(credentials_dict['Port'])
        self._database: Optional[str] = credentials_dict['Database']

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
    def execute(self, query: SQL | str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        connection = self._get_connection()
        with connection:
            connection.execute(query)

        if self.use_pool:
            self._session_pool.putconn(connection)

    @retry
    def safe_execute(self, query: SQL | str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        connection = self._get_connection()
        with connection:
            connection.execute(query, packed_values)

        if self.use_pool:
            self._session_pool.putconn(connection)

    @retry
    def execute_multiple(self, queries: list[tuple[SQL | str, dict]]) -> None:
        """
        Execute multiple queries

        :param queries: list of queries
        :return: None
        """

        connection = self._get_connection()
        with connection:
            for item in queries:
                query = item[0]
                packed_values = item[1]
                if packed_values:
                    connection.execute(query, packed_values)
                else:
                    connection.execute(query)

        if self.use_pool:
            self._session_pool.putconn(connection)

    @retry
    def execute_many(self, query: SQL | str, dictionary: list[dict]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        connection = self._get_connection()
        with connection:
            cursor = connection.cursor()
            cursor.executemany(query, dictionary)

        if self.use_pool:
            self._session_pool.putconn(connection)

    @retry
    def fetch_data(self, query: SQL | str, packed_data=None) -> list[tuple]:
        """
        Fetch the data from the query

        :param query: query
        :param packed_data: packed data
        :return: rows
        """

        connection = self._get_connection()
        with connection:
            cursor = connection.cursor()
            if packed_data:
                cursor.execute(query, packed_data)
            else:
                cursor.execute(query)
            rows = cursor.fetchall()

        if self.use_pool:
            self._session_pool.putconn(connection)
        return rows

    @retry
    def remove_matching_data(self, dfObj: pd.DataFrame, outputTableName: str, match_cols: list) -> None:
        """
        Remove matching data from the warehouse

        :param dfObj: DataFrame
        :param outputTableName: output table name
        :param match_cols: list of columns
        :return: None
        """

        df = dfObj[match_cols]
        param_list = []
        for column in match_cols:
            param_list.append(f"{column} = %({column})s")
        if len(param_list) > 1:
            params = ' AND '.join(param_list)
        else:
            params = param_list[0]

        main_dict = df.to_dict('records')
        query = """DELETE FROM {} WHERE {}""".format(outputTableName, params)
        self.execute_many(query, main_dict)

    @retry
    def export_DF_to_warehouse(self, dfObj: pd.DataFrame, outputTableName: str, columns: list, remove_nan=False) -> None:
        """
        Export the DataFrame to the warehouse

        :param dfObj: DataFrame
        :param outputTableName: output table name
        :param columns: list of columns
        :param remove_nan: remove NaN values
        :return: None
        """

        col = ', '.join(columns)
        param_list = []
        for column in columns:
            param_list.append(f"%({column})s")
        params = ', '.join(param_list)

        if remove_nan:
            dfObj = dfObj.replace({np.nan: None})
        main_dict = dfObj.to_dict('records')
        for record in main_dict:
            for key in record:
                if record[key] == '':
                    record[key] = None

        query = f"INSERT INTO {outputTableName} ({col}) VALUES ({params})"
        self.execute_many(query, main_dict)

    @retry
    def truncate_table(self, tableName: str) -> None:
        """
        Truncate the table

        :param tableName: table name
        :return: None
        """

        truncateQuery = f"TRUNCATE TABLE {tableName}"
        self.execute(truncateQuery)

    @retry
    def empty_table(self, tableName: str) -> None:
        """
        Empty the table

        :param tableName: table name
        :return: None
        """

        deleteQuery = f"DELETE FROM {tableName}"
        self.execute(deleteQuery)

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
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._hostname: Optional[str] = None
        self._port: Optional[int] = None
        self._database: Optional[str] = None
        self._connection: Optional[AsyncConnection] = None
        self._session_pool: Optional[AsyncConnectionPool] = None

        self.use_pool = use_pool
        self.min_connections = min_connections
        self.max_connections = max_connections

        self._retry_count = 0
        self.retry_limit = 50
        self.retry_error_codes = ['08001', '08004']

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

        self._username: Optional[str] = credentials_dict['UserName']
        self._password: Optional[str] = credentials_dict['Password']
        self._hostname: Optional[str] = credentials_dict['Host']
        self._port: Optional[int] = int(credentials_dict['Port'])
        self._database: Optional[str] = credentials_dict['Database']

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
    async def execute(self, query: SQL | str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        connection = await self._get_connection()
        async with connection:
            await connection.execute(query)

        if self.use_pool:
            await self._session_pool.putconn(connection)

    @async_retry
    async def safe_execute(self, query: SQL | str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        connection = await self._get_connection()
        async with connection:
            await connection.execute(query, packed_values)

        if self.use_pool:
            await self._session_pool.putconn(connection)

    @async_retry
    async def execute_multiple(self, queries: list[tuple[SQL | str, dict]]) -> None:
        """
        Execute multiple queries

        :param queries: list of queries
        :return: None
        """

        connection = await self._get_connection()
        async with connection:
            for item in queries:
                query = item[0]
                packed_values = item[1]
                if packed_values:
                    await connection.execute(query, packed_values)
                else:
                    await connection.execute(query)

        if self.use_pool:
            await self._session_pool.putconn(connection)

    @async_retry
    async def execute_many(self, query: SQL | str, dictionary: list[dict]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        connection = await self._get_connection()
        async with connection:
            cursor = connection.cursor()
            await cursor.executemany(query, dictionary)

        if self.use_pool:
            await self._session_pool.putconn(connection)

    @async_retry
    async def fetch_data(self, query: SQL | str, packed_data=None) -> list[tuple]:
        """
        Fetch the data from the query

        :param query: query
        :param packed_data: packed data
        :return: rows
        """

        connection = await self._get_connection()
        async with connection:
            cursor = connection.cursor()
            if packed_data:
                await cursor.execute(query, packed_data)
            else:
                await cursor.execute(query)
            rows = await cursor.fetchall()

        if self.use_pool:
            await self._session_pool.putconn(connection)
        return rows

    @async_retry
    async def remove_matching_data(self, dfObj: pd.DataFrame, outputTableName: str, match_cols: list) -> None:
        """
        Remove matching data from the warehouse

        :param dfObj: DataFrame
        :param outputTableName: output table name
        :param match_cols: list of columns
        :return: None
        """

        df = dfObj[match_cols]
        param_list = []
        for column in match_cols:
            param_list.append(f"{column} = %({column})s")
        if len(param_list) > 1:
            params = ' AND '.join(param_list)
        else:
            params = param_list[0]

        main_dict = df.to_dict('records')
        query = f"DELETE FROM {outputTableName} WHERE {params}"
        await self.execute_many(query, main_dict)

    @async_retry
    async def export_DF_to_warehouse(self, dfObj: pd.DataFrame, outputTableName: str, columns: list, remove_nan=False) -> None:
        """
        Export the DataFrame to the warehouse

        :param dfObj: DataFrame
        :param outputTableName: output table name
        :param columns: list of columns
        :param remove_nan: remove NaN values
        :return: None
        """

        col = ', '.join(columns)
        param_list = []
        for column in columns:
            param_list.append(f"%({column})s")
        params = ', '.join(param_list)

        if remove_nan:
            dfObj = dfObj.replace({np.nan: None})
        main_dict = dfObj.to_dict('records')
        for record in main_dict:
            for key in record:
                if record[key] == '':
                    record[key] = None

        query = f"INSERT INTO {outputTableName} ({col}) VALUES ({params})"
        await self.execute_many(query, main_dict)

    @async_retry
    async def truncate_table(self, tableName: str) -> None:
        """
        Truncate the table

        :param tableName: table name
        :return: None
        """

        truncateQuery = f"TRUNCATE TABLE {tableName}"
        await self.execute(truncateQuery)

    @async_retry
    async def empty_table(self, tableName: str) -> None:
        """
        Empty the table

        :param tableName: table name
        :return: None
        """

        deleteQuery = f"DELETE FROM {tableName}"
        await self.execute(deleteQuery)
