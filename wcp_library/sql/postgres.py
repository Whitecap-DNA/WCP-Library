import logging
from typing import Optional

import numpy as np
import pandas as pd
from psycopg.conninfo import make_conninfo
from psycopg.sql import SQL
from psycopg_pool import AsyncConnectionPool, ConnectionPool

from wcp_library.sql import retry, async_retry

logger = logging.getLogger(__name__)


def _connect_warehouse(username: str, password: str, hostname: str, port: int, database: str, min_connections: int,
                       max_connections: int) -> ConnectionPool:
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

    session_pool = ConnectionPool(
        conninfo=conninfo,
        min_size=min_connections,
        max_size=max_connections,
        kwargs={'options': '-c datestyle=ISO,YMD'},
        open=True
    )
    return session_pool


async def _async_connect_warehouse(username: str, password: str, hostname: str, port: int, database: str, min_connections: int,
                             max_connections: int) -> AsyncConnectionPool:
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

    session_pool = AsyncConnectionPool(
        conninfo=conninfo,
        min_size=min_connections,
        max_size=max_connections,
        kwargs={"options": "-c datestyle=ISO,YMD"}
    )
    return session_pool


"""~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"""


class PostgresConnection(object):
    """
    SQL Connection Class

    :return: None
    """

    def __init__(self, min_connections: int = 2, max_connections: int = 5):
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._hostname: Optional[str] = None
        self._port: Optional[int] = None
        self._database: Optional[str] = None
        self._session_pool: Optional[ConnectionPool] = None

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

        self._session_pool = _connect_warehouse(self._username, self._password, self._hostname, self._port,
                                               self._database, self.min_connections, self.max_connections)

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

        self._session_pool.close()

    @retry
    def execute(self, query: SQL | str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        with self._session_pool.connection() as connection:
            connection.execute(query)

    @retry
    def safe_execute(self, query: SQL | str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        with self._session_pool.connection() as connection:
            connection.execute(query, packed_values)

    @retry
    def execute_multiple(self, queries: list[list[SQL | str, dict]]) -> None:
        """
        Execute multiple queries

        :param queries: list of queries
        :return: None
        """

        with self._session_pool.connection() as connection:
            for item in queries:
                query = item[0]
                packed_values = item[1]
                if packed_values:
                    connection.execute(query, packed_values)
                else:
                    connection.execute(query)

    @retry
    def execute_many(self, query: SQL | str, dictionary: list[dict]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        with self._session_pool.connection() as connection:
            cursor = connection.cursor()
            cursor.executemany(query, dictionary)

    @retry
    def fetch_data(self, query: SQL | str, packed_data=None) -> list[tuple]:
        """
        Fetch the data from the query

        :param query: query
        :param packed_data: packed data
        :return: rows
        """

        with self._session_pool.connection() as connection:
            cursor = connection.cursor()
            if packed_data:
                cursor.execute(query, packed_data)
            else:
                cursor.execute(query)
            rows = cursor.fetchall()
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

        query = """INSERT INTO {} ({}) VALUES ({})""".format(outputTableName, col, params)
        self.execute_many(query, main_dict)

    @retry
    def truncate_table(self, tableName: str) -> None:
        """
        Truncate the table

        :param tableName: table name
        :return: None
        """

        truncateQuery = """TRUNCATE TABLE {}""".format(tableName)
        self.execute(truncateQuery)

    @retry
    def empty_table(self, tableName: str) -> None:
        """
        Empty the table

        :param tableName: table name
        :return: None
        """

        deleteQuery = """DELETE FROM {}""".format(tableName)
        self.execute(deleteQuery)

    def __del__(self) -> None:
        """
        Destructor

        :return: None
        """

        self._session_pool.close()


class AsyncPostgresConnection(object):
    """
    SQL Connection Class

    :return: None
    """

    def __init__(self, min_connections: int = 2, max_connections: int = 5):
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._hostname: Optional[str] = None
        self._port: Optional[int] = None
        self._database: Optional[str] = None
        self._session_pool: Optional[AsyncConnectionPool] = None

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

        self._session_pool = await _async_connect_warehouse(self._username, self._password, self._hostname, self._port,
                                                            self._database, self.min_connections, self.max_connections)
        await self._session_pool.open()

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

        await self._session_pool.close()

    @async_retry
    async def execute(self, query: SQL | str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        async with self._session_pool.connection() as connection:
            await connection.execute(query)

    @async_retry
    async def safe_execute(self, query: SQL | str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        async with self._session_pool.connection() as connection:
            await connection.execute(query, packed_values)

    @async_retry
    async def execute_multiple(self, queries: list[list[SQL | str, dict]]) -> None:
        """
        Execute multiple queries

        :param queries: list of queries
        :return: None
        """

        async with self._session_pool.connection() as connection:
            for item in queries:
                query = item[0]
                packed_values = item[1]
                if packed_values:
                    await connection.execute(query, packed_values)
                else:
                    await connection.execute(query)

    @async_retry
    async def execute_many(self, query: SQL | str, dictionary: list[dict]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        async with self._session_pool.connection() as connection:
            cursor = connection.cursor()
            await cursor.executemany(query, dictionary)

    @async_retry
    async def fetch_data(self, query: SQL | str, packed_data=None) -> list[tuple]:
        """
        Fetch the data from the query

        :param query: query
        :param packed_data: packed data
        :return: rows
        """

        async with self._session_pool.connection() as connection:
            cursor = connection.cursor()
            if packed_data:
                await cursor.execute(query, packed_data)
            else:
                await cursor.execute(query)
            rows = await cursor.fetchall()
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
        query = """DELETE FROM {} WHERE {}""".format(outputTableName, params)
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

        query = """INSERT INTO {} ({}) VALUES ({})""".format(outputTableName, col, params)
        await self.execute_many(query, main_dict)

    @async_retry
    async def truncate_table(self, tableName: str) -> None:
        """
        Truncate the table

        :param tableName: table name
        :return: None
        """

        truncateQuery = """TRUNCATE TABLE {}""".format(tableName)
        await self.execute(truncateQuery)

    @async_retry
    async def empty_table(self, tableName: str) -> None:
        """
        Empty the table

        :param tableName: table name
        :return: None
        """

        deleteQuery = """DELETE FROM {}""".format(tableName)
        await self.execute(deleteQuery)
