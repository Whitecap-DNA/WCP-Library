import logging
from typing import Optional

import pandas as pd
from psycopg.sql import SQL
from psycopg_pool import AsyncConnectionPool

from WCP_Library.async_sql import retry

logger = logging.getLogger(__name__)


async def connect_warehouse(username: str, password: str, hostname: str, port: int, database: str) -> AsyncConnectionPool:
    """
    Create Warehouse Connection

    :param username: username
    :param password: password
    :param hostname: hostname
    :param port: port
    :param database: database
    :return: session_pool
    """

    url = f"postgres://{username}:{password}@{hostname}:{port}/{database}"

    session_pool = AsyncConnectionPool(
        conninfo=url,
        min_size=2,
        max_size=5,
    )
    await session_pool.open()
    return session_pool


class SQLConnection(object):
    """
    SQL Connection Class

    :return: None
    """

    def __init__(self):
        self._username: Optional[str] = None
        self._password: Optional[str] = None
        self._hostname: Optional[str] = None
        self._port: Optional[int] = None
        self._database: Optional[str] = None
        self._session_pool: Optional[AsyncConnectionPool] = None

        self._retry_count = 0
        self.retry_limit = 50
        self.retry_error_codes = ['08001', '08004']

    @retry
    async def _connect(self) -> None:
        """
        Connect to the warehouse

        :return: None
        """

        self._session_pool = await connect_warehouse(self._username, self._password, self._hostname, self._port, self._database)

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

    @retry
    async def execute(self, query: SQL | str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        async with self._session_pool.connection() as connection:
            await connection.execute(query)

    @retry
    async def safe_execute(self, query: SQL | str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        async with self._session_pool.connection() as connection:
            await connection.execute(query, packed_values)

    @retry
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

    @retry
    async def execute_many(self, query: SQL | str, dictionary: list[dict]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        async with self._session_pool.connection() as connection:
            await connection.executemany(query, dictionary)

    @retry
    async def fetch_data(self, query: SQL | str, packed_data=None):
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

    @retry
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

        main_dict = dfObj.to_dict('records')
        if remove_nan:
            for val, item in enumerate(main_dict):
                for sub_item, value in item.items():
                    if pd.isna(value):
                        main_dict[val][sub_item] = None
                    else:
                        main_dict[val][sub_item] = value

        query = """INSERT INTO {} ({}) VALUES ({})""".format(outputTableName, col, params)
        await self.execute_many(query, main_dict)

    @retry
    async def truncate_table(self, tableName: str) -> None:
        """
        Truncate the table

        :param tableName: table name
        :return: None
        """

        truncateQuery = """TRUNCATE TABLE {}""".format(tableName)
        await self.execute(truncateQuery)

    @retry
    async def empty_table(self, tableName: str) -> None:
        """
        Empty the table

        :param tableName: table name
        :return: None
        """

        deleteQuery = """DELETE FROM {}""".format(tableName)
        await self.execute(deleteQuery)
