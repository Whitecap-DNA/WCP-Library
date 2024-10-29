import logging
from functools import wraps
from time import sleep
from typing import Optional

import pandas as pd
import oracledb
from oracledb import AsyncConnectionPool

logger = logging.getLogger(__name__)


def retry(f: callable) -> callable:
    """
    Decorator to retry a function

    Only retries on:

    ORA-01033: ORACLE initialization or shutdown in progress

    DPY-6005: Connection to the database failed

    :param f: function
    :return: function
    """

    @wraps(f)
    async def wrapper(self, *args, **kwargs):
        self._retry_count = 0
        while True:
            try:
                return await f(self, *args, **kwargs)
            except oracledb.OperationalError as e:
                error_obj, = e.args
                if error_obj.full_code in ['ORA-01033', 'DPY-6005'] and self._retry_count < self.retry_limit:
                    self._retry_count += 1
                    logger.debug("Oracle connection error")
                    logger.debug(error_obj.message)
                    logger.info("Waiting 5 minutes before retrying Oracle connection")
                    sleep(300)
                else:
                    raise e
    return wrapper



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

    dsn = oracledb.makedsn(hostname, port, sid=database)
    session_pool = oracledb.create_pool_async(
        user=username,
        password=password,
        dsn=dsn,
        min=2,
        max=5,
        increment=1,
        threaded=True,
        encoding="UTF-8"
    )
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

    @retry
    async def connect(self) -> None:
        """
        Connect to the warehouse

        :return: None
        """

        self._session_pool = await connect_warehouse(self._username, self._password, self._hostname, self._port, self._database)

    async def unpack_user(self, connectionDetails: dict) -> None:
        """
        Unpack the user credentials

        :param connectionDetails: dictionary of connection details
        :return: None
        """

        self._username = connectionDetails['username']
        self._password = connectionDetails['password']
        self._hostname = connectionDetails['hostname']
        self._port = int(connectionDetails['port'])
        self._database = connectionDetails['database']

    async def close_connection(self) -> None:
        """
        Close the connection

        :return: None
        """

        await self._session_pool.close()

    @retry
    async def execute(self, query: str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        connection = self._session_pool.acquire()
        cursor = connection.cursor()
        await cursor.execute(query)
        await connection.commit()
        await self._session_pool.release(connection)

    @retry
    async def safe_execute(self, query: str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        connection = self._session_pool.acquire()
        cursor = connection.cursor()
        await cursor.execute(query, packed_values)
        await connection.commit()
        await self._session_pool.release(connection)

    @retry
    async def execute_multiple(self, queries: list[list[str, dict]]) -> None:
        """
        Execute multiple queries

        :param queries: list of queries
        :return: None
        """

        connection = self._session_pool.acquire()
        cursor = connection.cursor()
        for item in queries:
            query = item[0]
            packed_values = item[1]
            if packed_values:
                await cursor.execute(query, packed_values)
            else:
                await cursor.execute(query)
        await connection.commit()
        await self._session_pool.release(connection)

    @retry
    async def execute_many(self, query: str, dictionary: list[dict]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        connection = self._session_pool.acquire()
        cursor = connection.cursor()
        await cursor.executemany(query, dictionary)
        await connection.commit()
        await self._session_pool.release(connection)

    @retry
    async def fetch_data(self, query: str, packed_data=None):
        """
        Fetch the data from the query

        :param query: query
        :param packed_data: packed data
        :return: rows
        """

        connection = self._session_pool.acquire()
        cursor = connection.cursor()
        if packed_data:
            await cursor.execute(query, packed_data)
        else:
            await cursor.execute(query)
        rows = cursor.fetchall()
        await self._session_pool.release(connection)
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
        bindList = []
        for column in columns:
            bindList.append(':' + column)
        bind = ', '.join(bindList)

        main_dict = dfObj.to_dict('records')
        if remove_nan:
            for val, item in enumerate(main_dict):
                for sub_item, value in item.items():
                    if pd.isna(value):
                        main_dict[val][sub_item] = None
                    else:
                        main_dict[val][sub_item] = value

        query = """INSERT INTO {} ({}) VALUES ({})""".format(outputTableName, col, bind)
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
