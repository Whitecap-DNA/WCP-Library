import logging
from typing import Optional

import pandas as pd
import oracledb
from oracledb import ConnectionPool

from WCP_Library.sql import retry

logger = logging.getLogger(__name__)


def connect_warehouse(username: str, password: str, hostname: str, port: int, database: str) -> ConnectionPool:
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
    session_pool = oracledb.create_pool(
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
        self._sid: Optional[str] = None
        self._session_pool: Optional[ConnectionPool] = None

        self._retry_count = 0
        self.retry_limit = 50
        self.retry_error_codes = ['ORA-01033', 'DPY-6005', 'DPY-4011']

    @retry
    def _connect(self) -> None:
        """
        Connect to the warehouse

        :return: None
        """

        sid_or_service = self._database if self._database else self._sid

        self._session_pool = connect_warehouse(self._username, self._password, self._hostname, self._port, sid_or_service)

    def set_user(self, credentials_dict: dict) -> None:
        """
        Set the user credentials and connect

        :param credentials_dict: dictionary of connection details
        :return: None
        """

        if not any([self._database, self._sid]):
            raise ValueError("Either Service or SID must be provided")

        self._username: Optional[str] = credentials_dict['UserName']
        self._password: Optional[str] = credentials_dict['Password']
        self._hostname: Optional[str] = credentials_dict['Host']
        self._port: Optional[int] = int(credentials_dict['Port'])
        self._database: Optional[str] = credentials_dict['Service'] if 'Service' in credentials_dict else None
        self._sid: Optional[str] = credentials_dict['SID'] if 'SID' in credentials_dict else None

        self._connect()

    def close_connection(self) -> None:
        """
        Close the connection

        :return: None
        """

        self._session_pool.close()

    @retry
    def execute(self, query: str) -> None:
        """
        Execute the query

        :param query: query
        :return: None
        """

        connection = self._session_pool.acquire()
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        self._session_pool.release(connection)

    @retry
    def safe_execute(self, query: str, packed_values: dict) -> None:
        """
        Execute the query without SQL Injection possibility, to be used with external facing projects.

        :param query: query
        :param packed_values: dictionary of values
        :return: None
        """

        connection = self._session_pool.acquire()
        cursor = connection.cursor()
        cursor.execute(query, packed_values)
        connection.commit()
        self._session_pool.release(connection)

    @retry
    def execute_multiple(self, queries: list[list[str, dict]]) -> None:
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
                cursor.execute(query, packed_values)
            else:
                cursor.execute(query)
        connection.commit()
        self._session_pool.release(connection)

    @retry
    def execute_many(self, query: str, dictionary: list[dict]) -> None:
        """
        Execute many queries

        :param query: query
        :param dictionary: dictionary of values
        :return: None
        """

        connection = self._session_pool.acquire()
        cursor = connection.cursor()
        cursor.executemany(query, dictionary)
        connection.commit()
        self._session_pool.release(connection)

    @retry
    def fetch_data(self, query: str, packed_data=None):
        """
        Fetch the data from the query

        :param query: query
        :param packed_data: packed data
        :return: rows
        """

        connection = self._session_pool.acquire()
        cursor = connection.cursor()
        if packed_data:
            cursor.execute(query, packed_data)
        else:
            cursor.execute(query)
        rows = cursor.fetchall()
        self._session_pool.release(connection)
        return rows

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
