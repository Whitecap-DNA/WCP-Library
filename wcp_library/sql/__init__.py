import asyncio
import logging
from functools import wraps
from time import sleep

import oracledb
import psycopg

logger = logging.getLogger(__name__)


def retry(func: callable) -> callable:
    """
    Decorator to retry a function

    :param func: function
    :return: function
    """

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        self._retry_count = 0
        while True:
            try:
                return func(self, *args, **kwargs)
            except (oracledb.OperationalError, oracledb.DatabaseError, psycopg.OperationalError) as e:
                if isinstance(e, (oracledb.OperationalError, oracledb.DatabaseError, psycopg.OperationalError, psycopg.DatabaseError)):
                    (error_obj,) = e.args
                    if isinstance(error_obj, str):
                        raise e
                    elif error_obj.full_code in self.retry_error_codes and self._retry_count < self.retry_limit:
                        self._retry_count += 1
                        logger.debug(f"Database connection error: {error_obj.full_code}")
                        logger.debug(f"Error message: {error_obj.message}")
                        logger.info(f"Retry attempt {self._retry_count}/{self.retry_limit}. Waiting 5 minutes before retrying connection")
                        sleep(300)
                    else:
                        if error_obj.full_code not in self.retry_error_codes:
                            logger.error(f"Non-retryable database error: {error_obj.full_code}")
                        else:
                            logger.error(f"Retry limit ({self.retry_limit}) reached for error: {error_obj.full_code}")
                        raise e
                else:
                    raise e
    return wrapper


def async_retry(func: callable) -> callable:
    """
    Decorator to retry a function

    :param func: function
    :return: function
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        self._retry_count = 0
        while True:
            try:
                return await func(self, *args, **kwargs)
            except (oracledb.OperationalError, oracledb.DatabaseError, psycopg.OperationalError) as e:
                if isinstance(e, (oracledb.OperationalError, oracledb.DatabaseError, psycopg.OperationalError, psycopg.DatabaseError)):
                    error_obj, = e.args
                    if isinstance(error_obj, str):
                        raise e
                    elif error_obj.full_code in self.retry_error_codes and self._retry_count < self.retry_limit:
                        self._retry_count += 1
                        logger.debug(f"Database connection error: {error_obj.full_code}")
                        logger.debug(f"Error message: {error_obj.message}")
                        logger.info(f"Retry attempt {self._retry_count}/{self.retry_limit}. Waiting 5 minutes before retrying connection")
                        await asyncio.sleep(300)
                    else:
                        if error_obj.full_code not in self.retry_error_codes:
                            logger.error(f"Non-retryable database error: {error_obj.full_code}")
                        else:
                            logger.error(f"Retry limit ({self.retry_limit}) reached for error: {error_obj.full_code}")
                        raise e
                else:
                    raise e
    return wrapper
