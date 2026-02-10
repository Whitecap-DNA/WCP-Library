import asyncio
import logging
import random
import sys
import time
from functools import wraps
from pathlib import Path
from typing import Callable, Generator, Optional, Type, Union

import cryptography.hazmat.primitives.kdf.pbkdf2

# PyInstaller import
import pip_system_certs.wrapt_requests

logger = logging.getLogger(__name__)

# For retry logic
MAX_ATTEMPTS = 5
DELAY = 2
BACKOFF = 2
JITTER = 3


# Application Path
if getattr(sys, "frozen", False):
    APPLICATION_PATH = sys.executable
    APPLICATION_PATH = Path(APPLICATION_PATH).parent
else:
    APPLICATION_PATH = Path().absolute()


def divide_chunks(list_obj: list, size: int) -> Generator:
    """
    Divide a list into chunks of size

    :param list_obj:
    :param size:
    :return: Generator
    """

    for i in range(0, len(list_obj), size):
        yield list_obj[i : i + size]


def retry(
    exceptions: tuple,
    max_attempts: Optional[int] = MAX_ATTEMPTS,
    delay: Optional[int] = DELAY,
    backoff: Optional[int] = BACKOFF,
    jitter: Optional[int] = JITTER,
) -> Callable:
    """
    Decorator to retry a function on a specified exception with exponential backoff and jitter.
    Automatically handles both sync and async functions.

    :param exceptions: Tuple of exception types to catch and retry on.
    :param max_attempts: Maximum number of retry attempts.
    :param delay: Initial delay between retries (in seconds).
    :param backoff: Multiplier to increase delay after each failure.
    :param jitter: Maximum number of seconds to add randomly to each delay.
    :return: The decorated function with retry logic.
    """

    def _handle_retry(attempt: int, error: Exception, wait_time: float) -> float:
        if attempt == max_attempts - 1:
            logger.error("Retry failed after %d attempts.", max_attempts)
            raise error

        randomized_delay = wait_time + random.uniform(0, jitter)
        logger.warning(
            "Attempt %d failed: %s. Retrying in %.2f seconds...",
            attempt + 1,
            error,
            randomized_delay,
        )
        return randomized_delay, wait_time * backoff

    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):

            @wraps(func)
            async def wrapper(*args, **kwargs):
                wait_time = delay
                for attempt in range(max_attempts):
                    try:
                        return await func(*args, **kwargs)
                    except exceptions as error:
                        sleep_time, wait_time = _handle_retry(attempt, error, wait_time)
                        await asyncio.sleep(sleep_time)
                return None

        else:

            @wraps(func)
            def wrapper(*args, **kwargs):
                wait_time = delay
                for attempt in range(max_attempts):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as error:
                        sleep_time, wait_time = _handle_retry(attempt, error, wait_time)
                        time.sleep(sleep_time)
                return None

        return wrapper

    return decorator
