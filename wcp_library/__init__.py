import logging
import sys
from pathlib import Path
from typing import Callable, Generator

import cryptography.hazmat.primitives.kdf.pbkdf2

# PyInstaller import
# import pip_system_certs.wrapt_requests

logger = logging.getLogger(__name__)

# Application Path
APPLICATION_PATH: Path = (
    Path(sys.executable).parent
    if getattr(sys, "frozen", False)
    else Path().absolute()
)


def divide_chunks(list_obj: list, size: int) -> Generator:
    """
    Divide a list into chunks of size

    :param list_obj:
    :param size:
    :return: Generator
    """

    for i in range(0, len(list_obj), size):
        yield list_obj[i : i + size]
