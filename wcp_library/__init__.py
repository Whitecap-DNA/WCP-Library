import sys
from pathlib import Path
from typing import Generator

# PyInstaller import
import pip_system_certs.wrapt_requests
import cryptography.hazmat.primitives.kdf.pbkdf2


# Application Path
if getattr(sys, 'frozen', False):
    application_path = sys.executable
    application_path = Path(application_path).parent
else:
    application_path = Path().absolute()


def divide_chunks(list_obj: list, size: int) -> Generator:
    """
    Divide a list into chunks of size

    :param list_obj:
    :param size:
    :return: Generator
    """

    for i in range(0, len(list_obj), size):
        yield list_obj[i:i + size]
