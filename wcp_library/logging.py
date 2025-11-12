import logging
import sys
from pathlib import Path

from wcp_library import application_path


def create_log(file_level: int, console_level: int, iterations: int, project_name: str, mode: str = "w",
               format: str = "%(asctime)s:%(levelname)s:%(module)s:%(filename)s:%(lineno)d:%(message)s",
               logging_dir: Path = application_path):
    """
    Create log file.

    Log levels: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET

    format help: https://docs.python.org/3/library/logging.html#logrecord-attributes

    :param file_level: Logging level to output to log file.
    :param console_level: Logging level to output to console.
    :param iterations: Number of log files to keep.
    :param project_name: Name of the project. (Used as the log file name)
    :param mode: Mode to open the log file. (Default: "w")
    :param format: Log Format (Default: "%(asctime)s:%(levelname)s:%(module)s:%(filename)s:%(lineno)d:%(message)s")
    :param logging_dir: Directory to save log files. (Default: application_path)
    :return:
    """

    possible_iterative_filenames = [(logging_dir / (project_name + ".log"))] + [logging_dir / (project_name + f"_{i + 1}.log") for i in range(iterations)]

    if iterations == 0:
        (logging_dir / (project_name + ".log")).unlink(missing_ok=True)
    else:
        last_file = possible_iterative_filenames[-1]
        last_file.unlink(missing_ok=True)

        for i in range(iterations, 0, -1):
            if possible_iterative_filenames[i - 1].exists():
                possible_iterative_filenames[i - 1].rename(possible_iterative_filenames[i])

    logging.basicConfig(
        filename=(logging_dir / (project_name + ".log")),
        level=file_level,
        format=format,
        filemode=mode
    )

    MIN_LEVEL = console_level
    stdout_hdlr = logging.StreamHandler(sys.stdout)
    stderr_hdlr = logging.StreamHandler(sys.stderr)
    stdout_hdlr.setLevel(MIN_LEVEL)
    stderr_hdlr.setLevel(max(MIN_LEVEL, logging.WARNING))

    rootLogger = logging.getLogger()
    rootLogger.addHandler(stdout_hdlr)
    rootLogger.addHandler(stderr_hdlr)
    logger = logging.getLogger(__name__)
    logger.setLevel(console_level)
