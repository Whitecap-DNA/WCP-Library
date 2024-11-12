import logging
import sys

from wcp_library import application_path


def create_log(level: int, iterations: int, project_name: str, mode: str = "w",
               format: str = "%(asctime)s:%(levelname)s:%(module)s:%(filename)s:%(lineno)d:%(message)s"):
    """
    Create log file.

    Log levels: CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET

    format help: https://docs.python.org/3/library/logging.html#logrecord-attributes

    :param level: Logging level to output to log file.
    :param iterations: Number of log files to keep.
    :param project_name: Name of the project. (Used as the log file name)
    :param mode: Mode to open the log file. (Default: "w")
    :param format: Log Format (Default: "%(asctime)s:%(levelname)s:%(module)s:%(filename)s:%(lineno)d:%(message)s")
    :return:
    """


    for i in range(iterations, 0, -1):
        if (application_path / (project_name + f"_{i}.log")).exists():
            (application_path / (project_name + f"_{i}.log")).rename((application_path / (project_name + f"_{i+1}.log")))
    if (application_path / (project_name + ".log")).exists():
        (application_path / (project_name + ".log")).rename((application_path / (project_name + "_1.log")))
    if (application_path / (project_name + f"_{iterations + 1}.log")).exists():
        (application_path / (project_name + f"_{iterations + 1}.log")).unlink()


    logging.basicConfig(
        filename=(application_path / (project_name + ".log")),
        level=level,
        format=format,
        filemode=mode
    )

    MIN_LEVEL = logging.DEBUG
    stdout_hdlr = logging.StreamHandler(sys.stdout)
    stderr_hdlr = logging.StreamHandler(sys.stderr)
    stdout_hdlr.setLevel(MIN_LEVEL)
    stderr_hdlr.setLevel(max(MIN_LEVEL, logging.WARNING))

    rootLogger = logging.getLogger()
    rootLogger.addHandler(stdout_hdlr)
    rootLogger.addHandler(stderr_hdlr)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)