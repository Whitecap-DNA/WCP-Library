import ftplib
import logging
import re
from pathlib import Path
from typing import Optional

import ftputil

logger = logging.getLogger(__name__)


class FTP:
    def __init__(self, host: Optional[str]=None, port: Optional[int]=21, password_vault_dict: Optional[dict]=None):
        self.host: str = host if not password_vault_dict else password_vault_dict['Host']
        self.port: int = port if not password_vault_dict else password_vault_dict['Port']
        self._username: Optional[str] = None if not password_vault_dict else password_vault_dict['UserName']
        self._password: Optional[str] = None if not password_vault_dict else password_vault_dict['Password']

        self._ftp_factory: ftputil.session.session_factory = ftputil.session.session_factory(base_class=ftplib.FTP_TLS,
                                                                                             port=self.port,
                                                                                             encrypt_data_channel=True,
                                                                                             encoding="UTF-8")
        self.ftp_connection: Optional[ftputil.FTPHost] = None if not (self._username and self._password) \
            else ftputil.FTPHost(self.host, self._username, self._password, session_factory=self._ftp_factory)

    def login(self, username: str, password: str) -> None:
        """
        Login to the FTP server

        :param username:
        :param password:
        :return:
        """

        logger.debug(f"Logging into {self.host} with username {username}")
        self.host = ftputil.FTPHost(self.host, username, password, session_factory=self._ftp_factory)

    def download(self, remote_file: Path, local_file: Path) -> None:
        """
        Download a file from the FTP server

        :param remote_file:
        :param local_file:
        :return:
        """

        local_file.parent.mkdir(parents=True, exist_ok=True)

        logger.debug(f"Downloading {remote_file} to {local_file}")
        self.ftp_connection.download(remote_file, local_file)

    def download_files(self, local_dir: Path, regex_pattern: str='*') -> None:
        """
        Download files from the FTP server matching the regex pattern

        :param local_dir:
        :param regex_pattern:
        :return:
        """

        local_dir.mkdir(parents=True, exist_ok=True)

        logger.debug(f"Downloading files from FTP server matching {regex_pattern} to {local_dir}")
        files = self.list_files()
        for file in files:
            if re.match(regex_pattern, file.name):
                logger.debug(f"Downloading {file} to {local_dir / file.name}")
                self.ftp_connection.download(file, local_dir / file.name)

    def list_files(self) -> list[Path]:
        """
        List files on the FTP server

        Returns a list of file paths

        :return:
        """

        logger.debug(f"Listing files on {self.ftp_connection.curdir}")
        return [Path(x) for x in self.ftp_connection.listdir(self.ftp_connection.curdir) if self.ftp_connection.path.isfile(x)]

    def list_dirs(self) -> list[Path]:
        """
        List directories on the FTP server

        Returns a list of directory paths

        :return:
        """

        logger.debug(f"Listing directories on {self.ftp_connection.curdir}")
        return [Path(x) for x in self.ftp_connection.listdir(self.ftp_connection.curdir) if self.ftp_connection.path.isdir(x)]

    def change_dir(self, remote_dir: Path) -> None:
        """
        Change the directory on the FTP server

        :param remote_dir:
        :return:
        """

        logger.debug(f"Changing FTP directory to {remote_dir}")
        self.ftp_connection.chdir(remote_dir)

    def upload(self, local_file: Path, remote_file: Path) -> None:
        """
        Upload a file to the FTP server

        :param local_file:
        :param remote_file:
        :return:
        """

        logger.debug(f"Uploading {local_file} to {remote_file}")
        self.ftp_connection.upload(local_file, remote_file)

    def close(self) -> None:
        """
        Close the FTP connection

        :return:
        """

        logger.debug(f"Closing FTP connection")
        self.ftp_connection.close()
        self.ftp_connection = None