import logging
import re
import stat
from pathlib import Path
from typing import Optional

import paramiko

logger = logging.getLogger(__name__)


class SFTP:
    def __init__(self, host: Optional[str]=None, port: Optional[int]=21, password_vault_dict: Optional[dict]=None):
        self.host: str = host if not password_vault_dict else password_vault_dict['Host']
        self.port: int = port if not password_vault_dict else password_vault_dict['Port']
        self._username: Optional[str] = None if not password_vault_dict else password_vault_dict['UserName']
        self._password: Optional[str] = None if not password_vault_dict else password_vault_dict['Password']

        self.ssh = paramiko.SSHClient()
        # AutoAddPolicy automatically adds the hostname and new host key to the local HostKeys object
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if not (self._username and self._password):
            self.sftp_connection: Optional[paramiko.SFTP] = None
        else:
            self.ssh.connect(self.host, self.port, self._username, self._password)
            self.sftp_connection = self.ssh.open_sftp()

    def login(self, username: str, password: str) -> None:
        """
        Login to the SFTP server

        :param username:
        :param password:
        :return:
        """

        logger.debug(f"Logging into {self.host} with username {username}")
        self.ssh.connect(self.host, self.port, username, password)
        self.sftp_connection = self.ssh.open_sftp()

    def download(self, remote_file: Path, local_file: Path) -> None:
        """
        Download a file from the SFTP server

        :param remote_file:
        :param local_file:
        :return:
        """

        local_file.parent.mkdir(parents=True, exist_ok=True)

        logger.debug(f"Downloading {remote_file} to {local_file}")
        self.sftp_connection.get(str(remote_file), local_file)

    def download_files(self, local_dir: Path, regex_pattern: str='*') -> None:
        """
        Download files from the SFTP server matching the regex pattern

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
                self.sftp_connection.get(str(file), local_dir / file.name)

    def list_files(self) -> list[Path]:
        """
        List files on the SFTP server

        Returns a list of file paths

        :return:
        """

        logger.debug(f"Listing files on {self.sftp_connection.getcwd()}")
        return [Path(x) for x in self.sftp_connection.listdir() if stat.S_ISREG(self.sftp_connection.lstat(x).st_mode)]

    def list_dirs(self) -> list[Path]:
        """
        List directories on the SFTP server

        Returns a list of directory paths

        :return:
        """

        logger.debug(f"Listing directories on {self.sftp_connection.getcwd()}")
        return [Path(x) for x in self.sftp_connection.listdir() if stat.S_ISDIR(self.sftp_connection.lstat(x).st_mode)]

    def change_dir(self, remote_dir: Path) -> None:
        """
        Change the directory on the SFTP server

        :param remote_dir:
        :return:
        """

        logger.debug(f"Changing FTP directory to {remote_dir}")
        self.sftp_connection.chdir(str(remote_dir))

    def upload(self, local_file: Path, remote_file: Path) -> None:
        """
        Upload a file to the SFTP server

        :param local_file:
        :param remote_file:
        :return:
        """

        logger.debug(f"Uploading {local_file} to {remote_file}")
        self.sftp_connection.put(local_file, str(remote_file))

    def close(self):
        """
        Close the SFTP connection

        :return:
        """

        logger.debug("Closing FTP connection")
        self.sftp_connection.close()
        self.sftp_connection = None