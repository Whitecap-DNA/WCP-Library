import logging

from wcp_library.credentials import _common_entry
from wcp_library.credentials._credential_manager_asynchronous import AsyncCredentialManager
from wcp_library.credentials._credential_manager_synchronous import CredentialManager

logger = logging.getLogger(__name__)


def _entry(password_list_id: int, d: dict) -> dict:
    """
    Build the vault entry for a new credential.

    :param password_list_id: The vault password list the entry belongs to.
    :param d: The caller's credentials dictionary.
    :return: The entry, as the vault API expects it.
    """

    return _common_entry(password_list_id, d) | {
        "UserName": d['UserName'].lower(),
        "GenericField1": d['Host'],
        "GenericField2": d['Port'],
        "GenericField3": d['FTP/SFTP'],
    }


class FTPCredentialManager(CredentialManager):
    def __init__(self, api_key: str):
        super().__init__(api_key, 208)

    def new_credentials(self, credentials_dict: dict) -> bool:
        """
        Create a new credential entry

        Credentials dictionary must have the following keys:
            - UserName
            - Password
            - Host
            - Port
            - FTP/SFTP (FTP or SFTP)

        :param credentials_dict:
        :return: True. Failure raises rather than being returned.
        :raises CredentialWriteError: If the vault rejects the new entry or
            the request fails.
        """

        return self._publish_new_password(_entry(self._password_list_id, credentials_dict))


class AsyncFTPCredentialManager(AsyncCredentialManager):
    def __init__(self, api_key: str):
        super().__init__(api_key, 208)

    async def new_credentials(self, credentials_dict: dict) -> bool:
        """
        Create a new credential entry

        Credentials dictionary must have the following keys:
            - UserName
            - Password
            - Host
            - Port
            - FTP/SFTP (FTP or SFTP)

        :param credentials_dict:
        :return:
        """

        return await self._publish_new_password(_entry(self._password_list_id, credentials_dict))