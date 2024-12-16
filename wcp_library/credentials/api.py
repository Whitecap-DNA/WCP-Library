import logging

from wcp_library.credentials._credential_manager_asynchronous import AsyncCredentialManager
from wcp_library.credentials._credential_manager_synchronous import CredentialManager

logger = logging.getLogger(__name__)


class APICredentialManager(CredentialManager):
    def __init__(self, passwordState_api_key: str):
        super().__init__(passwordState_api_key, 214)

    def new_credentials(self, credentials_dict: dict) -> bool:
        """
        Create a new credential entry

        Credentials dictionary can have the following keys:
            - Title
            - UserName
            - Password
            - API KEY
            - Authentication Header
            - URL

        :param credentials_dict:
        :return: True if successful, False otherwise
        """

        data = {
            "PasswordListID": self._password_list_id,
            "Title": credentials_dict['UserName'].upper() if "Title" not in credentials_dict else credentials_dict['Title'].upper(),
            "Notes": credentials_dict['Notes'] if 'Notes' in credentials_dict else None,
            "UserName": credentials_dict['UserName'],
            "Password": credentials_dict['Password'],
            "GenericField1": credentials_dict['API KEY'],
            "GenericField2": credentials_dict['Authentication Header'],
            "URL": credentials_dict['URL']
        }

        return self._publish_new_password(data)


class AsyncAPICredentialManager(AsyncCredentialManager):
    def __init__(self, passwordState_api_key: str):
        super().__init__(passwordState_api_key, 214)

    async def new_credentials(self, credentials_dict: dict) -> bool:
        """
        Create a new credential entry

        Credentials dictionary can have the following keys:
            - Title
            - UserName
            - Password
            - API KEY
            - Authentication Header
            - URL

        :param credentials_dict:
        :return:
        """

        data = {
            "PasswordListID": self._password_list_id,
            "Title": credentials_dict['UserName'].upper() if "Title" not in credentials_dict else credentials_dict['Title'].upper(),
            "Notes": credentials_dict['Notes'] if 'Notes' in credentials_dict else None,
            "UserName": credentials_dict['UserName'],
            "Password": credentials_dict['Password'],
            "GenericField1": credentials_dict['API KEY'],
            "GenericField2": credentials_dict['Authentication Header'],
            "URL": credentials_dict['URL']
        }

        return await self._publish_new_password(data)