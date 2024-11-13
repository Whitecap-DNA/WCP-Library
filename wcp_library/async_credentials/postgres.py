import logging

import aiohttp
from yarl import URL

from wcp_library.async_credentials import MissingCredentialsError

logger = logging.getLogger(__name__)


class AsyncPostgresCredentialManager:
    def __init__(self, passwordState_api_key: str):
        self.password_url = URL("https://vault.wcap.ca/api/passwords/")
        self.api_key = passwordState_api_key
        self.headers = {"APIKey": self.api_key, 'Reason': 'Python Script Access'}
        self._password_list_id = 207

    async def _get_credentials(self) -> dict:
        """
        Get all credentials from the password list

        :return:
        """

        logger.debug("Getting credentials from PasswordState")
        url = (self.password_url / str(self._password_list_id)).with_query("QueryAll")
        async with aiohttp.ClientSession() as session:
            async with session.get(str(url), headers=self.headers) as response:
                passwords = await response.json()

        if not passwords:
            raise MissingCredentialsError("No credentials found in this Password List")

        password_dict = {}
        for password in passwords:
            password_info = {'PasswordID': password['PasswordID'], 'UserName': password['UserName'], 'Password': password['Password']}
            for field in password['GenericFieldInfo']:
                password_info[field['DisplayName']] = field['Value'].lower() if field['DisplayName'].lower() == 'username' else field['Value']
            password_dict[password['UserName'].lower()] = password_info
        logger.debug("Credentials retrieved")
        return password_dict

    async def get_credentials(self, username: str) -> dict:
        """
        Get the credentials for a specific username

        :param username:
        :return:
        """

        logger.debug(f"Getting credentials for {username}")
        credentials = await self._get_credentials()

        try:
            return_credential = credentials[username.lower()]
        except KeyError:
            raise MissingCredentialsError(f"Credentials for {username} not found in this Password List")
        logger.debug(f"Credentials for {username} retrieved")
        return return_credential

    async def update_credential(self, credentials_dict: dict) -> bool:
        """
        Update username and password in PasswordState

        Credentials dictionary must have the following keys:
            - PasswordID
            - UserName
            - Password

        The dictionary can be obtained from the get_credentials method

        :param credentials_dict:
        :return:
        """

        logger.debug(f"Updating credentials for {credentials_dict['UserName']}")
        url = (self.password_url / str(self._password_list_id)).with_query("QueryAll")
        async with aiohttp.ClientSession() as session:
            async with session.get(str(url), headers=self.headers) as response:
                passwords = await response.json()

        relevant_credential_entry = [x for x in passwords if x['UserName'] == credentials_dict['UserName']][0]
        for field in relevant_credential_entry['GenericFieldInfo']:
            if field['DisplayName'] in credentials_dict:
                credentials_dict[field['GenericFieldID']] = credentials_dict[field['DisplayName']]
                credentials_dict.pop(field['DisplayName'])

        async with aiohttp.ClientSession() as session:
            async with session.put(str(self.password_url), json=credentials_dict, headers=self.headers) as response:
                if response.status == 200:
                    logger.debug(f"Credentials for {credentials_dict['UserName']} updated")
                    return True
                else:
                    logger.error(f"Failed to update credentials for {credentials_dict['UserName']}")
                    return False

    async def new_credentials(self, credentials_dict: dict) -> bool:
        """
        Create a new credential entry

        Credentials dictionary must have the following keys:
            - UserName
            - Password
            - Host
            - Port
            - Database

        :param credentials_dict:
        :return:
        """

        data = {
            "PasswordListID": self._password_list_id,
            "Title": credentials_dict['UserName'].upper() if "Title" not in credentials_dict else credentials_dict['Title'].upper(),
            "Notes": credentials_dict['Notes'] if 'Notes' in credentials_dict else None,
            "UserName": credentials_dict['UserName'].lower(),
            "Password": credentials_dict['Password'],
            "GenericField1": credentials_dict['Host'],
            "GenericField2": credentials_dict['Port'],
            "GenericField3": credentials_dict['Database']
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(str(self.password_url), json=data, headers=self.headers) as response:
                if response.status == 201:
                    logger.debug(f"New credentials for {credentials_dict['UserName']} created")
                    return True
                else:
                    logger.error(f"Failed to create new credentials for {credentials_dict['UserName']}")
                    return False
