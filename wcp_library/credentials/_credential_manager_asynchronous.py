import logging
from abc import ABC,abstractmethod

import aiohttp
from yarl import URL

from wcp_library.credentials import MissingCredentialsError

logger = logging.getLogger(__name__)


class AsyncCredentialManager(ABC):
    def __init__(self, api_key: str, password_list_id: int):
        self.password_url = URL("https://vault.wcap.ca/api/passwords/")
        self.api_key = api_key
        self.headers = {"APIKey": self.api_key, 'Reason': 'Python Script Access'}
        self._password_list_id = password_list_id

    async def _get_credentials(self) -> dict:
        """
        Get all credentials from the password list

        :return:
        """

        logger.debug("Getting credentials from Vault")
        url = (self.password_url / str(self._password_list_id)).with_query("QueryAll")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(str(url), headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    response.raise_for_status()
                    passwords = await response.json()
        except aiohttp.ClientError as e:
            raise MissingCredentialsError(f"Error retrieving credentials from password list {self._password_list_id}: {e}")
        except ValueError as e:
            raise MissingCredentialsError(f"Invalid JSON response from vault: {e}")

        if not passwords:
            raise MissingCredentialsError("No credentials found in this Password List")

        password_dict = {}
        for password in passwords:
            password_info = {'PasswordID': password['PasswordID'], 'UserName': password['UserName'], 'Password': password['Password']}
            for field in password['GenericFieldInfo']:
                password_info[field['DisplayName']] = field['Value'].lower() if field['DisplayName'].lower() == 'username' else field['Value']
            password_dict[password["UserName"].lower()] = password_info
            if "URL" in password:
                password_info['URL'] = password['URL']
            if password['OTP']:
                password_dict[password['UserName'].lower()]['OTP'] = password['OTP']
        logger.debug("Credentials retrieved")
        return password_dict

    async def _get_credential(self, password_id: int) -> dict:
        """
        Get a specific credential from the password list

        :param password_id:
        :return:
        """

        logger.debug(f"Getting credential with ID {password_id}")
        url = (self.password_url / str(password_id))

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(str(url), headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    response.raise_for_status()
                    password = await response.json()
        except aiohttp.ClientError as e:
            raise MissingCredentialsError(f"Error retrieving credential with ID {password_id}: {e}")
        except ValueError as e:
            raise MissingCredentialsError(f"Invalid JSON response from vault: {e}")

        if not password:
            raise MissingCredentialsError(f"No credentials found with ID {password_id}")
        password = password[0]

        password_info = {'PasswordID': password['PasswordID'], 'UserName': password['UserName'], 'Password': password['Password']}
        for field in password['GenericFieldInfo']:
            password_info[field['DisplayName']] = field['Value'].lower() if field['DisplayName'].lower() == 'username' else field['Value']
        if "URL" in password:
            password_info['URL'] = password['URL']
        if password['OTP']:
            password_info['OTP'] = password['OTP']
        return password_info

    async def _publish_new_password(self, data: dict) -> bool:
        """
        Publish a new password to the password list

        :param data:
        :return:
        """

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(str(self.password_url), json=data, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 201:
                        logger.debug(f"New credentials for {data['UserName']} created")
                        return True
                    else:
                        logger.error(f"Failed to create new credentials for {data['UserName']}: HTTP {response.status}")
                        return False
        except aiohttp.ClientError as e:
            logger.error(f"Error creating credentials for {data['UserName']}: {e}")
            return False

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

    async def get_credential_from_id(self, password_id: int) -> dict:
        """
        Get the credentials for a specific Password ID

        :param password_id:
        :return:
        """

        logger.debug(f"Getting credentials for ID {password_id}")
        credential = await self._get_credential(password_id)
        logger.debug(f"Credentials for ID {password_id} retrieved")
        return credential

    async def update_credential(self, credentials_dict: dict) -> bool:
        """
        Update username and password in Vault

        Credentials dictionary must the same keys as the original dictionary from the get_credentials method

        The dictionary can be obtained from the get_credentials method

        :param credentials_dict:
        :return:
        """

        if "OTP" in credentials_dict:
            credentials_dict.pop("OTP")

        logger.debug(f"Updating credentials for {credentials_dict['UserName']}")
        url = (self.password_url / str(self._password_list_id)).with_query("QueryAll")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(str(url), headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    response.raise_for_status()
                    passwords = await response.json()
        except aiohttp.ClientError as e:
            raise MissingCredentialsError(f"Error retrieving credentials from password list {self._password_list_id}: {e}")
        except ValueError as e:
            raise MissingCredentialsError(f"Invalid JSON response from vault: {e}")

        matching_credentials = [x for x in passwords if x['UserName'] == credentials_dict['UserName']]
        if not matching_credentials:
            raise MissingCredentialsError(f"Credentials for {credentials_dict['UserName']} not found in this Password List")
        relevant_credential_entry = matching_credentials[0]
        for field in relevant_credential_entry['GenericFieldInfo']:
            if field['DisplayName'] in credentials_dict:
                credentials_dict[field['GenericFieldID']] = credentials_dict[field['DisplayName']]
                credentials_dict.pop(field['DisplayName'])

        try:
            async with aiohttp.ClientSession() as session:
                async with session.put(str(self.password_url), json=credentials_dict, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        logger.debug(f"Credentials for {credentials_dict['UserName']} updated")
                        return True
                    else:
                        logger.error(f"Failed to update credentials for {credentials_dict['UserName']}: HTTP {response.status}")
                        return False
        except aiohttp.ClientError as e:
            logger.error(f"Error updating credentials for {credentials_dict['UserName']}: {e}")
            return False

    @abstractmethod
    async def new_credentials(self, credentials_dict: dict) -> bool:
        raise NotImplementedError("Must override in child class")
