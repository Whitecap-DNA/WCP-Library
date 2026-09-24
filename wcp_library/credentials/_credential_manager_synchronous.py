import logging
from abc import ABC,abstractmethod

import requests
from yarl import URL

from wcp_library.credentials import (CredentialWriteError,
                                     MissingCredentialsError)

logger = logging.getLogger(__name__)


class CredentialManager(ABC):
    def __init__(self, api_key: str, password_list_id: int):
        self.password_url = URL("https://vault.wcap.ca/api/passwords/")
        self.api_key = api_key
        self.headers = {"APIKey": self.api_key, 'Reason': 'Python Script Access'}
        self._password_list_id = password_list_id

    def _get_credentials(self) -> dict:
        """
        Get all credentials from the password list

        :return: Dictionary of credentials
        """

        logger.debug("Getting credentials from Vault")
        url = (self.password_url / str(self._password_list_id)).with_query("QueryAll")

        try:
            response = requests.get(str(url), headers=self.headers, timeout=30)
            response.raise_for_status()
            passwords = response.json()
        except requests.Timeout:
            raise MissingCredentialsError(f"Timeout retrieving credentials from password list {self._password_list_id}")
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            raise MissingCredentialsError(f"HTTP error retrieving credentials: {status}")
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

    def _get_credential(self, password_id: int | str) -> dict:
        """
        Get a specific credential from the password list

        :param password_id:
        :return:
        """

        logger.debug(f"Getting credential with ID {password_id}")
        url = (self.password_url / str(password_id))

        try:
            response = requests.get(str(url), headers=self.headers, timeout=30)
            response.raise_for_status()
            password = response.json()
        except requests.Timeout:
            raise MissingCredentialsError(f"Timeout retrieving credential with ID {password_id}")
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            raise MissingCredentialsError(f"HTTP error retrieving credential {password_id}: {status}")
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
        logger.debug("Credential retrieved")
        return password_info

    def _publish_new_password(self, data: dict) -> bool:
        """
        Publish a new password to the password list

        :param data: The entry to create, as the vault API expects it.
        :return: True. Failure raises rather than being returned.
        :raises CredentialWriteError: If the vault rejects the new entry or the
            request fails.
        """

        try:
            response = requests.post(str(self.password_url), json=data, headers=self.headers, timeout=30)
        except requests.Timeout as e:
            raise CredentialWriteError(
                f"Timeout creating credentials for {data['UserName']}"
            ) from e
        except requests.RequestException as e:
            raise CredentialWriteError(
                f"Request error creating credentials for {data['UserName']}: {e}"
            ) from e

        if response.status_code != 201:
            raise CredentialWriteError(
                f"Failed to create new credentials for {data['UserName']}: "
                f"HTTP {response.status_code}"
            )

        logger.debug(f"New credentials for {data['UserName']} created")
        return True

    def get_credentials(self, username: str) -> dict:
        """
        Get the credentials for a specific username

        :param username:
        :return: Dictionary of credentials
        """

        logger.debug(f"Getting credentials for {username}")
        credentials = self._get_credentials()

        try:
            return_credential = credentials[username.lower()]
        except KeyError:
            raise MissingCredentialsError(f"Credentials for {username} not found in this Password List")
        logger.debug(f"Credentials for {username} retrieved")
        return return_credential

    def get_credential_from_id(self, password_id: int | str) -> dict:
        """
        Get the credentials for a specific password ID

        :param password_id:
        :return:
        """

        return self._get_credential(password_id)


    def update_credential(self, credentials_dict: dict) -> bool:
        """
        Update the credentials for a specific username

        Credentials dictionary must the same keys as the original dictionary from the get_credentials method

        The dictionary should be obtained from the get_credentials method and modified accordingly

        :param credentials_dict: The modified credential dictionary.
        :return: True. Failure raises rather than being returned.
        :raises MissingCredentialsError: If the existing entry cannot be
            read from the vault, or no entry matches the username.
        :raises CredentialWriteError: If the vault rejects the update or
            the request fails.
        """

        if "OTP" in credentials_dict:
            credentials_dict.pop("OTP")

        logger.debug(f"Updating credentials for {credentials_dict['UserName']}")
        url = (self.password_url / str(self._password_list_id)).with_query("QueryAll")

        try:
            response = requests.get(str(url), headers=self.headers, timeout=30)
            response.raise_for_status()
            passwords = response.json()
        except requests.Timeout:
            raise MissingCredentialsError(f"Timeout retrieving credentials from password list {self._password_list_id}")
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            raise MissingCredentialsError(f"HTTP error retrieving credentials: {status}")
        except ValueError as e:
            raise MissingCredentialsError(f"Invalid JSON response from vault: {e}")

        matching = [x for x in passwords if x['UserName'] == credentials_dict['UserName']]
        if not matching:
            raise MissingCredentialsError(
                f"Credentials for {credentials_dict['UserName']} not found in this Password List"
            )
        relevant_credential_entry = matching[0]
        for field in relevant_credential_entry['GenericFieldInfo']:
            if field['DisplayName'] in credentials_dict:
                credentials_dict[field['GenericFieldID']] = credentials_dict[field['DisplayName']]
                credentials_dict.pop(field['DisplayName'])

        try:
            response = requests.put(str(self.password_url), json=credentials_dict, headers=self.headers, timeout=30)
        except requests.RequestException as e:
            raise CredentialWriteError(
                f"Error updating credentials for {credentials_dict['UserName']}: {e}"
            ) from e

        if response.status_code != 200:
            raise CredentialWriteError(
                f"Failed to update credentials for {credentials_dict['UserName']}: "
                f"HTTP {response.status_code}"
            )

        logger.debug(f"Credentials for {credentials_dict['UserName']} updated")
        return True

    @abstractmethod
    def new_credentials(self, credentials_dict: dict) -> bool:
        raise NotImplementedError("Must override in child class")
