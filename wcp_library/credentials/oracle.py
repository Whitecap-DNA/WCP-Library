import logging

import requests
from yarl import URL

from wcp_library.credentials import MissingCredentialsError

logger = logging.getLogger(__name__)


class OracleCredentialManager:
    def __init__(self, passwordState_api_key: str):
        self.password_url = URL("https://vault.wcap.ca/api/passwords/")
        self.api_key = passwordState_api_key
        self.headers = {"APIKey": self.api_key, 'Reason': 'Python Script Access'}
        self._password_list_id = 207

    def _get_credentials(self) -> dict:
        """
        Get all credentials from the password list

        :return: Dictionary of credentials
        """

        logger.debug("Getting credentials from PasswordState")
        url = (self.password_url / str(self._password_list_id)).with_query("QueryAll")
        passwords = requests.get(str(url), headers=self.headers).json()

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

    def update_credential(self, credentials_dict: dict) -> bool:
        """
        Update the credentials for a specific username

        Credentials dictionary must have the following keys:
            - PasswordID
            - UserName
            - Password

        The dictionary should be obtained from the get_credentials method and modified accordingly

        :param credentials_dict:
        :return: True if successful, False otherwise
        """

        logger.debug(f"Updating credentials for {credentials_dict['UserName']}")
        url = (self.password_url / str(self._password_list_id)).with_query("QueryAll")
        passwords = requests.get(str(url), headers=self.headers).json()

        relevant_credential_entry = [x for x in passwords if x['UserName'] == credentials_dict['UserName']][0]
        for field in relevant_credential_entry['GenericFieldInfo']:
            if field['DisplayName'] in credentials_dict:
                credentials_dict[field['GenericFieldID']] = credentials_dict[field['DisplayName']]
                credentials_dict.pop(field['DisplayName'])

        response = requests.put(str(self.password_url), json=credentials_dict, headers=self.headers)
        if response.status_code == 200:
            logger.debug(f"Credentials for {credentials_dict['UserName']} updated")
            return True
        else:
            logger.error(f"Failed to update credentials for {credentials_dict['UserName']}")
            return False

    def new_credentials(self, credentials_dict: dict) -> bool:
        """
        Create a new credential entry

        Credentials dictionary must have the following keys:
            - UserName
            - Password
            - Host
            - Port
            - Service or SID

        :param credentials_dict:
        :return: True if successful, False otherwise
        """

        data = {
            "PasswordListID": self._password_list_id,
            "Title": credentials_dict['UserName'].upper() if "Title" not in credentials_dict else credentials_dict['Title'].upper(),
            "Notes": credentials_dict['Notes'] if 'Notes' in credentials_dict else None,
            "UserName": credentials_dict['UserName'].lower(),
            "Password": credentials_dict['Password'],
            "GenericField1": credentials_dict['Host'],
            "GenericField2": credentials_dict['Port'],
            "GenericField3": credentials_dict['Service'] if 'Service' in credentials_dict else None,
            "GenericField4": credentials_dict['SID'] if 'SID' in credentials_dict else None
        }

        response = requests.post(str(self.password_url), json=data, headers=self.headers)
        if response.status_code == 201:
            logger.debug(f"New credentials for {credentials_dict['UserName']} created")
            return True
        else:
            logger.error(f"Failed to create new credentials for {credentials_dict['UserName']}")
            return False