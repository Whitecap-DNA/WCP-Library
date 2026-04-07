"""
Module for Microsoft Graph API authentication and configuration.
"""

from pathlib import Path

import requests

REQUEST_TIMEOUT = 30
RENEWAL_THRESHOLD = 60  # minutes


def get_headers(app_id: str, app_secret: str, tenant_id: str) -> dict:
    """Returns a dictionary containing the Authorization header with a Bearer token
    for use with Microsoft Graph API requests.

    :return: JSON: A dictionary containing the Authorization header with a Bearer token.
    """

    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "client_id": app_id,
        "client_secret": app_secret,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(
        token_url, data=data, headers=headers, timeout=REQUEST_TIMEOUT
    ).json()
    return {
        "Authorization": f"{response.get('token_type')} {response.get('access_token')}",
    }
