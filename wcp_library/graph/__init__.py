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


from tenacity import retry as tenacity_retry

from wcp_library.retry import GRAPH_RETRIABLE_STATUSES, _GraphRetriable, graph_retry_kwargs


@tenacity_retry(**graph_retry_kwargs)
def _request(method: str, url: str, headers: dict, **kwargs) -> requests.Response:
    """Execute a Graph HTTP request with retry on 429/503/504 and network errors.

    Module-private. Callers in :mod:`wcp_library.graph.sharepoint`,
    :mod:`wcp_library.graph.mail`, :mod:`wcp_library.graph.subscription`
    invoke it instead of ``requests.*`` directly. ``timeout`` and
    ``raise_for_status()`` are handled here.

    :param method: HTTP verb ("GET", "POST", "PATCH", "PUT", "DELETE").
    :param url: absolute URL.
    :param headers: request headers (including Authorization).
    :param kwargs: forwarded to :func:`requests.request`
        (e.g. ``json=``, ``data=``).
    :raises requests.HTTPError: for non-retryable 4xx/5xx responses.
    :raises _GraphRetriable: for 429/503/504 or network errors when
        tenacity has exhausted its retry budget.
    :return: :class:`requests.Response` for status < 400 outside of the
        retryable set.
    """
    try:
        response = requests.request(
            method, url, headers=headers, timeout=REQUEST_TIMEOUT, **kwargs,
        )
    except (requests.ConnectionError, requests.Timeout) as e:
        raise _GraphRetriable(underlying=e) from e
    if response.status_code in GRAPH_RETRIABLE_STATUSES:
        raise _GraphRetriable(response=response)
    response.raise_for_status()
    return response
