"""
Module for Microsoft Graph API authentication and configuration.
"""

from pathlib import Path

import requests
from tenacity import retry as tenacity_retry

from wcp_library.credentials.internet import InternetCredentialManager
from wcp_library.retry import (GRAPH_RETRIABLE_STATUSES, _GraphRetriable,
                               graph_retry_kwargs)

_GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
REQUEST_TIMEOUT = 30  # seconds; override via set_request_timeout()
RENEWAL_THRESHOLD = 60  # minutes


class GraphAuthError(Exception):
    """Raised when Microsoft Graph token acquisition fails."""


def set_request_timeout(seconds: int | float) -> None:
    """Override the HTTP timeout used by every Graph helper.

    Default is 30 seconds. Call once at application startup to change
    it for all subsequent Graph requests (``get_headers``, every
    sharepoint/mail/subscription helper).

    :param seconds: positive timeout in seconds.
    :raises ValueError: if ``seconds`` is not strictly positive.
    """
    global REQUEST_TIMEOUT
    if seconds <= 0:
        raise ValueError(f"seconds must be positive, got {seconds!r}")
    REQUEST_TIMEOUT = seconds


def get_headers(app_id: str, app_secret: str, tenant_id: str) -> dict:
    """Returns a dictionary containing the Authorization header with a Bearer token
    for use with Microsoft Graph API requests.

    Authenticates against Azure AD using the client credentials flow and
    requests a token scoped to https://graph.microsoft.com/.default.

    Note: If you're retrieving the app registration's credentials from the
    Password State vault, use get_headers_from_vault() instead. It only
    requires the vault API key and credential ID, and internally resolves
    the app id, secret, and tenant id before calling this function.

    :param app_id: The Azure AD application (client) ID.
    :param app_secret: The Azure AD application client secret.
    :param tenant_id: The Azure AD tenant ID.
    :return: A dictionary containing the Authorization header with a Bearer token,
        e.g. {"Authorization": "Bearer <token>"}.
    """
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "client_id": app_id,
        "client_secret": app_secret,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
        response = requests.post(
            token_url, data=data, headers=headers, timeout=REQUEST_TIMEOUT
        )
    except requests.exceptions.RequestException as e:
        raise GraphAuthError(
            f"Network error while requesting token for app '{app_id}' "
            f"in tenant '{tenant_id}': {e}"
        ) from e

    try:
        payload = response.json()
    except ValueError as e:
        raise GraphAuthError(
            f"Token endpoint returned non-JSON response (HTTP {response.status_code}) "
            f"for app '{app_id}': {response.text[:500]}"
        ) from e

    if "access_token" not in payload:
        error_code = payload.get("error", "unknown_error")
        error_description = payload.get("error_description", "No description provided")
        raise GraphAuthError(
            f"Token acquisition failed for app '{app_id}' in tenant '{tenant_id}' "
            f"(HTTP {response.status_code}): {error_code} — {error_description}"
        )

    return {
        "Authorization": f"{payload.get('token_type', 'Bearer')} {payload.get('access_token')}",
    }


def get_headers_from_vault(api_key: str, credential_id: int | str) -> dict:
    """Returns a dictionary containing the Authorization header with a Bearer token
    for use with Microsoft Graph API requests, using credentials stored in
    Password State.

    This is a convenience wrapper around get_headers() for callers who don't
    want to manage the app id, secret, and tenant id directly. It fetches the
    app registration's credentials from the vault (via InternetCredentialManager),
    derives the tenant id from the credential's URL field, and delegates to
    get_headers() to perform the actual token request.

    :param api_key: The Password State API key used to authenticate with
        InternetCredentialManager.
    :param credential_id: The Password State credential ID for the app
        registration (int or str, will be coerced to int).
    :return: A dictionary containing the Authorization header with a Bearer token,
        e.g. {"Authorization": "Bearer <token>"}.
    """
    try:
        creds = InternetCredentialManager(api_key).get_credential_from_id(credential_id)
    except Exception as e:
        raise GraphAuthError(
            f"Failed to retrieve credential {credential_id} from PasswordState: {e}"
        ) from e

    tenant_id = creds["URL"].rstrip("/").rsplit("/", 1)[-1]
    return get_headers(creds["UserName"], creds["Password"], tenant_id)


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
            method,
            url,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            **kwargs,
        )
    except (requests.ConnectionError, requests.Timeout) as e:
        raise _GraphRetriable(underlying=e) from e
    if response.status_code in GRAPH_RETRIABLE_STATUSES:
        raise _GraphRetriable(response=response)
    response.raise_for_status()
    return response
