"""
Module for Microsoft Graph API authentication and configuration.
"""

import logging
import threading
from typing import Callable, TypeVar

import requests
from tenacity import Retrying
from tenacity import retry as tenacity_retry

from wcp_library.credentials.internet import InternetCredentialManager
from wcp_library.retry import (GRAPH_RETRIABLE_STATUSES, _GraphRetriable,
                               graph_retry_kwargs, make_generic_retry)

logger = logging.getLogger(__name__)

_GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
REQUEST_TIMEOUT = 30  # seconds; override via set_request_timeout()
RENEWAL_THRESHOLD = 60  # minutes

# The one status that means "this token is no longer valid", as opposed to
# "this token is not allowed to do that". A 403 is never retried: a
# permission the app registration lacks is refused identically with a new
# token, so retrying turns a configuration error into a slow one.
_EXPIRED = 401


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


T = TypeVar("T")

# A re-mint sits on the critical path of every long-running job, not just its
# first second, so a transient failure at the vault or the token endpoint must
# not end the run. This also retries a permanently bad credential a few times
# before giving up, which is the price of not parsing Azure AD error codes.
_mint_retry_kwargs = make_generic_retry(GraphAuthError)


class GraphCredentials:
    """An app registration's identity, from which tokens are minted on demand.

    A token is valid for roughly an hour, so code holding one for longer
    breaks partway through its work. This class owns the credentials rather
    than a token, and re-mints when Graph reports the token expired.

    The dictionary handed out by :attr:`headers` is the same object for this
    object's whole life and is updated in place on a re-mint. Code that
    captured it earlier -- a closure, a queued coroutine, an
    ``asyncio.to_thread`` call already in flight -- therefore keeps working
    without being handed a replacement, and one wasted request per token
    lifetime replaces one per call.

    Pass an instance wherever a Graph helper takes ``headers`` and every
    request it makes gains expiry handling::

        credentials = GraphCredentials.from_vault(api_key, credential_id)
        items = sharepoint.list_folder(credentials, site_id, folder)

    Instances are safe to share between threads. Construct via
    :meth:`from_vault` or :meth:`from_app_registration` rather than calling
    the initialiser directly.
    """

    def __init__(self, token_source: Callable[[], dict]) -> None:
        """Mint the first token.

        Internal. Use :meth:`from_vault` or :meth:`from_app_registration`.

        :param token_source: callable returning fresh authorization headers.
        :raises GraphAuthError: if the token cannot be minted.
        """
        self._token_source = token_source
        self._lock = threading.Lock()
        self._generation = 0
        self._headers: dict = {}
        with self._lock:
            self._mint()

    @classmethod
    def from_vault(cls, api_key: str, credential_id: int | str) -> "GraphCredentials":
        """Build credentials from an app registration stored in the vault.

        The vault is re-read on every mint, so a rotated client secret is
        picked up by an already-running process instead of stranding it.

        :param api_key: The Password State API key used to authenticate with
            InternetCredentialManager.
        :param credential_id: The Password State credential ID for the app
            registration.
        :return: Credentials that mint tokens from the stored registration.
        :raises GraphAuthError: if the first token cannot be minted.
        """
        return cls(lambda: get_headers_from_vault(api_key, credential_id))

    @classmethod
    def from_app_registration(
        cls, app_id: str, app_secret: str, tenant_id: str
    ) -> "GraphCredentials":
        """Build credentials from an app registration held by the caller.

        For callers who already have these values and would otherwise call
        :func:`get_headers` directly.

        :param app_id: The Azure AD application (client) ID.
        :param app_secret: The Azure AD application client secret.
        :param tenant_id: The Azure AD tenant ID.
        :return: Credentials that mint tokens from these values.
        :raises GraphAuthError: if the first token cannot be minted.
        """
        return cls(lambda: get_headers(app_id, app_secret, tenant_id))

    @property
    def headers(self) -> dict:
        """The current authorization headers.

        Prefer passing the credentials object itself to a Graph helper, which
        handles expiry for you. Reach for this only when calling Graph outside
        this library, where nothing will re-mint on your behalf.

        :return: The live headers dictionary, updated in place on a re-mint.
        """
        return self._headers

    @property
    def generation(self) -> int:
        """How many tokens this object has minted.

        Captured before a request and handed back to :meth:`refresh`, so that
        concurrent callers who all saw the same expired token produce one
        re-mint between them rather than one each.

        :return: The current generation number.
        """
        return self._generation

    def _mint(self) -> None:
        """Replace the contents of the headers dictionary with a fresh token.

        The caller must hold ``self._lock``. Updated in place rather than
        rebound, and new keys are written before stale ones are removed, so a
        concurrent reader never sees a dictionary without an ``Authorization``
        header.

        :raises GraphAuthError: if the token cannot be minted, after retries.
        """
        fresh = Retrying(**_mint_retry_kwargs)(self._token_source)
        self._headers.update(fresh)
        for stale in set(self._headers) - set(fresh):
            del self._headers[stale]
        self._generation += 1

    def refresh(self, seen_generation: int) -> int:
        """Re-mint the token, unless another caller has already replaced it.

        :param seen_generation: The :attr:`generation` observed before the
            request that met an expired token.
        :return: The generation now in force.
        :raises GraphAuthError: if the token cannot be minted, after retries.
        """
        with self._lock:
            if seen_generation != self._generation:
                return self._generation
            logger.info(
                "Graph returned %d -- the access token has expired. "
                "Re-minting (generation %d).",
                _EXPIRED,
                self._generation + 1,
            )
            self._mint()
            return self._generation

    def call(self, operation: Callable[[dict], T]) -> T:
        """Run one Graph operation, re-minting the token if it has expired.

        An escape hatch for Graph calls this library does not wrap. Anything
        going through a helper in :mod:`wcp_library.graph.sharepoint`,
        :mod:`wcp_library.graph.mail` or :mod:`wcp_library.graph.subscription`
        needs no wrapping, because those handle expiry per request.

        ``operation`` must be safe to run twice. That holds for reads, for
        ``upload_file(conflict_behavior="replace")``, and effectively for
        ``remove_file``; nothing enforces it.

        :param operation: Callable taking the authorization headers and
            performing a single Graph request.
        :return: Whatever ``operation`` returns.
        :raises requests.HTTPError: if the retry also fails, or the status was
            anything other than 401.
        :raises GraphAuthError: if the token cannot be re-minted.
        """
        seen = self._generation
        try:
            return operation(self._headers)
        except requests.HTTPError as e:
            if e.response is None or e.response.status_code != _EXPIRED:
                raise
            self.refresh(seen)

        # Outside the handler so a failure here is not chained to the first
        # 401, which would make a genuine second failure read as the expiry.
        return operation(self._headers)


@tenacity_retry(**graph_retry_kwargs)
def _send(method: str, url: str, headers: dict, **kwargs) -> requests.Response:
    """Execute one Graph HTTP request, retrying 429/503/504 and network errors.

    Module-private transport. Callers go through :func:`_request`, which adds
    token-expiry handling on top. ``timeout`` and ``raise_for_status()`` are
    handled here.

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


def _merge_headers(headers: dict, extra: dict | None) -> dict:
    """Combine request headers with additions for a single call.

    Returns ``headers`` itself when there is nothing to add, so the common
    path passes the original dictionary straight through.

    :param headers: the base request headers.
    :param extra: headers to add, or None.
    :return: the headers to send.
    """
    if not extra:
        return headers
    return {**headers, **extra}


def _request(
    method: str,
    url: str,
    headers: dict | GraphCredentials,
    *,
    extra_headers: dict | None = None,
    **kwargs,
) -> requests.Response:
    """Execute a Graph HTTP request, re-minting the token if it has expired.

    Module-private. Callers in :mod:`wcp_library.graph.sharepoint`,
    :mod:`wcp_library.graph.mail`, :mod:`wcp_library.graph.subscription`
    invoke it instead of ``requests.*`` directly.

    Handed a plain dictionary this is exactly :func:`_send`. Handed a
    :class:`GraphCredentials` it retries once on HTTP 401 against a freshly
    minted token. The unit retried is the request rather than the calling
    operation, so a paginating helper resumes at the page it was on and a
    multi-file upload does not repeat the files that already landed.

    :param method: HTTP verb ("GET", "POST", "PATCH", "PUT", "DELETE").
    :param url: absolute URL.
    :param headers: request headers (including Authorization), or the
        credentials to mint them from.
    :param extra_headers: headers to add for this call only, such as a
        Content-Type. Merged at send time, so the retry that follows a
        re-mint carries the fresh token.
    :param kwargs: forwarded to :func:`requests.request`
        (e.g. ``json=``, ``data=``).
    :raises requests.HTTPError: for non-retryable 4xx/5xx responses, or for a
        401 that survives the re-mint.
    :raises _GraphRetriable: for 429/503/504 or network errors when
        tenacity has exhausted its retry budget.
    :raises GraphAuthError: if the token cannot be re-minted.
    :return: :class:`requests.Response` for status < 400 outside of the
        retryable set.
    """
    if not isinstance(headers, GraphCredentials):
        return _send(method, url, _merge_headers(headers, extra_headers), **kwargs)

    seen = headers.generation
    try:
        return _send(
            method, url, _merge_headers(headers.headers, extra_headers), **kwargs
        )
    except requests.HTTPError as e:
        if e.response is None or e.response.status_code != _EXPIRED:
            raise
        headers.refresh(seen)

    # Outside the handler so a failure here is not chained to the first 401,
    # which would make a genuine second failure read as the expiry.
    return _send(method, url, _merge_headers(headers.headers, extra_headers), **kwargs)
