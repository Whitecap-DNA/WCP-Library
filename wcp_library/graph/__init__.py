"""
Microsoft Graph API authentication and configuration.

This module gets bearer tokens for Microsoft Graph and sends Graph HTTP
requests. It also extracts resource identifiers from change and lifecycle
notifications.

Two ways to get a token:

* :func:`get_headers` -- one token from an app id, secret, and tenant id.
* :func:`get_headers_from_vault` -- one token from a Password State
  credential.

Both return a plain headers dictionary. A token expires after about an
hour, so a plain dictionary is only good for short-lived work.

For anything longer, use :class:`GraphCredentials` instead. It holds the
app registration, not a single token, and mints a new token when Graph
reports the old one expired::

    from wcp_library.graph import GraphCredentials
    from wcp_library.graph import sharepoint

    credentials = GraphCredentials.from_vault(api_key, credential_id)
    items = sharepoint.list_folder(credentials, site_id, folder)

    # Or, from an app registration you already hold:
    credentials = GraphCredentials.from_app_registration(
        app_id, app_secret, tenant_id
    )

Pass the ``credentials`` object anywhere a Graph helper takes ``headers``.
Every helper in :mod:`wcp_library.graph.sharepoint`,
:mod:`wcp_library.graph.mail`, and :mod:`wcp_library.graph.subscription`
re-mints the token on a 401 and retries the request once. For a Graph
call this library does not wrap, call it through
:meth:`GraphCredentials.call` instead::

    result = credentials.call(
        lambda headers: requests.get(url, headers=headers)
    )

``GraphCredentials`` instances are safe to share between threads.
"""

import logging
import re
import threading
from typing import Callable, TypeVar

import requests
from tenacity import Retrying
from tenacity import retry as tenacity_retry

from wcp_library.credentials.internet import InternetCredentialManager
from wcp_library.retry import (
    GRAPH_RETRIABLE_STATUSES,
    _GraphRetriable,
    graph_retry_kwargs,
    make_generic_retry,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_GRAPH_ROOT = "https://graph.microsoft.com/v1.0"
REQUEST_TIMEOUT = 30  # seconds; override via set_request_timeout()
RENEWAL_THRESHOLD = 60  # minutes

# The one status that means "this token is no longer valid", as opposed to
# "this token is not allowed to do that". A 403 is never retried: a
# permission the app registration lacks is refused identically with a new
# token, so retrying turns a configuration error into a slow one.
_EXPIRED = 401


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class GraphAuthError(Exception):
    """Raised when Microsoft Graph token acquisition fails."""


# ---------------------------------------------------------------------------
# Public configuration
# ---------------------------------------------------------------------------


def set_request_timeout(seconds: int | float) -> None:
    """Override the HTTP timeout used by every Graph helper.

    Default is 30 seconds. Call once at application startup to change
    it for all subsequent Graph requests (``get_headers``, every
    sharepoint/mail/subscription helper).

    :param seconds: positive timeout in seconds.
    :raises ValueError: if ``seconds`` is not strictly positive.
    """
    # pylint: disable=global-statement
    global REQUEST_TIMEOUT
    if seconds <= 0:
        raise ValueError(f"seconds must be positive, got {seconds!r}")
    REQUEST_TIMEOUT = seconds


# ---------------------------------------------------------------------------
# Token acquisition (low-level)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Credential lifecycle (auto-renewing token holder)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# HTTP transport (private)
# ---------------------------------------------------------------------------


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


def _iter_pages(
    url: str,
    headers: dict,
    page_size: int | None = None,
) -> list[dict]:
    """GET ``url`` and follow ``@odata.nextLink`` until exhausted.

    Returns the concatenated ``value`` arrays from every page.

    :param url: The initial URL to request.
    :param headers: The headers containing the Authorization token.
    :param page_size: If given, appended as ``$top`` on the first request.
        Graph echoes this on subsequent ``@odata.nextLink`` URLs, so it only
        needs to be set once.
    :return: The concatenated items across all pages.
    :raises requests.RequestException: If any paged request fails (including
        after retries are exhausted).
    """
    if page_size is not None:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}$top={page_size}"

    items: list[dict] = []
    next_url: str | None = url
    while next_url:
        response = _request("GET", next_url, headers)
        data = response.json()
        items.extend(data.get("value", []))
        next_url = data.get("@odata.nextLink")
    return items


# ---------------------------------------------------------------------------
# Notification resource-context extraction (private helpers)
# ---------------------------------------------------------------------------


def _matched_groups(match: re.Match | None) -> dict:
    """Returns a match's named groups, dropping any that didn't participate."""
    if match is None:
        return {}
    return {key: value for key, value in match.groupdict().items() if value}


_MAIL_PATTERN = re.compile(
    r"users/?\(?'?(?P<user_id>[^/')]+)'?\)?"
    r"(?:/mailfolders/?\(?'?(?P<mail_folder_id>[^/')]+)'?\)?)?"
    r"/messages/?\(?'?(?P<message_id>[^/')]+)'?\)?",
    re.IGNORECASE,
)


def _parse_mail(notification: dict) -> dict:
    """Extracts user, mail folder, and message IDs from a mail notification."""
    context = _matched_groups(_MAIL_PATTERN.search(notification.get("resource", "")))
    context.setdefault("message_id", notification.get("resourceData", {}).get("id"))
    return context


_CALENDAR_PATTERN = re.compile(
    r"users/?\(?'?(?P<user_id>[^/')]+)'?\)?"
    r"(?:/calendars/?\(?'?(?P<calendar_id>[^/')]+)'?\)?)?"
    r"/events/?\(?'?(?P<event_id>[^/')]+)'?\)?",
    re.IGNORECASE,
)


def _parse_calendar(notification: dict) -> dict:
    """Extracts user, calendar, and event IDs from a calendar notification."""
    context = _matched_groups(
        _CALENDAR_PATTERN.search(notification.get("resource", ""))
    )
    context.setdefault("event_id", notification.get("resourceData", {}).get("id"))
    return context


_CONTACTS_PATTERN = re.compile(
    r"users/?\(?'?(?P<user_id>[^/')]+)'?\)?"
    r"(?:/contactfolders/?\(?'?(?P<contact_folder_id>[^/')]+)'?\)?)?"
    r"/contacts/?\(?'?(?P<contact_id>[^/')]+)'?\)?",
    re.IGNORECASE,
)


def _parse_contacts(notification: dict) -> dict:
    """Extracts user, contact folder, and contact IDs from a contacts notification."""
    context = _matched_groups(
        _CONTACTS_PATTERN.search(notification.get("resource", ""))
    )
    context.setdefault("contact_id", notification.get("resourceData", {}).get("id"))
    return context


_SHAREPOINT_SITE_PATTERN = re.compile(
    r"sites/(?P<site_id>[^/]+)(?:/drives/(?P<drive_id>[^/]+))?",
    re.IGNORECASE,
)
_ONEDRIVE_PATTERN = re.compile(
    r"users/(?P<user_id>[^/]+)/drive|drives/(?P<drive_id>[^/]+)",
    re.IGNORECASE,
)

_DRIVE_ITEM_NOTE = (
    "driveItem notifications carry no item ID or name. Run a delta query "
    "on this drive to find what changed (see wcp_library.graph.sharepoint)."
)


def _parse_sharepoint(notification: dict) -> dict:
    """Extracts site/drive scope from a SharePoint driveItem notification.

    No item-level ID is available; see the module docstring.
    """
    context = _matched_groups(
        _SHAREPOINT_SITE_PATTERN.search(notification.get("resource", ""))
    )
    context["item_id"] = None
    context["note"] = _DRIVE_ITEM_NOTE
    return context


def _parse_drive(notification: dict) -> dict:
    """Extracts drive scope from a OneDrive driveItem notification.

    No item-level ID is available; see the module docstring.
    """
    context = _matched_groups(
        _ONEDRIVE_PATTERN.search(notification.get("resource", ""))
    )
    context["item_id"] = None
    context["note"] = _DRIVE_ITEM_NOTE
    return context


_TEAMS_CHANNEL_PATTERN = re.compile(
    r"teams/?\(?'?(?P<team_id>[^/')]+)'?\)?"
    r"/channels/?\(?'?(?P<channel_id>[^/')]+)'?\)?"
    r"/messages/?\(?'?(?P<message_id>[^/')]+)'?\)?"
    r"(?:/replies/?\(?'?(?P<reply_id>[^/')]+)'?\)?)?",
    re.IGNORECASE,
)
_TEAMS_CHAT_PATTERN = re.compile(
    r"chats/?\(?'?(?P<chat_id>[^/')]+)'?\)?/messages/?\(?'?(?P<message_id>[^/')]+)'?\)?",
    re.IGNORECASE,
)


def _parse_teams(notification: dict) -> dict:
    """Extracts team/channel or chat IDs and message/reply IDs from a Teams notification."""
    resource = notification.get("resource", "")
    match = _TEAMS_CHAT_PATTERN.search(resource) or _TEAMS_CHANNEL_PATTERN.search(
        resource
    )
    context = _matched_groups(match)
    context.setdefault("message_id", notification.get("resourceData", {}).get("id"))
    return context


_PRESENCE_PATTERN = re.compile(
    r"presences/?\(?'?(?P<user_id>[^/')]+)'?\)?", re.IGNORECASE
)


def _parse_presence(notification: dict) -> dict:
    """Extracts the user ID from a presence notification.

    Path shape confirmed at the subscription level only; see the module
    docstring's confidence note.
    """
    context = _matched_groups(
        _PRESENCE_PATTERN.search(notification.get("resource", ""))
    )
    context.setdefault("user_id", notification.get("resourceData", {}).get("id"))
    return context


_PRINT_JOB_PATTERN = re.compile(
    r"printers/?\(?'?(?P<printer_id>[^/')]+)'?\)?/jobs/?\(?'?(?P<job_id>[^/')]+)'?\)?",
    re.IGNORECASE,
)
_PRINT_TASK_PATTERN = re.compile(
    r"printtaskdefinitions?/?\(?'?(?P<task_definition_id>[^/')]+)'?\)?"
    r"/tasks/?\(?'?(?P<task_id>[^/')]+)'?\)?",
    re.IGNORECASE,
)


def _parse_print(notification: dict) -> dict:
    """Extracts printer/job or task definition/task IDs from a print notification."""
    resource = notification.get("resource", "")
    match = _PRINT_JOB_PATTERN.search(resource) or _PRINT_TASK_PATTERN.search(resource)
    return _matched_groups(match)


_TODO_PATTERN = re.compile(
    r"users/?\(?'?(?P<user_id>[^/')]+)'?\)?"
    r"/todo/lists/?\(?'?(?P<list_id>[^/')]+)'?\)?"
    r"/tasks/?\(?'?(?P<task_id>[^/')]+)'?\)?",
    re.IGNORECASE,
)


def _parse_todo(notification: dict) -> dict:
    """Extracts user, list, and task IDs from a To Do notification.

    Path shape inferred from the To Do REST API's URL structure; see the
    module docstring's confidence note.
    """
    context = _matched_groups(_TODO_PATTERN.search(notification.get("resource", "")))
    context.setdefault("task_id", notification.get("resourceData", {}).get("id"))
    return context


def _parse_security(notification: dict) -> dict:
    """Extracts the alert ID from a security notification.

    Security alert subscriptions are tenant-wide; the resource path
    itself carries no ID, so this relies entirely on resourceData.
    """
    alert_id = notification.get("resourceData", {}).get("id")
    return {"alert_id": alert_id} if alert_id else {}


_COPILOT_PATTERN = re.compile(
    r"copilot/users/?\(?'?(?P<user_id>[^/')]+)'?\)?", re.IGNORECASE
)


def _parse_copilot(notification: dict) -> dict:
    """Extracts the user ID (if scoped to one) and interaction ID from a
    Copilot notification.

    Path shape confirmed at the subscription level only; see the module
    docstring's confidence note.
    """
    context = _matched_groups(_COPILOT_PATTERN.search(notification.get("resource", "")))
    interaction_id = notification.get("resourceData", {}).get("id")
    if interaction_id:
        context["interaction_id"] = interaction_id
    return context


_DIRECTORY_PATTERN = re.compile(
    r"(?P<collection>users|groups)(?:/?\(?'?(?P<object_id>[^/')]+)'?\)?)?",
    re.IGNORECASE,
)


def _parse_directory(notification: dict) -> dict:
    """Extracts the collection (users/groups) and object ID from a directory notification.

    Most directory subscriptions watch the whole "/users" or "/groups"
    collection, so object_id is often absent from the path and comes
    from resourceData instead.
    """
    context = _matched_groups(
        _DIRECTORY_PATTERN.search(notification.get("resource", ""))
    )
    context.setdefault("object_id", notification.get("resourceData", {}).get("id"))
    return context


def _parse_default(notification: dict) -> dict:
    """Fallback for an unrecognized resource type: returns the raw resource path."""
    return {"resource": notification.get("resource", "")}


_PARSERS: dict[str, Callable[[dict], dict]] = {
    "mail": _parse_mail,
    "calendar": _parse_calendar,
    "contacts": _parse_contacts,
    "drive": _parse_drive,
    "sharepoint": _parse_sharepoint,
    "directory": _parse_directory,
    "teams": _parse_teams,
    "presence": _parse_presence,
    "print": _parse_print,
    "todo": _parse_todo,
    "security": _parse_security,
    "copilot": _parse_copilot,
    "default": _parse_default,
}


# ---------------------------------------------------------------------------
# Notification resource-context extraction (public entry point)
# ---------------------------------------------------------------------------


def get_resource_context(notification: dict) -> dict:
    """Extracts the identifiers needed to act on the resource behind a notification.

    Classifies the notification's resource type via
    ``wcp_library.graph.subscription.get_resource_type``, then runs the
    matching parser for that type.

    :param notification: One entry from a change or lifecycle notification
        payload (an item from the "value" list).
    :return: A dict of identifiers specific to the resource type, plus
        "resource_type" and "subscription_id". Field names vary by type
        (e.g. "message_id" for mail, "site_id"/"drive_id" for SharePoint).
        A field is omitted rather than set to None when it could not be
        determined, except where a type's parser documents otherwise
        (e.g. SharePoint's "item_id", always None by design).
    """
    # Imported here, not at module load time: wcp_library.graph.subscription
    # imports _GRAPH_ROOT, RENEWAL_THRESHOLD, and _request back from this
    # module. Importing it at the top of this file, before those names are
    # defined below, is a circular import that fails immediately on
    # "import wcp_library.graph" (subscription.py's own import statement
    # can't find them yet). Deferring the import to first use, once this
    # module has finished initializing, avoids the ordering dependency.
    # pylint: disable=import-outside-toplevel
    from wcp_library.graph.subscription import get_resource_type

    resource = notification.get("resource", "")
    resource_type = get_resource_type(resource)
    parser = _PARSERS.get(resource_type, _parse_default)

    context = parser(notification)
    context["resource_type"] = resource_type
    context["subscription_id"] = notification.get("subscriptionId")
    return context
