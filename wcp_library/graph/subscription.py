"""
Microsoft Graph API - Subscriptions Module

Provides functions for managing Microsoft Graph change notification subscriptions,
including creation, retrieval, renewal, reauthorization, recreation, and deletion.
Intended for use within the wcp_library Graph integration layer.

Subscriptions enable push notifications to a configured endpoint when changes occur
on a watched resource (e.g. new mail, SharePoint file updates, directory changes).
Each resource type has a Graph-enforced maximum lifetime; this module handles
expiration calculation automatically based on resource type.

Notification endpoints are derived from a base URL at creation time:
    - Change notifications:    {notification_url}/api/graph
    - Lifecycle notifications: {notification_url}/api/lifecycle

Expiration lifetimes by resource type (Graph-enforced maximums):
    - mail / calendar / contacts:   ~7 days  (10,060 min)
    - drive / sharepoint:           30 days  (42,300 min)
    - directory (users/groups):     29 days  (41,760 min)
    - teams / copilot:              3 days   ( 4,320 min)
    - security:                     30 days  (43,200 min)
    - print / todo:                 ~3 days  ( 4,230 min)
    - presence:                     1 hour   (    60 min)
    - default (fallback):           1 day    ( 1,440 min)

Resource type is inferred automatically from the resource path string when
renewing or recreating subscriptions (e.g. "users/.../messages" → "mail").

Encrypted resource data:
    App-only subscriptions on richer resources (e.g. Teams chat messages)
    require Graph to encrypt the resource data included in each notification.
    This requires ``includeResourceData=True`` plus a base64-encoded
    ``encryptionCertificate`` and its ``encryptionCertificateId``. See
    ``create_subscription`` for how to supply these.

Error handling contract:
    Every public function in this module makes a synchronous request via
    ``wcp_library.graph._request`` and does **not** catch
    ``requests.RequestException`` itself — the exception propagates to the
    caller after retries (if any) are exhausted by ``_request``. This is the
    behavior callers should rely on today. Note that this differs from the
    "returns None on exhausted retries" contract described in
    ``wcp_library.graph.retry``'s ``_GraphRetriable`` docstring; that
    docstring should be updated to match this module's actual behavior (or
    this module should be changed to match it) so the two don't drift apart.

Typical usage:
    from wcp_library.graph import get_auth_headers
    from wcp_library.graph.subscriptions import create_subscription, update_subscription_expiration

    headers = get_auth_headers(...)
    create_subscription(
        headers,
        notification_url="https://my-relay-endpoint.example.com",
        resource_type="mail",
        resource="users/user@example.com/messages",
        change_type="created",
        client_state="my-secret-state",
    )
    update_subscription_expiration(headers, subscription_id="abc-123")

API Reference:
    https://learn.microsoft.com/en-us/graph/api/resources/change-notifications-api-overview

Dependencies:
    - requests: Synchronous HTTP client for Graph API calls
    - wcp_library.graph: Shared constants (REQUEST_TIMEOUT, RENEWAL_THRESHOLD) and auth utilities
"""

import logging
from datetime import datetime, timedelta, timezone

import requests

from wcp_library.graph import _GRAPH_ROOT, RENEWAL_THRESHOLD, _request

logger = logging.getLogger(__name__)


def _iter_pages(url: str, headers: dict, page_size: int | None = None) -> list[dict]:
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


def create_subscription(
    headers: dict,
    notification_url: str,
    resource_type: str,
    resource: str,
    change_type: str,
    client_state: str,
    *,
    include_resource_data: bool = False,
    encryption_certificate: str | None = None,
    encryption_certificate_id: str | None = None,
) -> dict:
    """Creates a subscription to Microsoft Graph resources.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-post-subscriptions

    :param headers: The headers containing the Authorization token.
    :param notification_url: The URL to receive notifications.
    :param resource_type: The type of resource to subscribe to (e.g. "mail", "calendar",
        "contacts", "onedrive", "sharepoint", "directory", "teams", "presence", "print",
        "todo", "security", "copilot").
    :param resource: The resource to subscribe to.
    :param change_type: The type of change to subscribe to.
    :param client_state: A client-defined string that is sent with each notification.
    :param include_resource_data: Whether Graph should include the changed resource's
        data in the notification payload. Requires ``encryption_certificate`` and
        ``encryption_certificate_id`` to be supplied. Needed for app-only subscriptions
        on richer resources such as Teams chat messages.
    :param encryption_certificate: Base64-encoded public key certificate used by Graph
        to encrypt resource data before sending it in notifications. Required when
        ``include_resource_data`` is True.
    :param encryption_certificate_id: Client-defined identifier for
        ``encryption_certificate``, echoed back in notifications so the correct
        private key can be selected for decryption. Required when
        ``include_resource_data`` is True.
    :return: The created subscription as a JSON object.
    :raises ValueError: If ``include_resource_data`` is True but either
        ``encryption_certificate`` or ``encryption_certificate_id`` is missing.
    :raises requests.RequestException: If the HTTP request fails.
    """
    if include_resource_data and not (
        encryption_certificate and encryption_certificate_id
    ):
        raise ValueError(
            "encryption_certificate and encryption_certificate_id are required "
            "when include_resource_data is True."
        )

    url = f"{_GRAPH_ROOT}/subscriptions"

    expiration_datetime = _calculate_expiration_datetime(resource_type)
    payload = {
        "changeType": change_type,
        "clientState": client_state,
        "resource": resource,
        "notificationUrl": f"{notification_url}/api/graph",
        "lifecycleNotificationUrl": f"{notification_url}/api/lifecycle",
        "expirationDateTime": expiration_datetime,
    }
    if include_resource_data:
        payload["includeResourceData"] = True
        payload["encryptionCertificate"] = encryption_certificate
        payload["encryptionCertificateId"] = encryption_certificate_id

    response = _request("POST", url, headers, json=payload)
    subscription = response.json()
    logger.info(
        "Subscription %s created for resource %s (expires %s)",
        subscription.get("id"),
        resource,
        subscription.get("expirationDateTime"),
    )
    return subscription


def get_subscription(headers: dict, subscription_id: str) -> dict:
    """Retrieves a subscription by ID.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-get

    :param headers: The headers containing the Authorization token.
    :param subscription_id: The ID of the subscription to retrieve.
    :return: A dictionary containing the subscription details.
    :raises requests.RequestException: If the HTTP request fails.
    """
    url = f"{_GRAPH_ROOT}/subscriptions/{subscription_id}"
    response = _request("GET", url, headers)
    return response.json()


def list_subscriptions(headers: dict, *, page_size: int | None = None) -> list[dict]:
    """List all active subscriptions for the authenticated client.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-list

    Follows ``@odata.nextLink`` to completion, so the full result set is
    returned even once the number of active subscriptions grows beyond a
    single page.

    :param headers: The headers containing the Authorization token.
    :param page_size: Optional ``$top`` override.
    :return: A list of dictionaries containing the subscriptions across all pages.
    :raises requests.RequestException: If any paged request fails (including
        after retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/subscriptions"
    return _iter_pages(url, headers, page_size=page_size)


def update_subscription_expiration(headers: dict, subscription_id: str) -> dict:
    """Renews a subscription by updating its expiration date time.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-update

    :param headers: The headers containing the Authorization token.
    :param subscription_id: The ID of the subscription to renew.
    :return: A dictionary containing the updated subscription details.
    :raises requests.RequestException: If the HTTP request fails.
    """
    subscription = get_subscription(headers, subscription_id)
    resource_type = _get_resource_type(subscription.get("resource", ""))
    expiration_datetime = _calculate_expiration_datetime(resource_type)

    url = f"{_GRAPH_ROOT}/subscriptions/{subscription_id}"
    body = {"expirationDateTime": expiration_datetime}

    response = _request("PATCH", url, headers, json=body)
    updated = response.json()
    logger.info(
        "Subscription %s renewed, new expiration %s",
        subscription_id,
        updated.get("expirationDateTime"),
    )
    return updated


def renew_expiring_subscriptions(
    headers: dict, threshold_minutes: int = RENEWAL_THRESHOLD
) -> list[dict]:
    """Renews every active subscription expiring within ``threshold_minutes``.

    Intended to be run on a schedule (e.g. a Kestra flow) so subscriptions are
    proactively renewed before Graph lets them lapse, rather than relying on
    lifecycle/reauthorization notifications alone.

    A failure renewing one subscription is logged and does not prevent the
    remaining subscriptions from being processed.

    :param headers: The headers containing the Authorization token.
    :param threshold_minutes: How far into the future to look for subscriptions
        that are about to expire. Defaults to ``RENEWAL_THRESHOLD``.
    :return: A list of the subscription objects that were successfully renewed.
    :raises requests.RequestException: If listing subscriptions fails (including
        after retries are exhausted). Failures renewing individual subscriptions
        are caught and logged rather than raised.
    """
    cutoff = datetime.now(timezone.utc) + timedelta(minutes=threshold_minutes)

    renewed: list[dict] = []
    for subscription in list_subscriptions(headers):
        expiration = subscription.get("expirationDateTime")
        subscription_id = subscription.get("id")
        if not expiration or not subscription_id:
            continue

        expires_at = datetime.fromisoformat(expiration.replace("Z", "+00:00"))
        if expires_at > cutoff:
            continue

        try:
            renewed.append(update_subscription_expiration(headers, subscription_id))
        except requests.RequestException:
            logger.exception(
                "Failed to renew subscription %s (expires %s)",
                subscription_id,
                expiration,
            )

    return renewed


def _calculate_expiration_datetime(resource_type: str) -> str:
    """Calculates the expiration date for a subscription in ISO 8601 format.

    :param resource_type: The type of resource to subscribe to (e.g. "mail", "calendar",
        "contacts", "onedrive", "sharepoint", "directory", "teams", "presence", "print",
        "todo", "security", "copilot").
    :return: The expiration date in ISO 8601 format.
    """
    lifetime_table = {
        "mail": 10_060,  # Outlook mail messages/events/contacts (7 days)
        "calendar": 10_060,  # Outlook calendar
        "contacts": 10_060,  # Outlook contacts
        "drive": 42_300,  # OneDrive / SharePoint driveItem (30 days)
        "sharepoint": 42_300,  # SharePoint lists
        "directory": 41_760,  # Users / Groups / Directory objects (29 days)
        "teams": 4_320,  # Teams channels, chatMessages (3 days)
        "presence": 60,  # Presence (1 hour)
        "print": 4_230,  # Print resources (≈3 days)
        "todo": 4_230,  # To Do tasks (≈3 days)
        "security": 43_200,  # Security alerts (30 days)
        "copilot": 4_320,  # Copilot AI interactions (3 days)
        "default": 1_440,  # Fallback = 1 day
    }

    minutes = lifetime_table.get(resource_type, lifetime_table["default"])
    return (
        (datetime.now(timezone.utc) + timedelta(minutes=minutes))
        .isoformat()
        .replace("+00:00", "Z")
    )


def _get_resource_type(resource: str) -> str:
    """Infers a subscription's resource type from its Graph resource path.

    :param resource: The Graph resource path (e.g. "users/{id}/messages").
    :return: One of the keys used by ``_calculate_expiration_datetime``'s
        lifetime table, or "default" if none match.
    """
    resource_mappings = {
        "messages": "mail",
        "events": "calendar",
        "contacts": "contacts",
        "drive": "drive",
        "sites": "sharepoint",
        "groups": "directory",
        "users": "directory",
        "teams": "teams",
        "chats": "teams",
        "presence": "presence",
        "print": "print",
        "todo": "todo",
        "security": "security",
        "copilot": "copilot",
    }

    for key, value in resource_mappings.items():
        if key in resource.lower():
            return value
    return "default"


def delete_subscription(headers: dict, subscription_id: str) -> None:
    """Deletes a subscription by ID.

    :param headers: The headers containing the Authorization token.
    :param subscription_id: The ID of the subscription to delete.
    :raises requests.RequestException: If the HTTP request fails.
    """
    url = f"{_GRAPH_ROOT}/subscriptions/{subscription_id}"
    _request("DELETE", url, headers)
    logger.info("Subscription %s has been deleted", subscription_id)


def reauthorize_subscription(headers: dict, subscription_id: str) -> None:
    """Reauthorizes a subscription by ID.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-reauthorize

    Graph returns ``204 No Content`` for this action (no response body), so
    unlike the other mutating functions in this module there is nothing
    meaningful to return.

    :param headers: The headers containing the Authorization token.
    :param subscription_id: The ID of the subscription to reauthorize.
    :raises requests.RequestException: If the HTTP request fails.
    """
    url = f"{_GRAPH_ROOT}/subscriptions/{subscription_id}/reauthorize"
    _request("POST", url, headers)
    logger.info("Subscription %s has been reauthorized", subscription_id)


def recreate_subscription(headers: dict, subscription_id: str) -> dict:
    """Recreates a subscription by ID.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-post-subscriptions

    :param headers: The headers containing the Authorization token.
    :param subscription_id: The ID of the subscription to recreate.
    :return: The newly created subscription as a JSON object.
    :raises requests.RequestException: If the HTTP request fails.
    """
    subscription = get_subscription(headers, subscription_id)
    resource = subscription.get("resource")
    return create_subscription(
        headers,
        subscription.get("notificationUrl"),
        _get_resource_type(resource),
        resource,
        subscription.get("changeType"),
        subscription.get("clientState"),
    )


def update_notification_url(
    headers: dict, subscription_id: str, new_notification_url: str
) -> dict:
    """Changes the notification URL of an existing subscription.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-update

    :param headers: The headers containing the Authorization token.
    :param subscription_id: The ID of the subscription to update.
    :param new_notification_url: The new notification URL to set.
    :return: The updated subscription as a JSON object.
    :raises requests.RequestException: If the HTTP request fails.
    """
    url = f"{_GRAPH_ROOT}/subscriptions/{subscription_id}"
    body = {"notificationUrl": new_notification_url}

    response = _request("PATCH", url, headers, json=body)
    logger.info("Subscription %s notification URL has been updated", subscription_id)
    return response.json()
