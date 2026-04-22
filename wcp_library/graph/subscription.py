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
    - wcp_library.graph: Shared constants (REQUEST_TIMEOUT) and auth utilities
"""

import logging
from datetime import datetime, timedelta, timezone

import requests

from wcp_library.graph import REQUEST_TIMEOUT, _request

logger = logging.getLogger(__name__)


def create_subscription(
    headers: dict,
    notification_url: str,
    resource_type: str,
    resource: str,
    change_type: str,
    client_state: str,
) -> None:
    """Creates a subscription to Microsoft Graph resources.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-post-subscriptions

    :param headers: The headers containing the Authorization token.
    :param notification_url: The URL to receive notifications.
    :param resource_type (str): The type of resource to subscribe to (e.g. "mail", "calendar","contacts", "onedrive",
        "sharepoint", "directory", "teams", "presence", "print", "todo", "security", "copilot").
    :param resource: The resource to subscribe to.
    :param change_type (str): The type of change to subscribe to.
    :param client_state (str): A client-defined string that is sent with each notification.
    """
    url = "https://graph.microsoft.com/v1.0/subscriptions"

    expiration_datetime = _calculate_expiration_datetime(resource_type)
    payload = {
        "changeType": change_type,
        "clientState": client_state,
        "resource": resource,
        "notificationUrl": f"{notification_url}/api/graph",
        "lifecycleNotificationUrl": f"{notification_url}/api/lifecycle",
        "expirationDateTime": expiration_datetime,
    }

    try:
        response = _request(
            "POST",
            url,
            headers,
            json=payload,
        )
        data = response.json()
        logger.info("Subscription created with ID: %s", data.get("id"))
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)


def get_subscription(headers: dict, subscription_id: str) -> dict | None:
    """Retrieves a subscription by ID.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-get

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to retrieve.
    :return: A dictionary containing the subscription details, or None if not found.
    :rtype: dict | None
    """
    url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
    try:
        response = _request("GET", url, headers)
        return response.json()
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def update_subscription_expiration(headers: dict, subscription_id: str) -> None:
    """Renews a subscription by updating its expiration date time.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-update

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to renew.
    """
    subscription = get_subscription(headers, subscription_id)
    resource_type = _get_resource_type(subscription.get("resource", ""))
    expiration_datetime = _calculate_expiration_datetime(resource_type)

    url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
    body = {"expirationDateTime": expiration_datetime}

    try:
        response = _request(
            "PATCH", url, headers, json=body
        )
        logger.info(
            "Subscription %s has been renewed until %s",
            subscription_id,
            expiration_datetime,
        )
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)


def _calculate_expiration_datetime(resource_type: str) -> str:
    """Calculates the expiration date for a subscription in ISO 8601 format.

    :param resource: The resource to subscribe to (e.g. "mail", "calendar", "contacts", "onedrive",
        "sharepoint", "directory", "teams", "presence", "print", "todo", "security", "copilot").
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

    minutes = lifetime_table.get(resource_type)
    return (
        (datetime.now(timezone.utc) + timedelta(minutes=minutes))
        .isoformat()
        .replace("+00:00", "Z")
    )


def _get_resource_type(resource: str) -> str:
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


def list_subscriptions(headers: dict) -> list[dict] | None:
    """List all active subscriptions for the authenticated client.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-list

    :param headers: The headers containing the Authorization token.
    :return: A list of dictionaries containing the subscriptions.
    :rtype: list[dict]
    """
    url = "https://graph.microsoft.com/v1.0/subscriptions"
    try:
        response = _request("GET", url, headers)
        return response.json().get("value", [])
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def delete_subscription(headers: dict, subscription_id: str) -> None:
    """Deletes a subscription by ID.

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to delete.
    """
    url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
    try:
        response = _request("DELETE", url, headers)
        logger.info("Subscription %s has been deleted", subscription_id)
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)


def reauthorize_subscription(headers: dict, subscription_id: str) -> None:
    """
    Reauthorizes a subscription by ID.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-reauthorize

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to reauthorize.
    """

    url = (
        f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}/reauthorize"
    )
    try:
        response = _request("POST", url, headers)
        logger.info("Subscription %s has been reauthorized", subscription_id)
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)


def recreate_subscription(headers: dict, subscription_id: str) -> None:
    """Recreates a subscription by ID.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-post-subscriptions

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to recreate.
    """
    subscription = get_subscription(headers, subscription_id)
    create_subscription(
        headers,
        subscription.get("notificationUrl"),
        subscription.get("resource").split("/")[0],
        subscription.get("resource"),
        subscription.get("changeType"),
        subscription.get("clientState"),
    )


def update_notification_url(
    headers: dict, subscription_id: str, new_notification_url: str
) -> None:
    """Changes the notification URL of an existing subscription.
    API Reference: https://learn.microsoft.com/en-us/graph/api/subscription-update

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to update.
    :param new_notification_url (str): The new notification URL to set.
    """
    url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
    body = {"notificationUrl": new_notification_url}

    try:
        response = _request(
            "PATCH", url, headers, json=body
        )
        logger.info(
            "Subscription %s notification URL has been updated", subscription_id
        )
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
