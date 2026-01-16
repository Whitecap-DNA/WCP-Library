from datetime import datetime, timedelta, timezone

import requests

from wcp_library.graph import REQUEST_TIMEOUT


def create_subscription(
    headers: dict,
    notification_url: str,
    resource_type: str,
    resource: str,
    change_type: str,
    client_state: str,
) -> None:
    """Creates a subscription to Microsoft Graph resources.

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
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        print(f"Subscription created with ID: {data.get('id')}")
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")


def get_subscription(headers: dict, subscription_id: str) -> dict | None:
    """Retrieves a subscription by ID.

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to retrieve.
    :return: A dictionary containing the subscription details, or None if not found.
    :rtype: dict | None
    """
    url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def update_subscription_expiration(headers: dict, subscription_id: str) -> None:
    """Renews a subscription by updating its expiration date time.

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to renew.
    """
    subscription = get_subscription(headers, subscription_id)
    resource_type = _get_resource_type(subscription.get("resource", ""))
    expiration_datetime = _calculate_expiration_datetime(resource_type)

    url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
    body = {"expirationDateTime": expiration_datetime}

    try:
        response = requests.patch(
            url, headers=headers, json=body, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        print(
            f"Subscription {subscription_id} has been renewed until {expiration_datetime}"
        )
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")


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

    :param headers: The headers containing the Authorization token.
    :return: A list of dictionaries containing the subscriptions.
    :rtype: list[dict]
    """
    url = "https://graph.microsoft.com/v1.0/subscriptions"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json().get("value", [])
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def delete_subscription(headers: dict, subscription_id: str) -> None:
    """Deletes a subscription by ID.

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to delete.
    """
    url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
    try:
        response = requests.delete(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        print(f"Subscription {subscription_id} has been deleted")
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")


def reauthorize_subscription(headers: dict, subscription_id: str) -> None:
    """
    Reauthorizes a subscription by ID.

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to reauthorize.
    """

    url = (
        f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}/reauthorize"
    )
    try:
        response = requests.post(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        print(f"Subscription {subscription_id} has been reauthorized")
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")


def recreate_subscription(headers: dict, subscription_id: str) -> None:
    """Recreates a subscription by ID.

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
    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to update.
    :param new_notification_url (str): The new notification URL to set.
    """
    url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
    body = {"notificationUrl": new_notification_url}

    try:
        response = requests.patch(
            url, headers=headers, json=body, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        print(f"Subscription {subscription_id} notification URL has been updated")
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
