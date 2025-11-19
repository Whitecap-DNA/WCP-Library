import json
from datetime import datetime, timedelta, timezone

import requests

from wcp_library.graph import REQUEST_TIMEOUT, STORE_PATH


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
        "notificationUrl": notification_url,
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

        _add_or_update_subscription_metadata(
            data.get("id"),
            {
                "resource_type": resource_type,
                "change_type": change_type,
                "notification_url": notification_url,
                "resource": resource,
                "expiration_datetime": expiration_datetime,
                "clientState": client_state,
            },
        )
        print(f"Subscription created with ID: {data.get('id')}")
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")


def update_subscription_expiration(headers: dict, subscription_id: str) -> None:
    """Renews a subscription by updating its expiration date time.

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to renew.
    """
    subscription = get_subscription_metadata(subscription_id)

    if not subscription:
        print(f"Subscription {subscription_id} not found. Cannot renew.")
        return

    url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
    expiration_datetime = _calculate_expiration_datetime(
        subscription.get("resource_type", "default")
    )
    body = {"expirationDateTime": expiration_datetime}

    try:
        response = requests.patch(
            url, headers=headers, json=body, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        subscription["expiration_datetime"] = expiration_datetime
        _add_or_update_subscription_metadata(subscription_id, subscription)
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
        "mail": 10_069,  # Outlook mail messages/events/contacts (7 days)
        "calendar": 10_070,  # Outlook calendar
        "contacts": 10_070,  # Outlook contacts
        "onedrive": 42_300,  # OneDrive / SharePoint driveItem (30 days)
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

    minutes = lifetime_table.get(resource_type, lifetime_table.get("default"))
    return (
        (datetime.now(timezone.utc) + timedelta(minutes=minutes))
        .isoformat()
        .replace("+00:00", "Z")
    )


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
        _remove_subscription_from_file(subscription_id)
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
    """Recreates a stored subscription by ID.

    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to recreate.
    """
    subscription = get_subscription_metadata(subscription_id)
    if subscription:
        delete_subscription(headers, subscription_id)

        create_subscription(
            headers,
            subscription.get("notification_url"),
            subscription.get("resource_type"),
            subscription.get("resource"),
            subscription.get("change_type"),
            subscription.get("clientState"),
        )
        print(f"Subscription {subscription_id} has been recreated")
    else:
        print(f"Subscription {subscription_id} not found. Cannot be recreated")


def update_notification_url(
    headers: dict, subscription_id: str, new_notification_url: str
) -> None:
    """Changes the notification URL of an existing subscription.
    :param headers: The headers containing the Authorization token.
    :param subscription_id (str): The ID of the subscription to update.
    :param new_notification_url (str): The new notification URL to set.
    """
    subscription = get_subscription_metadata(subscription_id)
    url = f"https://graph.microsoft.com/v1.0/subscriptions/{subscription_id}"
    body = {"notificationUrl": new_notification_url}

    if subscription:
        try:
            response = requests.patch(
                url, headers=headers, json=body, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            print(f"Subscription {subscription_id} notification URL has been updated")
            subscription["notification_url"] = new_notification_url
            _add_or_update_subscription_metadata(subscription_id, subscription)
            print(f"Subscription {subscription_id} notification URL has been updated")
        except requests.RequestException as e:
            print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
    else:
        print(
            f"Subscription {subscription_id} not found. Cannot update notification URL."
        )


# --------------------- Subscription Storage Management ---------------------


def load_subscriptions_from_file() -> dict:
    """Load all stored subscriptions from the JSON file.

    :return: A dictionary containing all stored subscriptions.
    """
    return json.loads(STORE_PATH.read_text(encoding="utf-8"))


def get_subscription_metadata(subscription_id: str) -> dict:
    """Retrieve a subscription's metadata by its ID.

    :param subscription_id (str): The ID of the subscription to retrieve.
    :return: The subscription metadata as a dictionary, or None if not found.
    """
    return load_subscriptions_from_file().get(subscription_id) or {}


def get_subscription_differences(headers: dict) -> tuple[list[str], list[str]]:
    """
    Returns two lists:
    - Subscriptions in Microsoft Graph but not in the local file.
    - Subscriptions in the local file but not in Microsoft Graph.

    :param headers: The headers containing the Authorization token.
    :return: (graph_only, file_only)
    """
    saved_subscriptions = set(load_subscriptions_from_file().keys())
    graph_subscriptions = {
        subscription["id"] for subscription in list_subscriptions(headers)
    }
    return list(graph_subscriptions - saved_subscriptions), list(
        saved_subscriptions - graph_subscriptions
    )


def _save_subscriptions_to_file(subscriptions: dict) -> None:
    """Save the given subscriptions dictionary to the JSON file in a readable format.
    :param subscriptions (dict): A dictionary containing all subscriptions to save.
    """
    STORE_PATH.write_text(
        json.dumps(subscriptions, indent=2, sort_keys=True), encoding="utf-8"
    )


def _remove_subscription_from_file(subscription_id: str) -> None:
    """Remove a subscription from the store by its ID.

    :param subscription_id (str): The ID of the subscription to remove.
    """
    subscriptions = load_subscriptions_from_file()
    if subscriptions.pop(subscription_id, None):
        _save_subscriptions_to_file(subscriptions)


def _add_or_update_subscription_metadata(subscription_id: str, metadata: dict) -> None:
    """Add a new subscription or update an existing one by ID.

    :param subscription_id (str): The ID of the subscription to add or update.
    :param metadata (dict): The metadata of the subscription to add or update.
    """
    subscriptions = load_subscriptions_from_file()
    subscriptions[subscription_id] = metadata
    _save_subscriptions_to_file(subscriptions)
