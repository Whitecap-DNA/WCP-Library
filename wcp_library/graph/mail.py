import asyncio
import base64
import os
from pathlib import Path
from typing import Tuple

import aiofiles
import requests

from wcp_library.graph import REQUEST_TIMEOUT


# ----------------------------------- Mailbox Functions ----------------------------------- #
def get_mailbox_folders(
    headers: dict, mailbox: str, parent_folder_id: str | None = None
) -> list[dict]:
    """Lists mailbox folders from the user's mailbox using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :return: A list of mailbox folder metadata as JSON objects.
    """
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox}/mailFolders"
    if parent_folder_id:
        url += f"/{parent_folder_id}/childFolders"

    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data.get("value", [])
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return []


# ----------------------------------- Email Functions ----------------------------------- #


def parse_email_notification(notification: dict) -> Tuple[str, str]:
    """Parses the email notification from Microsoft Graph and returns the mailbox and message ID.

    :param notification: A JSON object containing the notification details.
    :return: The email notification as a JSON object.
    """
    resource_data = notification.get("resource", "")
    parts = resource_data.split("/")
    return parts[1], parts[3]  # mailbox, message_id


def get_email_metadata(headers: dict, mailbox: str, message_id: str) -> dict | None:
    """Retrieves the email details from a Microsoft Graph API response.

    :param headers: The headers containing the Authorization token.
    :param notification: The Microsoft Graph API response.
    :return: The email details as a JSON object.
    """
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox}/messages/{message_id}"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def get_emails(headers: dict, mailbox: str, folder_id: str | None = None) -> list[dict]:
    """Lists emails from the user's mailbox using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param mailbox: The user's mailbox.
    :param folder_id: The ID of the folder to list emails from. If None, lists from the root folder.
    :return: A list of email metadata as JSON objects.
    """
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox}"
    if folder_id:
        url += f"/mailFolders/{folder_id}"
    url += "/messages"

    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data.get("value", [])
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return []


def get_attachments(headers: dict, mailbox: str, message_id: str) -> list[dict]:
    """Fetch attachments from Microsoft Graph and include name/extension info.

    :param headers: The headers containing the Authorization token.
    :param response: The Microsoft Graph API response.
    :return: A list of dictionaries containing the attachment details.
    """
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox}/messages/{message_id}/attachments"
    try:
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return [
            {
                **att,
                "name_no_extension": os.path.splitext(att.get("name", ""))[0],
                "extension": os.path.splitext(att.get("name", ""))[1].lstrip("."),
            }
            for att in data.get("value", [])
        ]
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return []


def save_attachment(source: dict | bytes, location: Path) -> None:
    """Saves an attachment to a file at the specified location.

    :param source (dict | bytes): A dictionary or bytes object containing the attachment details.
    :param location (Path): The path to save the attachment to.
    """

    async def _save(content_bytes: bytes, location: Path) -> None:
        async with aiofiles.open(location, "wb") as f:
            await f.write(content_bytes)
        print(f"Saved attachment to {location}")

    if isinstance(source, dict):
        content_bytes = base64.b64decode(source.get("contentBytes", b""))
    elif isinstance(source, bytes):
        content_bytes = source
    else:
        raise TypeError("Source must be bytes or dict with 'contentBytes'.")

    asyncio.run(_save(content_bytes, location))
