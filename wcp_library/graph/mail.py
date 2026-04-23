"""
Microsoft Graph API - Mail Module

Provides functions for interacting with Microsoft Graph API mail resources,
including mailbox folder enumeration, message retrieval, attachment handling,
and email notification parsing. Intended for use within the wcp_library Graph
integration layer.

Functions are synchronous wrappers around Graph REST endpoints, with async
I/O used only where necessary (e.g., file writes). All functions accept a
pre-authenticated headers dict containing a valid Bearer token.

Typical usage:
    from wcp_library.graph import get_auth_headers
    from wcp_library.graph.mail import get_emails, get_attachments, save_attachment

    headers = get_auth_headers(...)
    emails = get_emails(headers, mailbox="user@example.com")
    attachments = get_attachments(headers, mailbox="user@example.com", message_id=emails[0]["id"])
    save_attachment(attachments[0], Path("/tmp/report.xlsx"))

API Reference:
    https://learn.microsoft.com/en-us/graph/api/resources/mail-api-overview

Dependencies:
    - aiofiles: Async file I/O for saving attachments
    - requests: Synchronous HTTP client for Graph API calls
    - wcp_library.graph: Shared constants (REQUEST_TIMEOUT) and auth utilities
"""

import asyncio
import base64
import logging
import os
from pathlib import Path
from typing import Tuple

import aiofiles
import requests

from wcp_library.graph import _request

logger = logging.getLogger(__name__)


# ----------------------------------- Mailbox Functions ----------------------------------- #
def get_mailbox_folders(
    headers: dict, mailbox: str, parent_folder_id: str | None = None
) -> list[dict]:
    """Lists mailbox folders from the user's mailbox using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/user-list-mailfolders

    :param headers: The headers containing the Authorization token.
    :return: A list of mailbox folder metadata as JSON objects.
    """
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox}/mailFolders"
    if parent_folder_id:
        url += f"/{parent_folder_id}/childFolders"

    try:
        response = _request("GET", url, headers)
        data = response.json()
        return data.get("value", [])
    except requests.RequestException as e:
        logger.error("Error retrieving mailbox folders: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return []


# ----------------------------------- Email Functions ----------------------------------- #


def parse_email_notification(notification: dict) -> Tuple[str, str]:
    """Parses the email notification from Microsoft Graph and returns the mailbox and message ID.

    :param notification: A JSON object containing the notification details.
    :return: The email notification as a JSON object.
    :raises ValueError: If the resource data is malformed
    """
    resource_data = notification.get("resource", "")
    parts = resource_data.split("/")

    if len(parts) < 4:
        raise ValueError(
            f"Malformed resource data: expected at least 4 parts, got {len(parts)} in '{resource_data}'"
        )

    return parts[1], parts[3]  # mailbox, message_id


def get_email_metadata(headers: dict, mailbox: str, message_id: str) -> dict | None:
    """Retrieves the email details from a Microsoft Graph API response.
    API Reference: https://learn.microsoft.com/en-us/graph/api/message-get

    :param headers: The headers containing the Authorization token.
    :param notification: The Microsoft Graph API response.
    :return: The email details as a JSON object.
    """
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox}/messages/{message_id}"
    try:
        response = _request("GET", url, headers)
        return response.json()
    except requests.RequestException as e:
        logger.error(
            "Error retrieving email metadata for message %s: %s", message_id, e
        )
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def get_emails(headers: dict, mailbox: str, folder_id: str | None = None) -> list[dict]:
    """Lists emails from the user's mailbox using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/user-list-messages

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
        response = _request("GET", url, headers)
        data = response.json()
        return data.get("value", [])
    except requests.RequestException as e:
        logger.error("Error retrieving emails from mailbox %s: %s", mailbox, e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return []


def get_attachments(headers: dict, mailbox: str, message_id: str) -> list[dict]:
    """Fetch attachments from Microsoft Graph and include name/extension info.
    API Reference: https://learn.microsoft.com/en-us/graph/api/message-list-attachments

    :param headers: The headers containing the Authorization token.
    :param response: The Microsoft Graph API response.
    :return: A list of dictionaries containing the attachment details.
    """
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox}/messages/{message_id}/attachments"
    try:
        resp = _request("GET", url, headers)
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
        logger.error("Error retrieving attachments for message %s: %s", message_id, e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return []


def save_attachment(source: dict | bytes, location: Path) -> None:
    """Saves an attachment to a file at the specified location.

    :param source (dict | bytes): A dictionary or bytes object containing the attachment details.
    :param location (Path): The path to save the attachment to.
    """

    async def _save(content_bytes: bytes, location: Path) -> None:
        async with aiofiles.open(location, "wb") as f:
            await f.write(content_bytes)
        logger.info("Saved attachment to %s", location)

    if isinstance(source, dict):
        content_bytes = base64.b64decode(source.get("contentBytes", b""))
    elif isinstance(source, bytes):
        content_bytes = source
    else:
        raise TypeError("Source must be bytes or dict with 'contentBytes'.")

    asyncio.run(_save(content_bytes, location))
