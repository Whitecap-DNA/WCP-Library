"""
Microsoft Graph API - Mail Module

Provides functions for interacting with Microsoft Graph API mail resources:
mailbox folder enumeration, message listing and retrieval, and attachment
listing and download. Intended for use within the wcp_library Graph
integration layer.

All functions are synchronous and accept a pre-authenticated headers dict
containing a valid Bearer token, or a ``GraphCredentials`` instance that
mints and re-mints one.

Trigger:
    Called directly from code that already holds Graph headers or
    credentials (a Kestra task, a scheduled job, a notification handler).
    Not a standalone entry point.

Output:
    JSON dicts/lists mirroring Graph's mail response payloads. For
    attachments, :func:`save_attachment` also writes decoded content to a
    file on disk.

Functional areas:

    Mailboxes:
        - Listing mail folders (top-level or child folders).

    Messages:
        - Listing messages in a mailbox or folder.
        - Retrieving a single message's metadata.

    Attachments:
        - Listing a message's attachments, with name/extension split out.
        - Saving an attachment's content to disk.

Typical usage:
    from wcp_library.graph import get_headers
    from wcp_library.graph.mail import get_emails, get_attachments, save_attachment

    headers = get_headers(...)
    emails = get_emails(headers, mailbox="user@example.com")
    attachments = get_attachments(headers, mailbox="user@example.com", message_id=emails[0]["id"])
    save_attachment(attachments[0], Path("/tmp/report.xlsx"))

API Reference:
    https://learn.microsoft.com/en-us/graph/api/resources/mail-api-overview

Dependencies:
    - wcp_library.graph: Shared constants (_GRAPH_ROOT) and auth/paging
      utilities (_iter_pages, _request); handles retries and token renewal.
"""

import base64
import logging
import os
from pathlib import Path

from wcp_library.graph import (_GRAPH_ROOT, GraphCredentials, _iter_pages,
                               _request)

logger = logging.getLogger(__name__)


# ----------------------------------- Mailbox Functions ----------------------------------- #


def get_mailbox_folders(
    headers: dict | GraphCredentials,
    mailbox: str,
    parent_folder_id: str | None = None,
    page_size: int | None = None,
) -> list[dict]:
    """Lists mailbox folders from the user's mailbox using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/user-list-mailfolders

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param mailbox: The user's mailbox.
    :param parent_folder_id: If given, lists the child folders of that folder
        instead of the top-level folders of the mailbox.
    :param page_size: Optional ``$top`` override for the number of folders per page.
    :return: A list of mailbox folder metadata as JSON objects.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/users/{mailbox}/mailFolders"
    if parent_folder_id:
        url += f"/{parent_folder_id}/childFolders"
    return _iter_pages(url, headers, page_size=page_size)


# ----------------------------------- Message Functions ----------------------------------- #


def get_email_metadata(
    headers: dict | GraphCredentials, mailbox: str, message_id: str
) -> dict:
    """Retrieves the details of a single email message.
    API Reference: https://learn.microsoft.com/en-us/graph/api/message-get

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param mailbox: The email address of the mailbox holding the message.
    :param message_id: The ID of the message to retrieve.
    :return: The email details as a JSON object.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/users/{mailbox}/messages/{message_id}"
    response = _request("GET", url, headers)
    return response.json()


def get_emails(
    headers: dict | GraphCredentials,
    mailbox: str,
    folder_id: str | None = None,
    page_size: int | None = None,
) -> list[dict]:
    """Lists emails from the user's mailbox using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/user-list-messages

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param mailbox: The user's mailbox.
    :param folder_id: The ID of the folder to list emails from. If None,
        lists from the root folder.
    :param page_size: Optional ``$top`` override for the number of emails per page.
    :return: A list of email metadata as JSON objects.
    :raises requests.RequestException: If any paged request fails (including
        after retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/users/{mailbox}"
    if folder_id:
        url += f"/mailFolders/{folder_id}"
    url += "/messages"
    return _iter_pages(url, headers, page_size=page_size)


# ----------------------------------- Attachment Functions ----------------------------------- #


def get_attachments(
    headers: dict | GraphCredentials,
    mailbox: str,
    message_id: str,
    page_size: int | None = None,
) -> list[dict]:
    """Lists a message's attachments, with name and extension split out.
    API Reference: https://learn.microsoft.com/en-us/graph/api/message-list-attachments

    Each returned object gains two extra keys, ``name_no_extension`` and
    ``extension`` (without the leading dot), derived from ``name``. These
    are absent from Graph's own payload and added here for convenience.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param mailbox: The user's mailbox.
    :param message_id: The ID of the message to fetch attachments for.
    :param page_size: Optional ``$top`` override for the number of attachments per page.
    :return: A list of attachment objects as JSON, each with
        ``name_no_extension`` and ``extension`` added.
    :raises requests.RequestException: If any paged request fails (including
        after retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/users/{mailbox}/messages/{message_id}/attachments"
    attachments = _iter_pages(url, headers, page_size=page_size)
    for attachment in attachments:
        name_no_extension, extension = os.path.splitext(attachment.get("name", ""))
        attachment["name_no_extension"] = name_no_extension
        attachment["extension"] = extension.lstrip(".")
    return attachments


def _extract_content_bytes(source: dict | bytes) -> bytes:
    """Resolves an attachment's raw content to bytes.

    :param source: A Graph fileAttachment object (must contain a
        base64-encoded "contentBytes" field) or raw bytes.
    :return: The decoded attachment content.
    :raises ValueError: If ``source`` is a dict with no
        "contentBytes" field. itemAttachment and referenceAttachment
        objects carry no inline content and cannot be saved this way.
    :raises TypeError: If ``source`` is neither a dict nor bytes.
    """
    if isinstance(source, bytes):
        return source

    if isinstance(source, dict):
        content_bytes = source.get("contentBytes")
        if content_bytes is None:
            raise ValueError(
                f"Attachment {source.get('name', '<unknown>')!r} has no "
                "'contentBytes' field. Only fileAttachment content can be "
                "saved this way."
            )
        return base64.b64decode(content_bytes)

    raise TypeError(f"source must be bytes or dict, got {type(source).__name__}")


def save_attachment(source: dict | bytes, location: Path) -> None:
    """Saves an attachment to a file at the specified location.

    :param source: A Graph attachment object, whose base64 ``contentBytes``
        field is decoded, or the raw attachment bytes.
    :param location: The path to write the attachment to.
    :raises ValueError: If ``source`` is a dict with no
        "contentBytes" field.
    :raises TypeError: If ``source`` is neither a dictionary nor bytes.
    """
    content_bytes = _extract_content_bytes(source)
    location.write_bytes(content_bytes)
    logger.debug("Saved attachment to %s", location)
