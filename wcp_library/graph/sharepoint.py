"""
Microsoft Graph API - SharePoint Module

Provides functions for interacting with Microsoft Graph API SharePoint resources,
covering site metadata resolution, drive file operations, and list/list item
management. Intended for use within the wcp_library Graph integration layer.

All functions are synchronous and accept a pre-authenticated headers dict
containing a valid Bearer token. File content inputs are normalized to bytes
internally, accepting bytes, bytearray, memoryview, or base64-encoded strings.

Functional areas:

    Sites:
        - Resolving site metadata and IDs from a SharePoint site home URL.

    Files (Drive Items):
        - Retrieving file metadata by site-relative path.
        - Uploading, downloading, moving, copying, renaming, and deleting files.
        - Conflict behavior on upload is configurable (rename, replace, fail).

    Lists:
        - Enumerating, creating, and deleting SharePoint lists.
        - Full CRUD operations on list items, with optional OData filtering.

Typical usage:
    from wcp_library.graph import get_auth_headers
    from wcp_library.graph.sharepoint import get_site_metadata, upload_file, create_list_item

    headers = get_auth_headers(...)
    site = get_site_metadata(headers, "https://contoso.sharepoint.com/sites/DataOps")
    upload_file(headers, site["id"], "/Shared Documents/Reports", "report.xlsx", content)
    create_list_item(headers, site["id"], list_id, {"Title": "Q3 Report", "Status": "Draft"})

API Reference:
    https://learn.microsoft.com/en-us/graph/api/resources/sharepoint

Dependencies:
    - requests: Synchronous HTTP client for Graph API calls
    - yarl: URL parsing for extracting host and path from SharePoint site URLs
    - wcp_library.graph: Shared constants (REQUEST_TIMEOUT) and auth utilities
"""

import base64
import logging
from pathlib import Path

import requests
from yarl import URL

from wcp_library.graph import _request

logger = logging.getLogger(__name__)

_GRAPH_ROOT = "https://graph.microsoft.com/v1.0"


def _drive_base(site_id: str, drive_id: str | None) -> str:
    """Return the Graph URL prefix for file operations.

    If ``drive_id`` is provided it wins over ``site_id`` (targets a specific
    document library). Otherwise the site's default drive is used.
    """
    if drive_id:
        return f"{_GRAPH_ROOT}/drives/{drive_id}"
    return f"{_GRAPH_ROOT}/sites/{site_id}/drive"


def _iter_pages(
    url: str,
    headers: dict,
    page_size: int | None = None,
) -> list[dict]:
    """GET ``url`` and follow ``@odata.nextLink`` until exhausted.

    Returns the concatenated ``value`` arrays from every page. Raises
    ``requests.RequestException`` on HTTP failure — callers wrap this in
    their own try/except to preserve their existing error-reporting style
    (some use ``print``, some use ``logger``).

    :param page_size: If given, appended as ``$top`` on the first request.
        Graph echoes this on subsequent ``@odata.nextLink`` URLs, so we only
        need to set it once.
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


# ----------------------------------- Site Functions ----------------------------------- #


def get_site_metadata(headers: dict, site_home_url: str) -> dict | None:
    """Retrieves the site ID from a SharePoint site URL (needs to be the home page)
    API Reference: https://learn.microsoft.com/en-us/graph/api/site-get

    :param headers: The headers containing the Authorization token.
    :param site_home_url: The URL of the SharePoint site.
    :return: The site metadata as a JSON object.
    """
    url = URL(site_home_url)
    url = f"{_GRAPH_ROOT}/sites/{url.host}:{url.path}"
    try:
        response = _request("GET", url, headers)
        return response.json()
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def get_drives(
    headers: dict,
    site_id: str,
    *,
    page_size: int | None = None,
) -> list[dict] | None:
    """List document libraries (drives) on a SharePoint site.

    API Reference: https://learn.microsoft.com/en-us/graph/api/drive-list

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param page_size: Optional ``$top`` override.
    :return: A list of drive metadata objects across all pages, or ``None``
        on error.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/drives"
    try:
        return _iter_pages(url, headers, page_size=page_size)
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def get_drive_id_by_name(
    headers: dict,
    site_id: str,
    drive_name: str,
) -> str | None:
    """Resolve a drive ID by display name. Case-sensitive exact match on
    ``name``. Returns ``None`` if the drive is not found or the request fails.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param drive_name: The display name of the drive.
    :return: The drive ID, or ``None`` if no match.
    """
    drives = get_drives(headers, site_id)
    if drives is None:
        return None
    for drive in drives:
        if drive.get("name") == drive_name:
            return drive.get("id")
    return None


# ----------------------------------- File Functions ----------------------------------- #


def list_folder(
    headers: dict,
    site_id: str,
    folder_path: str,
    *,
    drive_id: str | None = None,
    page_size: int | None = None,
) -> list | None:
    """Lists files in a SharePoint folder using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param folder_path: The folder path (e.g. "/Shared Documents/My Folder").
        Use ``"/"`` or ``""`` to list the root of the drive.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :param page_size: Optional ``$top`` value passed on the initial request
        to tune Graph's page size. Default: let Graph decide.
    :return: A list of file/folder metadata objects across all pages, or
        ``None`` on error.
    """
    base = _drive_base(site_id, drive_id)
    if folder_path in ("", "/"):
        url = f"{base}/root/children"
    else:
        url = f"{base}/root:{folder_path}:/children"
    try:
        return _iter_pages(url, headers, page_size=page_size)
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def get_file_metadata(
    headers: dict,
    site_id: str,
    file_path: str,
    *,
    drive_id: str | None = None,
) -> dict | None:
    """Retrieves the file metadata from a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-get
    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The path of the file (e.g. "/Shared Documents/My Folder/file.txt")
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :return: The file metadata as a JSON object.
    """
    url = f"{_drive_base(site_id, drive_id)}/root:{file_path}"
    try:
        response = _request("GET", url, headers)
        return response.json()
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def get_file_content(
    headers: dict,
    site_id: str,
    file_path: str,
    *,
    drive_id: str | None = None,
) -> bytes | None:
    """Retrieves the file content from a SharePoint site using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The path of the file (e.g. "/Shared Documents/My Folder/file.txt")
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :return: The file content as bytes.
    """
    url = f"{_drive_base(site_id, drive_id)}/root:{file_path}:/content"
    try:
        response = _request("GET", url, headers)
        return response.content
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def get_file_content_by_id(headers: dict, drive_id: str, item_id: str) -> bytes | None:
    """Retrieves the file content from a SharePoint site using the Microsoft Graph API
        with drive and item IDs (for files in personal OneDrive).

    :param headers: The headers containing the Authorization token.
    :param drive_id: The OneDrive drive ID.
    :param item_id: The OneDrive item ID.
    :return: The file content as bytes.
    """
    url = f"{_GRAPH_ROOT}/drives/{drive_id}/items/{item_id}/content"
    try:
        response = _request("GET", url, headers)
        return response.content
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def upload_file(
    headers: dict,
    site_id: str,
    file_path: str,
    filename: str,
    content: bytes | bytearray | memoryview | str,
    conflict_behavior: str = "rename",
    *,
    drive_id: str | None = None,
) -> dict | None:
    """Saves a file to a SharePoint site using the Microsoft Graph API.
    No need to create parent folders.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-put-content

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The location of the file to save (e.g. "Shared Documents/My Folder")
    :param filename: The name of the file to save.
    :param content: The file content as bytes, bytearray, memoryview,
        or base64-encoded string (from Graph API).
    :param conflict_behavior: The behavior when a file with the same name already exists.
        Options are "rename", "replace", or "fail". Default is "rename".
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :return: The response from the Microsoft Graph API as a JSON object.
    """
    url = (
        f"{_drive_base(site_id, drive_id)}/root:"
        f"{file_path}/{filename}:/content"
        f"?@microsoft.graph.conflictBehavior={conflict_behavior}"
    )
    try:
        response = _request("PUT", url, headers, data=_ensure_bytes(content))
        json_response = response.json()
        parent_path = json_response.get("parentReference", {}).get("path", "")
        logger.info("%s has been uploaded to: %s", filename, parent_path)
        return json_response
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def _ensure_bytes(content: bytes | bytearray | memoryview | str) -> bytes:
    if isinstance(content, bytes):
        return content
    if isinstance(content, (bytearray, memoryview)):
        return bytes(content)
    if isinstance(content, str):
        return base64.b64decode(content)
    raise TypeError(f"Unsupported content type: {type(content).__name__}")


def download_file(
    headers: dict,
    site_id: str,
    file_path: str,
    download_folder: Path,
    *,
    drive_id: str | None = None,
) -> Path | None:
    """Downloads a file from a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-get-content

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The path of the file to download
        (e.g. "/Shared Documents/My Folder/file.txt")
    :param download_folder: The local folder path to save the file to. Defaults to current directory.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :return: The path to the downloaded file, or None if the download failed.
    """
    url = f"{_drive_base(site_id, drive_id)}/root:{file_path}:/content"
    try:
        response = _request("GET", url, headers)
        output_path = download_folder / Path(file_path).name
        output_path.write_bytes(response.content)
        return output_path
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def move_file(
    headers: dict,
    site_id: str,
    source_path: str,
    destination_path: str,
    new_filename: str | None = None,
    *,
    drive_id: str | None = None,
) -> dict | None:
    """Moves a file within a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-move

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param source_path: The current path of the file to move
        (e.g. "/Shared Documents/My Folder/file.txt")
    :param destination_path: The destination folder path (e.g. "/Shared Documents/Other Folder")
    :param new_filename: The new name for the file. If None, the original name is kept.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :return: The response from the Microsoft Graph API as a JSON object.
    """
    url = f"{_drive_base(site_id, drive_id)}/root:{source_path}"
    payload = _build_payload(destination_path, new_filename, drive_id=drive_id)
    try:
        response = _request(
            "PATCH",
            url,
            {**headers, "Content-Type": "application/json"},
            json=payload,
        )
        response_json = response.json()
        parent_path = response_json.get("parentReference", {}).get("path", "")
        logger.info(
            "%s has been updated to: %s/%s",
            source_path,
            parent_path,
            response_json.get("name", ""),
        )
        return response.json()
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def rename_file(
    headers: dict,
    site_id: str,
    file_path: str,
    new_filename: str,
    *,
    drive_id: str | None = None,
) -> dict | None:
    """Moves a file within a SharePoint site using the Microsoft Graph API
        (using the move_file function).
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-move

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The current path of the file to rename
        (e.g. "/Shared Documents/My Folder/file.txt")
    :param new_filename: The new name for the file.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :return: The response from the Microsoft Graph API as a JSON object.
    """
    return move_file(
        headers,
        site_id,
        file_path,
        destination_path=file_path,
        new_filename=new_filename,
        drive_id=drive_id,
    )


def copy_file(
    headers: dict,
    site_id: str,
    source_path: str,
    destination_path: str,
    new_filename: str | None = None,
    *,
    drive_id: str | None = None,
) -> dict | None:
    """Copies a file within a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-copy

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param source_path: The current path of the file to copy
        (e.g. "/Shared Documents/My Folder/file.txt")
    :param destination_path: The destination folder path
        (e.g. "/Shared Documents/Other Folder")
    :param new_filename: The new name for the copied file. If None, the original name is kept.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :return: The response from the Microsoft Graph API as a JSON object.
    """
    url = f"{_drive_base(site_id, drive_id)}/root:{source_path}:/copy"
    payload = _build_payload(destination_path, new_filename, drive_id=drive_id)
    try:
        response = _request(
            "POST",
            url,
            {**headers, "Content-Type": "application/json"},
            json=payload,
        )
        logger.info("%s has been copied to: %s", source_path, destination_path)
        return response.json()
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def _build_payload(
    destination_path: str | None,
    new_filename: str | None = None,
    drive_id: str | None = None,
) -> dict:
    if drive_id:
        parent_path = f"/drives/{drive_id}/root:{destination_path}"
    else:
        parent_path = f"/drive/root:{destination_path}"
    payload = {"parentReference": {"path": parent_path}}
    if new_filename:
        payload["name"] = new_filename
    return payload


def remove_file(
    headers: dict,
    site_id: str,
    file_path: str,
    *,
    drive_id: str | None = None,
) -> bool:
    """Removes a file from a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-delete

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The path of the file to remove
        (e.g. "/Shared Documents/My Folder/file.txt")
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :return: True if the file was removed successfully, False otherwise.
    """
    url = f"{_drive_base(site_id, drive_id)}/root:{file_path}"
    try:
        _request("DELETE", url, headers)
        logger.info("%s has been removed from SharePoint.", file_path)
        return True
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return False


# ----------------------------------- List Functions ----------------------------------- #


def get_lists(
    headers: dict,
    site_id: str,
    *,
    page_size: int | None = None,
) -> list[dict]:
    """Retrieves the lists from a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/list-list

    Follows ``@odata.nextLink`` to completion. Returns ``[]`` on error
    (unchanged contract).

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param page_size: Optional ``$top`` override.
    :return: A list of SharePoint lists as JSON objects across all pages.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists"
    try:
        return _iter_pages(url, headers, page_size=page_size)
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return []


def get_list_metadata(headers: dict, site_id: str, list_id: str) -> dict | None:
    """Retrieves the metadata of a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/list-get

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :return: The list metadata as a JSON object.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}"
    try:
        response = _request("GET", url, headers)
        return response.json()
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def create_list(
    headers: dict, site_id: str, list_name: str, list_template: str = "genericList"
) -> dict | None:
    """Creates a new SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/list-create

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_name: The name of the new SharePoint list.
    :param list_template: The template for the new SharePoint list. Default is "genericList".
    :return: The created list metadata as a JSON object.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists"
    payload = {"displayName": list_name, "list": {"template": list_template}}
    try:
        response = _request(
            "POST",
            url,
            {**headers, "Content-Type": "application/json"},
            json=payload,
        )
        return response.json()
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def remove_list(headers: dict, site_id: str, list_id: str) -> bool:
    """Removes a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/list-delete

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :return: True if the list was removed successfully, False otherwise.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}"
    try:
        _request("DELETE", url, headers)
        logger.info("List %s has been removed from site %s.", list_id, site_id)
        return True
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return False


def get_list_items(
    headers: dict,
    site_id: str,
    list_id: str,
    odata_filter: str | None = None,
    *,
    page_size: int | None = None,
) -> list[dict] | None:
    """Retrieves the items from a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-list

    Follows ``@odata.nextLink`` to completion.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param odata_filter: An optional OData filter string to filter the list items.
    :param page_size: Optional ``$top`` override.
    :return: A list of SharePoint list items as JSON objects across all pages,
        or ``None`` on error.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items"
    if odata_filter:
        url += f"?$filter={odata_filter}"
    try:
        return _iter_pages(url, headers, page_size=page_size)
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def get_list_item_metadata(
    headers: dict, site_id: str, list_id: str, item_id: str
) -> dict | None:
    """Retrieves the metadata of a SharePoint list item using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-get

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param item_id: The ID of the SharePoint list item.
    :return: The list item metadata as a JSON object.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items/{item_id}?expand=fields"
    try:
        response = _request("GET", url, headers)
        return response.json()
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def create_list_item(
    headers: dict, site_id: str, list_id: str, fields: dict
) -> dict | None:
    """Creates a new item in a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-create

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param fields: A dictionary containing the field values for the new list item.
    :return: The created list item metadata as a JSON object.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items"
    payload = {"fields": fields}
    try:
        response = _request(
            "POST",
            url,
            {**headers, "Content-Type": "application/json"},
            json=payload,
        )
        return response.json()
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def update_list_item(
    headers: dict, site_id: str, list_id: str, item_id: str, fields: dict
) -> dict | None:
    """Updates an existing item in a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-update

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param item_id: The ID of the SharePoint list item.
    :param fields: A dictionary containing the updated field values for the list item.
    :return: The updated list item metadata as a JSON object.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    try:
        response = _request(
            "PATCH",
            url,
            {**headers, "Content-Type": "application/json"},
            json=fields,
        )
        return response.json()
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def remove_list_item(headers: dict, site_id: str, list_id: str, item_id: str) -> bool:
    """Removes an item from a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-delete

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param item_id: The ID of the SharePoint list item.
    :return: True if the item was removed successfully, False otherwise.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items/{item_id}"
    try:
        _request("DELETE", url, headers)
        logger.info("Item %s has been removed from list %s.", item_id, list_id)
        return True
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return False
