"""
Microsoft Graph API - SharePoint Module

Provides functions for interacting with Microsoft Graph API SharePoint resources,
covering site metadata resolution, drive file operations, delta (change) tracking,
and list/list item management. Intended for use within the wcp_library Graph
integration layer.

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

    Delta:
        - Tracking changed drive items since a previous sync point.
        - Filtering changes down to file content changes only.

    Lists:
        - Enumerating, creating, and deleting SharePoint lists.
        - Full CRUD operations on list items, with optional OData filtering.

Typical usage:
    from wcp_library.graph import get_headers
    from wcp_library.graph.sharepoint import get_site_metadata, upload_file, create_list_item

    headers = get_headers(...)
    site = get_site_metadata(headers, "https://contoso.sharepoint.com/sites/DataOps")
    upload_file(headers, site["id"], "/Shared Documents/Reports", "report.xlsx", content)
    create_list_item(headers, site["id"], list_id, {"Title": "Q3 Report", "Status": "Draft"})

API Reference:
    https://learn.microsoft.com/en-us/graph/api/resources/sharepoint

Dependencies:
    - yarl: URL parsing for extracting host and path from SharePoint site URLs
    - wcp_library.graph: Shared constants (REQUEST_TIMEOUT) and auth utilities
"""

import base64
import logging
import os
import threading
from pathlib import Path

from yarl import URL

from wcp_library.graph import (_GRAPH_ROOT, GraphCredentials, _iter_pages,
                               _request)

logger = logging.getLogger(__name__)


# ----------------------------------- Site Functions ----------------------------------- #


def get_site_metadata(headers: dict | GraphCredentials, site_home_url: str) -> dict:
    """Retrieves the site ID from a SharePoint site URL (needs to be the home page).
    API Reference: https://learn.microsoft.com/en-us/graph/api/site-get

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_home_url: The URL of the SharePoint site.
    :return: The site metadata as a JSON object.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    site = URL(site_home_url)
    url = f"{_GRAPH_ROOT}/sites/{site.host}:{site.path}"
    response = _request("GET", url, headers)
    return response.json()


def get_drives(
    headers: dict | GraphCredentials,
    site_id: str,
    *,
    page_size: int | None = None,
) -> list[dict]:
    """List document libraries (drives) on a SharePoint site.

    API Reference: https://learn.microsoft.com/en-us/graph/api/drive-list

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param page_size: Optional ``$top`` override.
    :return: A list of drive metadata objects across all pages.
    :raises requests.RequestException: If any paged request fails (including
        after retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/drives"
    return _iter_pages(url, headers, page_size=page_size)


def get_drive_id_by_name(
    headers: dict | GraphCredentials,
    site_id: str,
    drive_name: str,
) -> str | None:
    """Resolve a drive ID by display name. Case-sensitive exact match on
    ``name``. Returns ``None`` if the drive is not found.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param drive_name: The display name of the drive.
    :return: The drive ID, or ``None`` if no match.
    :raises requests.RequestException: If the underlying drives fetch fails
        (including after retries are exhausted).
    """
    drives = get_drives(headers, site_id)
    for drive in drives:
        if drive.get("name") == drive_name:
            return drive.get("id")
    return None


def _drive_base(site_id: str | None, drive_id: str | None) -> str:
    """Return the Graph URL prefix for file operations.

    If ``drive_id`` is provided it wins over ``site_id`` (targets a specific
    document library directly, so ``site_id`` may be ``None``). Otherwise the
    site's default drive is used, and ``site_id`` is required.

    :param site_id: The ID of the SharePoint site, or ``None`` if ``drive_id``
        is given.
    :param drive_id: The document library (drive) ID, or ``None`` to use the
        site's default drive.
    :return: The Graph URL prefix to build item requests on.
    :raises ValueError: If both ``site_id`` and ``drive_id`` are ``None``.
    """
    if drive_id is not None:
        return f"{_GRAPH_ROOT}/drives/{drive_id}"
    if site_id is not None:
        return f"{_GRAPH_ROOT}/sites/{site_id}/drive"
    raise ValueError("Provide either site_id or drive_id.")


# ----------------------------------- Delta Functions ----------------------------------- #


def _get_delta(
    headers: dict | GraphCredentials,
    site_id: str | None = None,
    *,
    drive_id: str | None = None,
    delta_link: str | None = None,
    page_size: int | None = None,
) -> tuple[list[dict], str | None]:
    """Retrieves changed items in a drive since the last delta call.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-delta

    A driveItem change notification carries no item ID, name, or path -
    Graph returns null resource data for this resource. This is the
    function that fills that gap: call it once per notification (or on a
    schedule) to find out what actually changed.

    First call: omit ``delta_link``. Graph returns every item currently
    in the drive as a baseline, not a change set, plus a delta link.
    Store that link. On every later call, pass it back as ``delta_link``;
    Graph then returns only what changed since that link was issued.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly. Ignored if ``delta_link`` is given, since the
        link already encodes it.
    :param drive_id: Optional drive (document library) ID. If omitted,
        the site's default drive is used. Ignored if ``delta_link`` is
        given.
    :param delta_link: A previous call's returned delta link. Omit for
        the first call.
    :param page_size: Optional ``$top`` override, applied only on the
        first call (Graph echoes it on every ``@odata.nextLink`` after
        that). Ignored if ``delta_link`` is given.
    :return: A tuple of ``(items, delta_link)``. ``items`` is every
        changed item across all pages, each still carrying whatever
        facets Graph included (``file``, ``folder``, ``deleted``).
        ``delta_link`` is the link to pass on the next call; store it
        even when ``items`` is empty.
    :raises ValueError: If neither ``site_id`` nor ``drive_id`` is given
        and ``delta_link`` is not given either.
    :raises requests.RequestException: If any paged request fails
        (including after retries are exhausted).
    """
    if delta_link:
        url = delta_link
    else:
        url = f"{_drive_base(site_id, drive_id)}/root/delta"
        if page_size is not None:
            url = f"{url}?$top={page_size}"

    items: list[dict] = []
    next_url: str | None = url
    new_delta_link: str | None = None
    while next_url:
        response = _request("GET", next_url, headers)
        data = response.json()
        items.extend(data.get("value", []))
        next_url = data.get("@odata.nextLink")
        if next_url is None:
            new_delta_link = data.get("@odata.deltaLink")

    return items, new_delta_link


def get_changed_items(
    headers: dict | GraphCredentials,
    site_id: str | None = None,
    *,
    drive_id: str | None = None,
    delta_link: str | None = None,
    page_size: int | None = None,
) -> tuple[list[dict], str | None]:
    """Retrieves changed files since the last delta call.

    Thin filter over :func:`get_delta` for the common case: reacting to
    file content changes only. Folder items and deleted items are dropped
    from the result. Call :func:`get_delta` directly instead if folder
    changes or deletions also matter.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly. Ignored if ``delta_link`` is given.
    :param drive_id: Optional drive (document library) ID. If omitted,
        the site's default drive is used. Ignored if ``delta_link`` is
        given.
    :param delta_link: A previous call's returned delta link. Omit for
        the first call.
    :param page_size: Optional ``$top`` override, applied only on the
        first call. Ignored if ``delta_link`` is given.
    :return: A tuple of ``(files, delta_link)``, using the same delta
        link contract as :func:`get_delta`. ``files`` contains only
        items that carry a ``file`` facet and are not deletions.
    :raises requests.RequestException: If any paged request fails
        (including after retries are exhausted).
    """
    items, new_delta_link = _get_delta(
        headers,
        site_id,
        drive_id=drive_id,
        delta_link=delta_link,
        page_size=page_size,
    )
    files = [item for item in items if "file" in item and "deleted" not in item]
    return files, new_delta_link


# ----------------------------------- File Functions ----------------------------------- #


def list_folder(
    headers: dict | GraphCredentials,
    site_id: str | None,
    folder_path: str | None = None,
    *,
    drive_id: str | None = None,
    item_id: str | None = None,
    page_size: int | None = None,
) -> list[dict]:
    """Lists files in a SharePoint folder using the Microsoft Graph API.

    Two addressing modes are supported, both scoped to the given site
    unless ``drive_id`` is given directly:
    - **Path-based**: provide ``folder_path``.
    - **ID-based**: provide ``item_id``.

    If neither is given, the root of the drive is listed.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly.
    :param folder_path: The folder path (e.g. "/Shared Documents/My Folder").
        Use ``"/"`` or ``""`` to list the root of the drive. Ignored if
        ``item_id`` is given.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used. Valid in either mode.
    :param item_id: Optional folder ID. If given, ``folder_path`` is ignored.
    :param page_size: Optional ``$top`` value passed on the initial request
        to tune Graph's page size. Default: let Graph decide.
    :return: A list of file/folder metadata objects across all pages.
    :raises ValueError: If neither ``site_id`` nor ``drive_id`` is given.
    :raises requests.RequestException: If any paged request fails (including
        after retries are exhausted).
    """
    if item_id is not None:
        url = f"{_drive_base(site_id, drive_id)}/items/{item_id}/children"
    elif folder_path in ("", "/", None):
        url = f"{_drive_base(site_id, drive_id)}/root/children"
    else:
        url = f"{_drive_base(site_id, drive_id)}/root:{folder_path}:/children"

    return _iter_pages(url, headers, page_size=page_size)


def _resolve_item_url(
    site_id: str | None,
    file_path: str | None,
    drive_id: str | None,
    item_id: str | None,
    action: str | None = None,
) -> str:
    """Builds a Microsoft Graph driveItem URL for a SharePoint site item.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-get

    Two addressing modes are supported, both scoped to the given site
    unless ``drive_id`` is given directly:
    - **Path-based**: provide ``file_path``.
    - **ID-based**: provide ``item_id``.

    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly.
    :param file_path: The path of the file (e.g. "/Shared Documents/My Folder/file.txt").
        Required for path-based mode.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used. Valid in either mode.
    :param item_id: The ID of the file. Required for ID-based mode.
    :param action: Optional Graph API action to append (e.g. "content", "copy").
    :return: The full Graph API URL for the item, with the action appended if given.
    :raises ValueError: If neither ``file_path`` nor ``item_id`` is given.
    """
    if item_id is not None:
        url = f"{_drive_base(site_id, drive_id)}/items/{item_id}"
        return f"{url}/{action}" if action else url

    if file_path is not None:
        url = f"{_drive_base(site_id, drive_id)}/root:{file_path}"
        return f"{url}:/{action}" if action else url

    raise ValueError("Provide either file_path or item_id.")


def get_item_metadata(
    headers: dict | GraphCredentials,
    site_id: str | None,
    file_path: str | None,
    *,
    drive_id: str | None = None,
    item_id: str | None = None,
) -> dict:
    """Retrieves file metadata from a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-get

    If the item is a file, the returned metadata also has "name_no_extension"
    and "extension" keys, split from "name".

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly.
    :param file_path: The path of the file. Required for path-based mode
        (e.g. "/Shared Documents/My Folder/file.txt").
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :param item_id: The ID of the file. Required for ID-based mode.
    :return: The file metadata as a JSON object.
    :raises ValueError: If neither ``file_path`` nor ``item_id`` is given.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = _resolve_item_url(site_id, file_path, drive_id, item_id)
    response = _request("GET", url, headers)
    metadata = response.json()

    if "file" in metadata:
        name_no_extension, extension = os.path.splitext(metadata.get("name", ""))
        metadata["name_no_extension"] = name_no_extension
        metadata["extension"] = extension.lstrip(".")

    return metadata


def get_file_content(
    headers: dict | GraphCredentials,
    site_id: str | None,
    file_path: str | None,
    *,
    drive_id: str | None = None,
    item_id: str | None = None,
) -> bytes:
    """Retrieves file content from SharePoint or OneDrive via the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-get-content

    Two addressing modes are supported, both scoped to the given site
    unless ``drive_id`` is given directly:
    - **Path-based**: provide ``file_path``.
    - **ID-based**: provide ``item_id``.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly (for example, to address a OneDrive item by
        ``drive_id`` and ``item_id`` alone).
    :param file_path: The path of the file (e.g. "/Shared Documents/My Folder/file.txt").
        Required for path-based mode.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :param item_id: The ID of the file. Required for ID-based mode.
    :return: The file content as bytes.
    :raises ValueError: If neither ``file_path`` nor ``item_id`` is given.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = _resolve_item_url(site_id, file_path, drive_id, item_id, action="content")
    return _request("GET", url, headers).content


def download_file(
    headers: dict | GraphCredentials,
    site_id: str | None,
    file_path: str | None,
    download_folder: Path,
    *,
    drive_id: str | None = None,
    item_id: str | None = None,
    filename: str | None = None,
) -> Path:
    """Downloads a file from a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-get-content

    Two addressing modes are supported, both scoped to the given site
    unless ``drive_id`` is given directly:
    - **Path-based**: provide ``file_path``.
    - **ID-based**: provide ``item_id``.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly.
    :param file_path: The path of the file to download. Required for
        path-based mode (e.g. "/Shared Documents/My Folder/file.txt").
    :param download_folder: Local directory to save the downloaded file into.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :param item_id: The ID of the file. Required for ID-based mode.
    :param filename: The name to save the file as. If omitted and
        ``file_path`` is given, the name is taken from the last path
        segment (no extra request). If omitted in ID-based mode (no
        ``file_path``), a metadata request is made to look up the name.
    :return: The path to the downloaded file.
    :raises ValueError: If neither ``file_path`` nor ``item_id`` is given.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    content = get_file_content(
        headers, site_id, file_path, drive_id=drive_id, item_id=item_id
    )

    if filename is None:
        if file_path is not None:
            filename = Path(file_path).name
        else:
            metadata = get_item_metadata(
                headers, site_id, file_path, drive_id=drive_id, item_id=item_id
            )
            filename = metadata["name"]

    output_path = download_folder / filename
    output_path.write_bytes(content)
    logger.info("Downloaded %s to %s", filename, output_path)
    return output_path


def _ensure_bytes(content: bytes | bytearray | memoryview | str) -> bytes:
    """Normalizes file content to raw bytes.

    :param content: The file content as bytes, bytearray, memoryview, or a
        base64-encoded string (as returned by the Graph API).
    :return: The content as raw bytes.
    :raises TypeError: If ``content`` is not one of the supported types.
    """
    if isinstance(content, bytes):
        return content
    if isinstance(content, (bytearray, memoryview)):
        return bytes(content)
    if isinstance(content, str):
        return base64.b64decode(content)
    raise TypeError(f"Unsupported content type: {type(content).__name__}")


def upload_file(
    headers: dict | GraphCredentials,
    site_id: str | None,
    file_path: str | None,
    filename: str,
    content: bytes | bytearray | memoryview | str,
    conflict_behavior: str = "rename",
    *,
    drive_id: str | None = None,
    item_id: str | None = None,
) -> dict:
    """Uploads a file to a SharePoint site using the Microsoft Graph API.
    No need to create parent folders.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-put-content

    Two addressing modes are supported for the destination folder, both
    scoped to the given site unless ``drive_id`` is given directly:
    - **Path-based**: provide ``file_path``.
    - **ID-based**: provide ``item_id`` (the folder's ID).

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly.
    :param file_path: The destination folder path (e.g. "Shared Documents/My Folder").
        Required for path-based mode. Ignored if ``item_id`` is given.
    :param filename: The name of the file to save.
    :param content: The file content as bytes, bytearray, memoryview,
        or base64-encoded string (from Graph API).
    :param conflict_behavior: The behavior when a file with the same name already exists.
        Options are "rename" (default), "replace", or "fail".
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used. Valid in either mode.
    :param item_id: The ID of the destination folder. Required for ID-based
        mode.
    :return: The response from the Microsoft Graph API as a JSON object.
    :raises ValueError: If neither ``file_path`` nor ``item_id`` is given.
    :raises TypeError: If ``content`` is not bytes, bytearray, memoryview,
        or a base64-encoded string.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    if item_id is not None:
        # Graph's documented simple-upload form for creating a child of a
        # folder addressed by its ID: /items/{parent-id}:/{filename}:/content
        url = f"{_drive_base(site_id, drive_id)}/items/{item_id}:/{filename}:/content"
    elif file_path is not None:
        # Path addressing takes the full path of the item being written, so the
        # filename belongs inside the single ``root:...:`` segment. Closing the
        # path at the folder and opening a second segment for the filename is
        # not valid syntax and Graph answers 400. Built through
        # :func:`_resolve_item_url` so one function owns that shape.
        url = _resolve_item_url(
            site_id, f"{file_path}/{filename}", drive_id, None, action="content"
        )
    else:
        raise ValueError("Provide either file_path or item_id.")

    url = f"{url}?@microsoft.graph.conflictBehavior={conflict_behavior}"
    response = _request("PUT", url, headers, data=_ensure_bytes(content))
    json_response = response.json()
    parent_path = json_response.get("parentReference", {}).get("path", "")
    logger.info("%s has been uploaded to: %s", filename, parent_path)
    return json_response


def upload_multiple_files(
    headers: dict | GraphCredentials,
    site_id: str | None,
    files: list[tuple[str | None, str, bytes | bytearray | memoryview | str]],
    conflict_behavior: str = "rename",
    *,
    drive_id: str | None = None,
    item_id: str | None = None,
) -> list[dict]:
    """Uploads multiple files to the same SharePoint folder in parallel.

    Each file is uploaded with :func:`upload_file`, one thread per
    in-flight upload.

    Every file is attempted: all uploads are in flight before any
    result is known, so one failure cannot cancel the others. If any
    file fails, the successful responses are discarded and an
    ``ExceptionGroup`` of the failures is raised, each exception
    carrying a note naming its file. Re-running the whole batch is the
    intended recovery, and is safe with
    ``conflict_behavior="replace"``.

    Every exception is collected this way, not only the expected
    ``requests.RequestException`` (including one that survives all of
    ``_request``'s retries) and ``TypeError`` (an unsupported
    ``content`` type for one file). An exception left uncaught inside a
    worker thread never reaches the caller at all: it prints a
    traceback and leaves that file's entry empty, which is exactly the
    silent failure this contract exists to prevent.

    .. versionchanged:: 1.15.3
        Failures are raised as an ``ExceptionGroup`` again, and every
        exception type is collected rather than only
        ``RequestException`` and ``TypeError``. 1.15.1 and 1.15.2
        returned failures as ``{"filename": ..., "error": ...}``
        entries in the result list; 1.15.0 raised. See
        ``docs/adr/0003-upload-multiple-files-raises.md``.

    Two addressing modes are supported for the destination folder, both
    scoped to the given site unless ``drive_id`` is given directly, and
    shared by every file in the batch:
    - **Path-based**: give each tuple's ``file_path``.
    - **ID-based**: provide ``item_id`` (the folder's ID) once for the
        whole batch; each tuple's ``file_path`` is then ignored and may
        be ``None``.

    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-put-content

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly.
    :param files: A list of ``(file_path, filename, content)`` tuples. ``file_path`` is the
        destination folder path in SharePoint (e.g., "Shared Documents/My Folder"),
        or ``None`` if ``item_id`` is given for the batch.
        ``filename`` is the name of the file to be uploaded.
        ``content`` is bytes, bytearray, memoryview, or a base64-encoded string (from Graph API), as
        accepted by :func:`upload_file`.
    :param conflict_behavior: The behavior when a file with the same name already exists.
        Options are "rename" (default), "replace", or "fail". Applied to every file.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :param item_id: The ID of the destination folder, shared by every file
        in the batch. If given, each tuple's ``file_path`` is ignored.
    :return: A list of the Graph API responses, one per entry in ``files``,
        in the same order, as returned by :func:`upload_file`.
    :raises ExceptionGroup: If any upload failed. The group holds one
        exception per failed file, each carrying a note naming it.
    """
    responses: list[dict] = [{} for _ in files]
    failures: list[tuple[int, Exception]] = []

    def _upload_one(index: int, file_path: str | None, filename: str, content) -> None:
        try:
            responses[index] = upload_file(
                headers,
                site_id,
                file_path,
                filename,
                content,
                conflict_behavior,
                drive_id=drive_id,
                item_id=item_id,
            )
        except Exception as e:
            # Deliberately broad: anything not caught here dies with its
            # worker thread and never reaches the caller. Nothing is
            # swallowed, because every one is re-raised in the group below.
            destination = file_path if file_path is not None else f"item {item_id}"
            e.add_note(f"file: {destination}/{filename}")
            failures.append((index, e))

    threads = [
        threading.Thread(target=_upload_one, args=(index, file_path, filename, content))
        for index, (file_path, filename, content) in enumerate(files)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    if failures:
        failures.sort(key=lambda failure: failure[0])
        raise ExceptionGroup(
            f"{len(failures)} of {len(files)} uploads failed",
            [error for _, error in failures],
        )

    return responses


def _build_payload(
    destination_path: str | None,
    new_filename: str | None = None,
    drive_id: str | None = None,
    destination_id: str | None = None,
) -> dict:
    """Builds the payload for moving or renaming a file in SharePoint.

    :param destination_path: The path to the destination folder in SharePoint.
        This must be a folder path, not a file path.
    :param new_filename: The new name for the file, if renaming.
    :param drive_id: Optional drive (document library) ID.
    :param destination_id: The ID of the destination folder.
    :return: A dictionary representing the payload for the Graph API request.
    """
    payload: dict = {}

    if destination_id is not None:
        parent_reference: dict = {"id": destination_id}
        if drive_id is not None:
            parent_reference["driveId"] = drive_id
        payload["parentReference"] = parent_reference
    elif destination_path is not None:
        if drive_id is not None:
            parent_path = f"/drives/{drive_id}/root:{destination_path}"
        else:
            parent_path = f"/drive/root:{destination_path}"
        payload["parentReference"] = {"path": parent_path}

    if new_filename is not None:
        payload["name"] = new_filename
    return payload


def move_file(
    headers: dict | GraphCredentials,
    site_id: str | None,
    source_path: str | None,
    destination_path: str | None = None,
    new_filename: str | None = None,
    *,
    drive_id: str | None = None,
    item_id: str | None = None,
    destination_id: str | None = None,
) -> dict:
    """Moves a file within a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-move

    The source file can be addressed by path or by ID:
    - **Path-based**: provide ``source_path``.
    - **ID-based**: provide ``item_id``.

    The destination folder can likewise be given as a path or an ID:
    - **Path-based**: provide ``destination_path`` (a folder path, not a
        file path).
    - **ID-based**: provide ``destination_id``.

    If neither destination option is given, the file stays in its current
    folder (a rename-only move).

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly.
    :param source_path: The current path of the file to move
        (e.g. "/Shared Documents/My Folder/file.txt"). Required for
        path-based source addressing. Ignored if ``item_id`` is given.
    :param destination_path: The destination folder path (e.g. "/Shared Documents/Other Folder").
        Ignored if ``destination_id`` is given.
    :param new_filename: The new name for the file. If None, the original name is kept.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used. Valid for either addressing mode.
    :param item_id: The ID of the file to move. Required for ID-based
        source addressing.
    :param destination_id: The ID of the destination folder.
    :return: The response from the Microsoft Graph API as a JSON object.
    :raises ValueError: If neither ``source_path`` nor ``item_id`` is given.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    if item_id is not None:
        url = f"{_drive_base(site_id, drive_id)}/items/{item_id}"
    elif source_path is not None:
        url = f"{_drive_base(site_id, drive_id)}/root:{source_path}"
    else:
        raise ValueError("Provide either source_path or item_id.")

    payload = _build_payload(
        destination_path,
        new_filename,
        drive_id=drive_id,
        destination_id=destination_id,
    )
    response = _request(
        "PATCH",
        url,
        headers,
        extra_headers={"Content-Type": "application/json"},
        json=payload,
    )
    response_json = response.json()
    parent_path = response_json.get("parentReference", {}).get("path", "")
    logger.info(
        "%s has been updated to: %s/%s",
        source_path or item_id,
        parent_path,
        response_json.get("name", ""),
    )
    return response_json


def rename_file(
    headers: dict | GraphCredentials,
    site_id: str | None,
    file_path: str | None,
    new_filename: str,
    *,
    drive_id: str | None = None,
    item_id: str | None = None,
) -> dict:
    """Renames a file within a SharePoint site using the Microsoft Graph API
        (using the move_file function).
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-move

    The file can be addressed by path or by ID:
    - **Path-based**: provide ``file_path``.
    - **ID-based**: provide ``item_id``.

    The file's folder is left unchanged; only its name changes.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly.
    :param file_path: The current path of the file to rename
        (e.g. "/Shared Documents/My Folder/file.txt"). Required for
        path-based addressing. Ignored if ``item_id`` is given.
    :param new_filename: The new name for the file.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :param item_id: The ID of the file to rename. Required for ID-based
        addressing.
    :return: The response from the Microsoft Graph API as a JSON object.
    :raises ValueError: If neither ``file_path`` nor ``item_id`` is given.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    # No destination given: file_path is a full file path, not a folder
    # path, so it must not be passed as destination_path.
    return move_file(
        headers,
        site_id,
        file_path,
        new_filename=new_filename,
        drive_id=drive_id,
        item_id=item_id,
    )


def copy_file(
    headers: dict | GraphCredentials,
    site_id: str | None,
    source_path: str | None,
    destination_path: str | None = None,
    new_filename: str | None = None,
    *,
    drive_id: str | None = None,
    item_id: str | None = None,
    destination_id: str | None = None,
) -> dict | None:
    """Copies a file within a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-copy

    The source file can be addressed by path or by ID:
    - **Path-based**: provide ``source_path``.
    - **ID-based**: provide ``item_id``.

    The destination folder can likewise be given as a path or an ID:
    - **Path-based**: provide ``destination_path`` (a folder path, not a
        file path).
    - **ID-based**: provide ``destination_id``.

    Graph runs the copy asynchronously. A same-drive copy of a small file
    often completes immediately and returns the new item; Graph is also
    free to return 202 Accepted with an empty body and a monitor URL in
    the ``Location`` header, with the copy still running in the
    background.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly.
    :param source_path: The current path of the file to copy
        (e.g. "/Shared Documents/My Folder/file.txt"). Required for
        path-based source addressing. Ignored if ``item_id`` is given.
    :param destination_path: The destination folder path
        (e.g. "/Shared Documents/Other Folder"). Ignored if
        ``destination_id`` is given.
    :param new_filename: The new name for the copied file. If None, the original name is kept.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used. Valid for either addressing mode.
    :param item_id: The ID of the file to copy. Required for ID-based
        source addressing.
    :param destination_id: The ID of the destination folder.
    :return: The copied item as a JSON object if Graph completed the copy
        synchronously, or ``None`` if Graph accepted the request and is
        still processing it (check the ``Location`` header of the raw
        response, via ``_request``'s return value, to poll for completion).
    :raises ValueError: If neither ``source_path`` nor ``item_id`` is given,
        or if neither ``destination_path`` nor ``destination_id`` is given.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    if item_id is not None:
        url = f"{_drive_base(site_id, drive_id)}/items/{item_id}/copy"
    elif source_path is not None:
        url = f"{_drive_base(site_id, drive_id)}/root:{source_path}:/copy"
    else:
        raise ValueError("Provide either source_path or item_id.")

    payload = _build_payload(
        destination_path,
        new_filename,
        drive_id=drive_id,
        destination_id=destination_id,
    )
    if "parentReference" not in payload:
        raise ValueError("Provide either destination_path or destination_id.")

    response = _request(
        "POST",
        url,
        headers,
        extra_headers={"Content-Type": "application/json"},
        json=payload,
    )
    logger.info(
        "%s has been copied to: %s",
        source_path or item_id,
        destination_path or destination_id,
    )

    # 202 Accepted has no JSON body: the copy is still running in Graph.
    if response.status_code == 202 or not response.content:
        return None
    return response.json()


def remove_file(
    headers: dict | GraphCredentials,
    site_id: str | None,
    file_path: str | None,
    *,
    drive_id: str | None = None,
    item_id: str | None = None,
) -> None:
    """Removes a file from a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/driveitem-delete

    The file can be addressed by path or by ID:
    - **Path-based**: provide ``file_path``.
    - **ID-based**: provide ``item_id``.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site. Optional if ``drive_id``
        is given directly.
    :param file_path: The path of the file to remove
        (e.g. "/Shared Documents/My Folder/file.txt"). Required for
        path-based addressing. Ignored if ``item_id`` is given.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :param item_id: The ID of the file to remove. Required for ID-based
        addressing.
    :raises ValueError: If neither ``file_path`` nor ``item_id`` is given.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    if item_id is not None:
        url = f"{_drive_base(site_id, drive_id)}/items/{item_id}"
    elif file_path is not None:
        url = f"{_drive_base(site_id, drive_id)}/root:{file_path}"
    else:
        raise ValueError("Provide either file_path or item_id.")

    _request("DELETE", url, headers)
    logger.info("%s has been removed from SharePoint.", file_path or item_id)


# ----------------------------------- List Functions ----------------------------------- #


def get_lists(
    headers: dict | GraphCredentials,
    site_id: str,
    *,
    page_size: int | None = None,
) -> list[dict]:
    """Retrieves the lists from a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/list-list

    Follows ``@odata.nextLink`` to completion.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param page_size: Optional ``$top`` override.
    :return: A list of SharePoint lists as JSON objects across all pages.
    :raises requests.RequestException: If any paged request fails (including
        after retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists"
    return _iter_pages(url, headers, page_size=page_size)


def get_list_metadata(
    headers: dict | GraphCredentials, site_id: str, list_id: str
) -> dict:
    """Retrieves the metadata of a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/list-get

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :return: The list metadata as a JSON object.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}"
    response = _request("GET", url, headers)
    return response.json()


def create_list(
    headers: dict | GraphCredentials,
    site_id: str,
    list_name: str,
    list_template: str = "genericList",
) -> dict:
    """Creates a new SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/list-create

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param list_name: The name of the new SharePoint list.
    :param list_template: The template for the new SharePoint list. Default is "genericList".
    :return: The created list metadata as a JSON object.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists"
    payload = {"displayName": list_name, "list": {"template": list_template}}
    response = _request(
        "POST",
        url,
        headers,
        extra_headers={"Content-Type": "application/json"},
        json=payload,
    )
    return response.json()


def remove_list(headers: dict | GraphCredentials, site_id: str, list_id: str) -> None:
    """Removes a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/list-delete

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}"
    _request("DELETE", url, headers)
    logger.info("List %s has been removed from site %s.", list_id, site_id)


def get_list_items(
    headers: dict | GraphCredentials,
    site_id: str,
    list_id: str,
    odata_filter: str | None = None,
    *,
    expand: str | None = None,
    page_size: int | None = None,
) -> list[dict]:
    """Retrieves the items from a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-list

    Follows ``@odata.nextLink`` to completion.

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param odata_filter: An optional OData filter string to filter the list items.
    :param expand: Optional ``$expand`` value, e.g. ``"fields"`` to include
        column values in each returned item, or ``"fields(select=Title,Status)"``
        to limit which columns come back. Without this, returned items carry
        only base metadata (id, webUrl, timestamps) and no field values.
    :param page_size: Optional ``$top`` override.
    :return: A list of SharePoint list items as JSON objects across all pages.
    :raises requests.RequestException: If any paged request fails (including
        after retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items"

    params = []
    if odata_filter:
        params.append(f"$filter={odata_filter}")
    if expand:
        params.append(f"$expand={expand}")
    if params:
        url += "?" + "&".join(params)

    return _iter_pages(url, headers, page_size=page_size)


def get_list_item_metadata(
    headers: dict | GraphCredentials, site_id: str, list_id: str, item_id: str
) -> dict:
    """Retrieves the metadata of a SharePoint list item using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-get

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param item_id: The ID of the SharePoint list item.
    :return: The list item metadata as a JSON object.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items/{item_id}?expand=fields"
    response = _request("GET", url, headers)
    return response.json()


def create_list_item(
    headers: dict | GraphCredentials, site_id: str, list_id: str, fields: dict
) -> dict:
    """Creates a new item in a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-create

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param fields: A dictionary containing the field values for the new list item.
    :return: The created list item metadata as a JSON object.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items"
    payload = {"fields": fields}
    response = _request(
        "POST",
        url,
        headers,
        extra_headers={"Content-Type": "application/json"},
        json=payload,
    )
    return response.json()


def update_list_item(
    headers: dict | GraphCredentials,
    site_id: str,
    list_id: str,
    item_id: str,
    fields: dict,
) -> dict:
    """Updates an existing item in a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-update

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param item_id: The ID of the SharePoint list item.
    :param fields: A dictionary containing the updated field values for the list item.
    :return: The updated list item metadata as a JSON object.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    response = _request(
        "PATCH",
        url,
        headers,
        extra_headers={"Content-Type": "application/json"},
        json=fields,
    )
    return response.json()


def remove_list_item(
    headers: dict | GraphCredentials, site_id: str, list_id: str, item_id: str
) -> None:
    """Removes an item from a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-delete

    :param headers: The headers containing the Authorization token, or a
        ``GraphCredentials`` to mint and re-mint them.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param item_id: The ID of the SharePoint list item.
    :raises requests.RequestException: If the request fails (including after
        retries are exhausted).
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items/{item_id}"
    _request("DELETE", url, headers)
    logger.info("Item %s has been removed from list %s.", item_id, list_id)
