import base64
from typing import Union

import requests
from yarl import URL

from wcp_library.graph import REQUEST_TIMEOUT

# ----------------------------------- Site Functions ----------------------------------- #


def get_site_metadata(headers: dict, site_home_url: str) -> dict | None:
    """Retrieves the site ID from a SharePoint site URL (needs to be the home page)

    :param headers: The headers containing the Authorization token.
    :param site_home_url: The URL of the SharePoint site.
    :return: The site metadata as a JSON object.
    """
    url = URL(site_home_url)
    url = f"https://graph.microsoft.com/v1.0/sites/{url.host}:{url.path}"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


# ----------------------------------- File Functions ----------------------------------- #


def get_file_metadata(headers: dict, site_id: str, file_path: str) -> dict | None:
    """Retrieves the file metadata from a SharePoint site using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The path of the file (e.g. "/Shared Documents/My Folder/file.txt")
    :return: The file metadata as a JSON object.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path}"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def upload_file(
    headers: dict,
    site_id: str,
    file_path: str,
    filename: str,
    content: Union[bytes, bytearray, memoryview, str],
    conflict_behavior: str = "rename",
) -> dict | None:
    """Saves a file to a SharePoint site using the Microsoft Graph API.
    No need to create parent folders.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The location of the file to save (e.g. "Shared Documents/My Folder")
    :param filename: The name of the file to save.
    :param content: The file content as bytes, bytearray, memoryview,
        or base64-encoded string (from Graph API).
    :param conflict_behavior: The behavior when a file with the same name already exists.
        Options are "rename", "replace", or "fail". Default is "rename".
    :return: The response from the Microsoft Graph API as a JSON object.
    """
    url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:"
        f"{file_path}/{filename}:/content"
        f"?@microsoft.graph.conflictBehavior={conflict_behavior}"
    )
    try:
        response = requests.put(
            url,
            headers=headers,
            data=_ensure_bytes(content),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        json_response = response.json()
        parent_path = json_response.get("parentReference", {}).get("path", "")
        print(f"{filename} has been uploaded to: {parent_path}")
        return json_response
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def _ensure_bytes(content: Union[bytes, bytearray, memoryview, str]) -> bytes:
    if isinstance(content, bytes):
        return content
    if isinstance(content, (bytearray, memoryview)):
        return bytes(content)
    if isinstance(content, str):
        return base64.b64decode(content)
    raise TypeError(f"Unsupported content type: {type(content).__name__}")


def download_file(headers: dict, site_id: str, file_path: str) -> bytes | None:
    """Downloads a file from a SharePoint site using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The path of the file to download
        (e.g. "/Shared Documents/My Folder/file.txt")
    :return: The content of the file as bytes.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path}:/content"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def move_file(
    headers: dict,
    site_id: str,
    source_path: str,
    destination_path: str,
    new_filename: str | None = None,
) -> dict | None:
    """Moves a file within a SharePoint site using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param source_path: The current path of the file to move
        (e.g. "/Shared Documents/My Folder/file.txt")
    :param destination_path: The destination folder path (e.g. "/Shared Documents/Other Folder")
    :param new_filename: The new name for the file. If None, the original name is kept.
    :return: The response from the Microsoft Graph API as a JSON object.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{source_path}"
    payload = _build_payload(destination_path, new_filename)
    try:
        response = requests.patch(
            url,
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        response_json = response.json()
        parent_path = response_json.get("parentReference", {}).get("path", "")
        print(
            f"{source_path} has been updated to: {parent_path}/{response_json.get('name', '')}"
        )
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def rename_file(
    headers: dict,
    site_id: str,
    file_path: str,
    new_filename: str,
) -> dict | None:
    """Moves a file within a SharePoint site using the Microsoft Graph API
        (using the move_file function).

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The current path of the file to rename
        (e.g. "/Shared Documents/My Folder/file.txt")
    :param new_filename: The new name for the file.
    :return: The response from the Microsoft Graph API as a JSON object.
    """
    return move_file(
        headers,
        site_id,
        file_path,
        destination_path=file_path,
        new_filename=new_filename,
    )


def copy_file(
    headers: dict,
    site_id: str,
    source_path: str,
    destination_path: str,
    new_filename: str | None = None,
) -> dict | None:
    """Copies a file within a SharePoint site using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param source_path: The current path of the file to copy
        (e.g. "/Shared Documents/My Folder/file.txt")
    :param destination_path: The destination folder path
        (e.g. "/Shared Documents/Other Folder")
    :param new_filename: The new name for the copied file. If None, the original name is kept.
    :return: The response from the Microsoft Graph API as a JSON object.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{source_path}:/copy"
    payload = _build_payload(destination_path, new_filename)
    try:
        response = requests.post(
            url,
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        print(f"{source_path} has been copied to: {destination_path}")
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def _build_payload(
    destination_path: str | None, new_filename: str | None = None
) -> dict:
    payload = {"parentReference": {"path": f"/drive/root:{destination_path}"}}
    if new_filename:
        payload["name"] = new_filename
    return payload


def remove_file(headers: dict, site_id: str, file_path: str) -> bool:
    """Removes a file from a SharePoint site using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param file_path: The path of the file to remove
        (e.g. "/Shared Documents/My Folder/file.txt")
    :return: True if the file was removed successfully, False otherwise.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path}"
    try:
        response = requests.delete(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        print(f"{file_path} has been removed from SharePoint.")
        return True
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return False


# ----------------------------------- List Functions ----------------------------------- #


def get_lists(headers: dict, site_id: str) -> list[dict]:
    """Retrieves the lists from a SharePoint site using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :return: A list of SharePoint lists as JSON objects.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data.get("value", [])
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return []


def get_list_metadata(headers: dict, site_id: str, list_id: str) -> dict | None:
    """Retrieves the metadata of a SharePoint list using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :return: The list metadata as a JSON object.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def create_list(
    headers: dict, site_id: str, list_name: str, list_template: str = "genericList"
) -> dict | None:
    """Creates a new SharePoint list using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_name: The name of the new SharePoint list.
    :param list_template: The template for the new SharePoint list. Default is "genericList".
    :return: The created list metadata as a JSON object.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    payload = {"displayName": list_name, "list": {"template": list_template}}
    try:
        response = requests.post(
            url,
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def remove_list(headers: dict, site_id: str, list_id: str) -> bool:
    """Removes a SharePoint list using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :return: True if the list was removed successfully, False otherwise.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}"
    try:
        response = requests.delete(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        print(f"List {list_id} has been removed from site {site_id}.")
        return True
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return False


def get_list_items(
    headers: dict, site_id: str, list_id: str, odata_filter: str | None = None
) -> list[dict]:
    """Retrieves the items from a SharePoint list using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param filter: An optional OData filter string to filter the list items.
    :return: A list of SharePoint list items as JSON objects.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    if odata_filter:
        url += f"?$filter={odata_filter}"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data.get("value", [])
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def get_list_item_metadata(
    headers: dict, site_id: str, list_id: str, item_id: str
) -> dict | None:
    """Retrieves the metadata of a SharePoint list item using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param item_id: The ID of the SharePoint list item.
    :return: The list item metadata as a JSON object.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}?expand=fields"
    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def create_list_item(
    headers: dict, site_id: str, list_id: str, fields: dict
) -> dict | None:
    """Creates a new item in a SharePoint list using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param fields: A dictionary containing the field values for the new list item.
    :return: The created list item metadata as a JSON object.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    payload = {"fields": fields}
    try:
        response = requests.post(
            url,
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def update_list_item(
    headers: dict, site_id: str, list_id: str, item_id: str, fields: dict
) -> dict | None:
    """Updates an existing item in a SharePoint list using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param item_id: The ID of the SharePoint list item.
    :param fields: A dictionary containing the updated field values for the list item.
    :return: The updated list item metadata as a JSON object.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}/fields"
    try:
        response = requests.patch(
            url,
            headers={**headers, "Content-Type": "application/json"},
            json=fields,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return None


def remove_list_item(headers: dict, site_id: str, list_id: str, item_id: str) -> bool:
    """Removes an item from a SharePoint list using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param item_id: The ID of the SharePoint list item.
    :return: True if the item was removed successfully, False otherwise.
    """
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}"
    try:
        response = requests.delete(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        print(f"Item {item_id} has been removed from list {list_id}.")
        return True
    except requests.RequestException as e:
        print(f"Error: {e}\nResponse: {getattr(e.response, 'text', '')}")
        return False
