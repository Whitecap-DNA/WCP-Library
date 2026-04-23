"""Mock tests for wcp_library.graph.sharepoint.

All HTTP calls are patched via unittest.mock. No network access occurs.
No real filesystem writes occur (download_file uses Path.write_bytes which
is stubbed out).
"""
import base64
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from wcp_library.graph import sharepoint


# --------------------------- Helpers --------------------------- #


def _ok_json(payload, status_code=200):
    """Build a MagicMock response object that returns ``payload`` from .json()."""
    mock = MagicMock()
    mock.json.return_value = payload
    mock.status_code = status_code
    mock.content = b""
    return mock


def _ok_bytes(content, status_code=200):
    mock = MagicMock()
    mock.status_code = status_code
    mock.content = content
    return mock


def _http_error(status=500):
    err = requests.exceptions.RequestException("boom")
    err.response = MagicMock(status_code=status, text="error body")
    return err


def _called_method(mock):
    """Extract the HTTP method argument from a patched _request call."""
    return mock.call_args[0][0]


def _called_url(mock):
    """Extract the URL argument from a patched _request call."""
    return mock.call_args[0][1]


def _called_headers(mock):
    """Extract the headers argument from a patched _request call."""
    return mock.call_args[0][2]


HEADERS = {"Authorization": "Bearer testtoken"}
SITE_ID = "contoso.sharepoint.com,abc-123,def-456"
DRIVE_ID = "b!drive-xyz"
LIST_ID = "list-abc"
ITEM_ID = "item-42"


# ======================= Site functions ======================= #


class TestGetSiteMetadata:
    def test_returns_site_json_and_calls_correct_url(self):
        with patch("wcp_library.graph.sharepoint._request") as mock_req:
            mock_req.return_value = _ok_json({"id": SITE_ID, "displayName": "DataOps"})
            result = sharepoint.get_site_metadata(
                HEADERS, "https://contoso.sharepoint.com/sites/DataOps"
            )
            assert result == {"id": SITE_ID, "displayName": "DataOps"}
            assert mock_req.call_count == 1
            assert _called_method(mock_req) == "GET"
            assert _called_url(mock_req) == (
                "https://graph.microsoft.com/v1.0/sites/"
                "contoso.sharepoint.com:/sites/DataOps"
            )
            assert _called_headers(mock_req) == HEADERS

    def test_returns_none_on_request_exception(self):
        with patch("wcp_library.graph.sharepoint._request") as mock_req:
            mock_req.side_effect = _http_error()
            assert (
                sharepoint.get_site_metadata(
                    HEADERS, "https://contoso.sharepoint.com/sites/x"
                )
                is None
            )


class TestGetDrives:
    def test_returns_all_drives(self):
        page = _ok_json({"value": [{"id": "d1", "name": "Documents"}]})
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=page
        ) as mock_req:
            result = sharepoint.get_drives(HEADERS, SITE_ID)
            assert result == [{"id": "d1", "name": "Documents"}]
            assert mock_req.call_count == 1
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives"
            )

    def test_passes_page_size_as_top_param(self):
        page = _ok_json({"value": []})
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=page
        ) as mock_req:
            sharepoint.get_drives(HEADERS, SITE_ID, page_size=100)
            assert "$top=100" in _called_url(mock_req)

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.get_drives(HEADERS, SITE_ID) is None


class TestGetDriveIdByName:
    def test_returns_matching_drive_id(self):
        drives_payload = {
            "value": [
                {"id": "d1", "name": "Documents"},
                {"id": "d2", "name": "Reports"},
            ]
        }
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json(drives_payload),
        ):
            assert sharepoint.get_drive_id_by_name(HEADERS, SITE_ID, "Reports") == "d2"

    def test_returns_none_when_name_not_found(self):
        drives_payload = {"value": [{"id": "d1", "name": "Documents"}]}
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json(drives_payload),
        ):
            assert sharepoint.get_drive_id_by_name(HEADERS, SITE_ID, "Missing") is None

    def test_returns_none_when_drives_fetch_fails(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.get_drive_id_by_name(HEADERS, SITE_ID, "Anything") is None


# ======================= File (DriveItem) functions ======================= #


class TestListFolder:
    def test_returns_items_for_root_when_path_is_slash(self):
        page = _ok_json({"value": [{"id": "f1", "name": "file.txt"}]})
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=page
        ) as mock_req:
            result = sharepoint.list_folder(HEADERS, SITE_ID, "/")
            assert result == [{"id": "f1", "name": "file.txt"}]
            assert _called_url(mock_req).endswith(
                f"/sites/{SITE_ID}/drive/root/children"
            )

    def test_returns_items_for_named_folder(self):
        page = _ok_json({"value": [{"id": "f1"}]})
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=page
        ) as mock_req:
            sharepoint.list_folder(HEADERS, SITE_ID, "/Shared Documents/Reports")
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive"
                f"/root:/Shared Documents/Reports:/children"
            )

    def test_uses_drive_id_when_provided(self):
        page = _ok_json({"value": []})
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=page
        ) as mock_req:
            sharepoint.list_folder(HEADERS, SITE_ID, "/folder", drive_id=DRIVE_ID)
            called_url = _called_url(mock_req)
            assert f"/drives/{DRIVE_ID}/" in called_url
            assert f"/sites/{SITE_ID}/" not in called_url

    def test_follows_next_link_across_pages(self):
        page1 = _ok_json(
            {
                "value": [{"id": "a"}],
                "@odata.nextLink": "https://graph.microsoft.com/page2",
            }
        )
        page2 = _ok_json({"value": [{"id": "b"}]})
        with patch(
            "wcp_library.graph.sharepoint._request",
            side_effect=[page1, page2],
        ) as mock_req:
            result = sharepoint.list_folder(HEADERS, SITE_ID, "/folder")
            assert result == [{"id": "a"}, {"id": "b"}]
            assert mock_req.call_count == 2
            assert mock_req.call_args_list[1][0][1] == "https://graph.microsoft.com/page2"

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.list_folder(HEADERS, SITE_ID, "/folder") is None


class TestGetFileMetadata:
    def test_returns_metadata_json(self):
        payload = {"id": "file-1", "name": "report.xlsx"}
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json(payload),
        ) as mock_req:
            result = sharepoint.get_file_metadata(
                HEADERS, SITE_ID, "/Shared Documents/report.xlsx"
            )
            assert result == payload
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root:"
                "/Shared Documents/report.xlsx"
            )

    def test_uses_drive_id_when_provided(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"id": "x"}),
        ) as mock_req:
            sharepoint.get_file_metadata(
                HEADERS, SITE_ID, "/a.txt", drive_id=DRIVE_ID
            )
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/a.txt"
            )

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.get_file_metadata(HEADERS, SITE_ID, "/x.txt") is None


class TestGetFileContent:
    def test_returns_bytes(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_bytes(b"contents"),
        ) as mock_req:
            result = sharepoint.get_file_content(
                HEADERS, SITE_ID, "/Shared Documents/a.txt"
            )
            assert result == b"contents"
            assert _called_url(mock_req).endswith(
                f"/sites/{SITE_ID}/drive/root:/Shared Documents/a.txt:/content"
            )

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.get_file_content(HEADERS, SITE_ID, "/x.txt") is None


class TestGetFileContentById:
    def test_returns_bytes(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_bytes(b"raw"),
        ) as mock_req:
            result = sharepoint.get_file_content_by_id(HEADERS, DRIVE_ID, ITEM_ID)
            assert result == b"raw"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{ITEM_ID}/content"
            )

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.get_file_content_by_id(HEADERS, DRIVE_ID, ITEM_ID) is None


class TestUploadFile:
    def test_uploads_bytes_with_default_conflict_behavior(self):
        response_payload = {
            "id": "item-1",
            "name": "report.xlsx",
            "parentReference": {"path": "/drives/x/root:/Shared Documents"},
        }
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json(response_payload),
        ) as mock_req:
            result = sharepoint.upload_file(
                HEADERS,
                SITE_ID,
                "/Shared Documents",
                "report.xlsx",
                b"file-bytes",
            )
            assert result == response_payload
            assert _called_method(mock_req) == "PUT"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root:"
                "/Shared Documents/report.xlsx:/content"
                "?@microsoft.graph.conflictBehavior=rename"
            )
            assert mock_req.call_args.kwargs["data"] == b"file-bytes"
            assert _called_headers(mock_req) == HEADERS

    def test_custom_conflict_behavior_appears_in_url(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"parentReference": {"path": ""}}),
        ) as mock_req:
            sharepoint.upload_file(
                HEADERS, SITE_ID, "/Docs", "a.txt", b"x", conflict_behavior="replace"
            )
            assert _called_url(mock_req).endswith(
                "@microsoft.graph.conflictBehavior=replace"
            )

    def test_base64_string_is_decoded_before_upload(self):
        b64 = base64.b64encode(b"hello world").decode()
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"parentReference": {"path": ""}}),
        ) as mock_req:
            sharepoint.upload_file(HEADERS, SITE_ID, "/Docs", "a.txt", b64)
            assert mock_req.call_args.kwargs["data"] == b"hello world"

    def test_bytearray_is_converted_to_bytes(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"parentReference": {"path": ""}}),
        ) as mock_req:
            sharepoint.upload_file(
                HEADERS, SITE_ID, "/Docs", "a.txt", bytearray(b"abc")
            )
            assert mock_req.call_args.kwargs["data"] == b"abc"

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert (
                sharepoint.upload_file(
                    HEADERS, SITE_ID, "/Docs", "a.txt", b"x"
                )
                is None
            )


class TestDownloadFile:
    def test_writes_content_to_download_folder_and_returns_path(self, tmp_path):
        content = b"downloaded-bytes"
        fake_folder = MagicMock(spec=Path)
        fake_output = MagicMock(spec=Path)
        fake_folder.__truediv__.return_value = fake_output
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_bytes(content),
        ) as mock_req:
            result = sharepoint.download_file(
                HEADERS, SITE_ID, "/Docs/report.xlsx", fake_folder
            )
            assert _called_url(mock_req).endswith(
                f"/sites/{SITE_ID}/drive/root:/Docs/report.xlsx:/content"
            )
            # download_folder / Path(file_path).name -> fake_output.
            # Path(file_path).name returns a plain string, not a Path.
            fake_folder.__truediv__.assert_called_once_with("report.xlsx")
            fake_output.write_bytes.assert_called_once_with(content)
            assert result is fake_output

    def test_returns_none_on_request_exception(self, tmp_path):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert (
                sharepoint.download_file(
                    HEADERS, SITE_ID, "/Docs/report.xlsx", tmp_path
                )
                is None
            )


class TestMoveFile:
    def test_patches_with_parent_reference_payload(self):
        response_payload = {
            "id": "item-1",
            "name": "file.txt",
            "parentReference": {"path": "/drive/root:/Shared Documents/Other"},
        }
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json(response_payload),
        ) as mock_req:
            result = sharepoint.move_file(
                HEADERS,
                SITE_ID,
                "/Shared Documents/file.txt",
                "/Shared Documents/Other",
            )
            assert result == response_payload
            assert _called_method(mock_req) == "PATCH"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root:"
                "/Shared Documents/file.txt"
            )
            sent = mock_req.call_args.kwargs["json"]
            assert sent == {
                "parentReference": {"path": "/drive/root:/Shared Documents/Other"}
            }
            assert _called_headers(mock_req)["Content-Type"] == "application/json"

    def test_new_filename_added_to_payload(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"parentReference": {"path": ""}, "name": "r.txt"}),
        ) as mock_req:
            sharepoint.move_file(
                HEADERS, SITE_ID, "/a/b.txt", "/a/newfolder", new_filename="renamed.txt"
            )
            sent = mock_req.call_args.kwargs["json"]
            assert sent["name"] == "renamed.txt"

    def test_drive_id_changes_parent_path_prefix(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"parentReference": {"path": ""}, "name": ""}),
        ) as mock_req:
            sharepoint.move_file(
                HEADERS, SITE_ID, "/a.txt", "/b", drive_id=DRIVE_ID
            )
            sent = mock_req.call_args.kwargs["json"]
            assert sent["parentReference"]["path"] == f"/drives/{DRIVE_ID}/root:/b"

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert (
                sharepoint.move_file(HEADERS, SITE_ID, "/a.txt", "/b") is None
            )


class TestRenameFile:
    def test_delegates_to_move_file_with_same_path(self):
        with patch("wcp_library.graph.sharepoint.move_file") as mock_move:
            mock_move.return_value = {"renamed": True}
            result = sharepoint.rename_file(
                HEADERS, SITE_ID, "/Docs/old.txt", "new.txt"
            )
            assert result == {"renamed": True}
            mock_move.assert_called_once_with(
                HEADERS,
                SITE_ID,
                "/Docs/old.txt",
                destination_path="/Docs/old.txt",
                new_filename="new.txt",
                drive_id=None,
            )

    def test_propagates_none_when_move_fails(self):
        with patch("wcp_library.graph.sharepoint.move_file", return_value=None):
            assert (
                sharepoint.rename_file(
                    HEADERS, SITE_ID, "/Docs/old.txt", "new.txt"
                )
                is None
            )


class TestCopyFile:
    def test_posts_with_parent_reference_payload(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"id": "new"}),
        ) as mock_req:
            result = sharepoint.copy_file(
                HEADERS, SITE_ID, "/a/b.txt", "/other"
            )
            assert result == {"id": "new"}
            assert _called_method(mock_req) == "POST"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root:"
                "/a/b.txt:/copy"
            )
            sent = mock_req.call_args.kwargs["json"]
            assert sent == {"parentReference": {"path": "/drive/root:/other"}}
            assert _called_headers(mock_req)["Content-Type"] == "application/json"

    def test_new_filename_is_included(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"id": "new"}),
        ) as mock_req:
            sharepoint.copy_file(
                HEADERS, SITE_ID, "/a.txt", "/other", new_filename="renamed.txt"
            )
            sent = mock_req.call_args.kwargs["json"]
            assert sent["name"] == "renamed.txt"

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.copy_file(HEADERS, SITE_ID, "/a.txt", "/b") is None


class TestRemoveFile:
    def test_returns_true_on_success(self):
        response = MagicMock()
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=response
        ) as mock_req:
            assert sharepoint.remove_file(HEADERS, SITE_ID, "/a.txt") is True
            assert _called_method(mock_req) == "DELETE"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root:/a.txt"
            )

    def test_returns_false_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.remove_file(HEADERS, SITE_ID, "/a.txt") is False


# ======================= List functions ======================= #


class TestGetLists:
    def test_returns_all_lists(self):
        page = _ok_json({"value": [{"id": "l1"}, {"id": "l2"}]})
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=page
        ) as mock_req:
            result = sharepoint.get_lists(HEADERS, SITE_ID)
            assert result == [{"id": "l1"}, {"id": "l2"}]
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/lists"
            )

    def test_follows_next_link_across_pages(self):
        page1 = _ok_json(
            {
                "value": [{"id": "a"}],
                "@odata.nextLink": "https://graph.microsoft.com/next",
            }
        )
        page2 = _ok_json({"value": [{"id": "b"}]})
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=[page1, page2]
        ) as mock_req:
            result = sharepoint.get_lists(HEADERS, SITE_ID)
            assert result == [{"id": "a"}, {"id": "b"}]
            assert mock_req.call_count == 2

    def test_returns_empty_list_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.get_lists(HEADERS, SITE_ID) == []


class TestGetListMetadata:
    def test_returns_list_json(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"id": LIST_ID, "displayName": "Tasks"}),
        ) as mock_req:
            result = sharepoint.get_list_metadata(HEADERS, SITE_ID, LIST_ID)
            assert result == {"id": LIST_ID, "displayName": "Tasks"}
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/lists/{LIST_ID}"
            )

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.get_list_metadata(HEADERS, SITE_ID, LIST_ID) is None


class TestCreateList:
    def test_posts_with_default_template(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"id": "new-list"}),
        ) as mock_req:
            result = sharepoint.create_list(HEADERS, SITE_ID, "MyList")
            assert result == {"id": "new-list"}
            assert _called_method(mock_req) == "POST"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/lists"
            )
            assert mock_req.call_args.kwargs["json"] == {
                "displayName": "MyList",
                "list": {"template": "genericList"},
            }
            assert _called_headers(mock_req)["Content-Type"] == "application/json"

    def test_custom_template_in_payload(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"id": "x"}),
        ) as mock_req:
            sharepoint.create_list(HEADERS, SITE_ID, "L", list_template="documentLibrary")
            sent = mock_req.call_args.kwargs["json"]
            assert sent["list"]["template"] == "documentLibrary"

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.create_list(HEADERS, SITE_ID, "N") is None


class TestRemoveList:
    def test_returns_true_on_success(self):
        response = MagicMock()
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=response
        ) as mock_req:
            assert sharepoint.remove_list(HEADERS, SITE_ID, LIST_ID) is True
            assert _called_method(mock_req) == "DELETE"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/lists/{LIST_ID}"
            )

    def test_returns_false_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.remove_list(HEADERS, SITE_ID, LIST_ID) is False


class TestGetListItems:
    def test_returns_items_without_filter(self):
        page = _ok_json({"value": [{"id": "i1"}]})
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=page
        ) as mock_req:
            result = sharepoint.get_list_items(HEADERS, SITE_ID, LIST_ID)
            assert result == [{"id": "i1"}]
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/lists/{LIST_ID}/items"
            )

    def test_appends_filter_to_url(self):
        page = _ok_json({"value": []})
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=page
        ) as mock_req:
            sharepoint.get_list_items(
                HEADERS, SITE_ID, LIST_ID, odata_filter="fields/Status eq 'Open'"
            )
            assert "$filter=fields/Status eq 'Open'" in _called_url(mock_req)

    def test_follows_next_link_across_pages(self):
        page1 = _ok_json(
            {
                "value": [{"id": "a"}],
                "@odata.nextLink": "https://graph.microsoft.com/listpage2",
            }
        )
        page2 = _ok_json({"value": [{"id": "b"}]})
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=[page1, page2]
        ) as mock_req:
            result = sharepoint.get_list_items(HEADERS, SITE_ID, LIST_ID)
            assert result == [{"id": "a"}, {"id": "b"}]
            assert mock_req.call_count == 2

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert sharepoint.get_list_items(HEADERS, SITE_ID, LIST_ID) is None


class TestGetListItemMetadata:
    def test_returns_item_json_and_expands_fields(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"id": ITEM_ID, "fields": {"Title": "x"}}),
        ) as mock_req:
            result = sharepoint.get_list_item_metadata(
                HEADERS, SITE_ID, LIST_ID, ITEM_ID
            )
            assert result == {"id": ITEM_ID, "fields": {"Title": "x"}}
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}"
                f"/lists/{LIST_ID}/items/{ITEM_ID}?expand=fields"
            )

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert (
                sharepoint.get_list_item_metadata(
                    HEADERS, SITE_ID, LIST_ID, ITEM_ID
                )
                is None
            )


class TestCreateListItem:
    def test_posts_payload_wrapped_in_fields(self):
        fields = {"Title": "Q3 Report", "Status": "Draft"}
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"id": "new-item"}),
        ) as mock_req:
            result = sharepoint.create_list_item(HEADERS, SITE_ID, LIST_ID, fields)
            assert result == {"id": "new-item"}
            assert _called_method(mock_req) == "POST"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/lists/{LIST_ID}/items"
            )
            assert mock_req.call_args.kwargs["json"] == {"fields": fields}
            assert _called_headers(mock_req)["Content-Type"] == "application/json"

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert (
                sharepoint.create_list_item(HEADERS, SITE_ID, LIST_ID, {"Title": "x"})
                is None
            )


class TestUpdateListItem:
    def test_patches_fields_endpoint_directly(self):
        fields = {"Status": "Complete"}
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"Status": "Complete"}),
        ) as mock_req:
            result = sharepoint.update_list_item(
                HEADERS, SITE_ID, LIST_ID, ITEM_ID, fields
            )
            assert result == {"Status": "Complete"}
            assert _called_method(mock_req) == "PATCH"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}"
                f"/lists/{LIST_ID}/items/{ITEM_ID}/fields"
            )
            assert mock_req.call_args.kwargs["json"] == fields
            assert _called_headers(mock_req)["Content-Type"] == "application/json"

    def test_returns_none_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert (
                sharepoint.update_list_item(
                    HEADERS, SITE_ID, LIST_ID, ITEM_ID, {"Status": "x"}
                )
                is None
            )


class TestRemoveListItem:
    def test_returns_true_on_success(self):
        response = MagicMock()
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=response
        ) as mock_req:
            assert (
                sharepoint.remove_list_item(HEADERS, SITE_ID, LIST_ID, ITEM_ID)
                is True
            )
            assert _called_method(mock_req) == "DELETE"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}"
                f"/lists/{LIST_ID}/items/{ITEM_ID}"
            )

    def test_returns_false_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            assert (
                sharepoint.remove_list_item(HEADERS, SITE_ID, LIST_ID, ITEM_ID)
                is False
            )
