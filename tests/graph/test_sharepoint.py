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


def _ok_json(payload, status_code=200, content=b"{}"):
    """Build a MagicMock response object that returns ``payload`` from .json().

    ``content`` defaults to a non-empty placeholder: some functions
    (``copy_file``) branch on whether the raw response body is empty to
    detect a 202-Accepted, still-processing response, and an always-empty
    ``.content`` here would silently defeat that check.
    """
    mock = MagicMock()
    mock.json.return_value = payload
    mock.status_code = status_code
    mock.content = content
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
    """Extract the effective headers from a patched _request call.

    Additions for a single call, such as a Content-Type, are passed separately
    as ``extra_headers`` and merged at send time, so they are merged here too
    to give what the request will actually carry.
    """
    args, kwargs = mock.call_args
    return {**args[2], **(kwargs.get("extra_headers") or {})}


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

    def test_raises_on_request_exception(self):
        with patch("wcp_library.graph.sharepoint._request") as mock_req:
            mock_req.side_effect = _http_error()
            with pytest.raises(requests.RequestException):
                sharepoint.get_site_metadata(
                    HEADERS, "https://contoso.sharepoint.com/sites/x"
                )


class TestGetDrives:
    def test_returns_all_drives(self):
        page = _ok_json({"value": [{"id": "d1", "name": "Documents"}]})
        with patch(
            "wcp_library.graph._request", return_value=page
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
            "wcp_library.graph._request", return_value=page
        ) as mock_req:
            sharepoint.get_drives(HEADERS, SITE_ID, page_size=100)
            assert "$top=100" in _called_url(mock_req)

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.get_drives(HEADERS, SITE_ID)


class TestGetDriveIdByName:
    def test_returns_matching_drive_id(self):
        drives_payload = {
            "value": [
                {"id": "d1", "name": "Documents"},
                {"id": "d2", "name": "Reports"},
            ]
        }
        with patch(
            "wcp_library.graph._request",
            return_value=_ok_json(drives_payload),
        ):
            assert sharepoint.get_drive_id_by_name(HEADERS, SITE_ID, "Reports") == "d2"

    def test_returns_none_when_name_not_found(self):
        drives_payload = {"value": [{"id": "d1", "name": "Documents"}]}
        with patch(
            "wcp_library.graph._request",
            return_value=_ok_json(drives_payload),
        ):
            assert sharepoint.get_drive_id_by_name(HEADERS, SITE_ID, "Missing") is None

    def test_raises_when_drives_fetch_fails(self):
        with patch(
            "wcp_library.graph._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.get_drive_id_by_name(HEADERS, SITE_ID, "Anything")


# ======================= Delta functions ======================= #


class TestGetDelta:
    """Tests _get_delta directly, unlike this suite's usual convention of
    only exercising private helpers (_drive_base, _resolve_item_url, etc.)
    through their public callers. _get_delta carries enough of its own
    logic - pagination, delta-link capture, the site_id/drive_id/delta_link
    contract - to warrant direct coverage; TestGetChangedItems below covers
    the thin public wrapper without duplicating that logic.
    """

    def test_first_call_without_delta_link_hits_root_delta(self):
        payload = {
            "value": [{"id": "a", "file": {}}],
            "@odata.deltaLink": "https://graph.microsoft.com/final-link",
        }
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=_ok_json(payload)
        ) as mock_req:
            items, delta_link = sharepoint._get_delta(HEADERS, SITE_ID)
            assert items == [{"id": "a", "file": {}}]
            assert delta_link == "https://graph.microsoft.com/final-link"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root/delta"
            )

    def test_uses_drive_id_when_provided(self):
        payload = {"value": [], "@odata.deltaLink": "https://graph.microsoft.com/final"}
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=_ok_json(payload)
        ) as mock_req:
            sharepoint._get_delta(HEADERS, drive_id=DRIVE_ID)
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root/delta"
            )

    def test_uses_delta_link_directly_when_provided(self):
        stored_link = (
            "https://graph.microsoft.com/v1.0/sites/x/drive/root/delta?token=prev"
        )
        payload = {
            "value": [],
            "@odata.deltaLink": "https://graph.microsoft.com/next-token",
        }
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=_ok_json(payload)
        ) as mock_req:
            sharepoint._get_delta(HEADERS, delta_link=stored_link)
            assert _called_url(mock_req) == stored_link

    def test_page_size_appends_top_param_on_first_request(self):
        payload = {"value": [], "@odata.deltaLink": "https://graph.microsoft.com/final"}
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=_ok_json(payload)
        ) as mock_req:
            sharepoint._get_delta(HEADERS, SITE_ID, page_size=50)
            assert "$top=50" in _called_url(mock_req)

    def test_follows_next_link_and_captures_delta_link_on_final_page(self):
        page1 = _ok_json(
            {
                "value": [{"id": "a"}],
                "@odata.nextLink": "https://graph.microsoft.com/page2",
            }
        )
        page2 = _ok_json(
            {
                "value": [{"id": "b"}],
                "@odata.deltaLink": "https://graph.microsoft.com/final",
            }
        )
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=[page1, page2]
        ) as mock_req:
            items, delta_link = sharepoint._get_delta(HEADERS, SITE_ID)
            assert items == [{"id": "a"}, {"id": "b"}]
            assert delta_link == "https://graph.microsoft.com/final"
            assert mock_req.call_count == 2

    def test_raises_value_error_without_site_id_drive_id_or_delta_link(self):
        with pytest.raises(ValueError):
            sharepoint._get_delta(HEADERS)

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint._get_delta(HEADERS, SITE_ID)


class TestGetChangedItems:
    def test_filters_out_folders_and_deleted_items(self):
        with patch(
            "wcp_library.graph.sharepoint._get_delta",
            return_value=(
                [
                    {"id": "f1", "file": {}},
                    {"id": "folder1", "folder": {}},
                    {"id": "f2", "file": {}, "deleted": {"state": "softDeleted"}},
                ],
                "https://graph.microsoft.com/final",
            ),
        ) as mock_get_delta:
            files, delta_link = sharepoint.get_changed_items(HEADERS, SITE_ID)
            assert files == [{"id": "f1", "file": {}}]
            assert delta_link == "https://graph.microsoft.com/final"
            mock_get_delta.assert_called_once_with(
                HEADERS, SITE_ID, drive_id=None, delta_link=None, page_size=None
            )

    def test_forwards_drive_id_delta_link_and_page_size(self):
        with patch(
            "wcp_library.graph.sharepoint._get_delta", return_value=([], "next")
        ) as mock_get_delta:
            sharepoint.get_changed_items(
                HEADERS, SITE_ID, drive_id=DRIVE_ID, delta_link="prev", page_size=25
            )
            mock_get_delta.assert_called_once_with(
                HEADERS, SITE_ID, drive_id=DRIVE_ID, delta_link="prev", page_size=25
            )

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._get_delta", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.get_changed_items(HEADERS, SITE_ID)


# ======================= File (DriveItem) functions ======================= #


class TestListFolder:
    def test_returns_items_for_root_when_path_is_slash(self):
        page = _ok_json({"value": [{"id": "f1", "name": "file.txt"}]})
        with patch(
            "wcp_library.graph._request", return_value=page
        ) as mock_req:
            result = sharepoint.list_folder(HEADERS, SITE_ID, "/")
            assert result == [{"id": "f1", "name": "file.txt"}]
            assert _called_url(mock_req).endswith(
                f"/sites/{SITE_ID}/drive/root/children"
            )

    def test_returns_items_for_named_folder(self):
        page = _ok_json({"value": [{"id": "f1"}]})
        with patch(
            "wcp_library.graph._request", return_value=page
        ) as mock_req:
            sharepoint.list_folder(HEADERS, SITE_ID, "/Shared Documents/Reports")
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive"
                f"/root:/Shared Documents/Reports:/children"
            )

    def test_uses_drive_id_when_provided(self):
        page = _ok_json({"value": []})
        with patch(
            "wcp_library.graph._request", return_value=page
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
            "wcp_library.graph._request",
            side_effect=[page1, page2],
        ) as mock_req:
            result = sharepoint.list_folder(HEADERS, SITE_ID, "/folder")
            assert result == [{"id": "a"}, {"id": "b"}]
            assert mock_req.call_count == 2
            assert mock_req.call_args_list[1][0][1] == "https://graph.microsoft.com/page2"

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.list_folder(HEADERS, SITE_ID, "/folder")


class TestGetItemMetadata:
    def test_returns_folder_metadata_unmodified(self):
        # No "file" facet on a folder, so no name_no_extension/extension
        # keys should be added.
        payload = {"id": "folder-1", "name": "Reports", "folder": {"childCount": 3}}
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json(payload),
        ) as mock_req:
            result = sharepoint.get_item_metadata(
                HEADERS, SITE_ID, "/Shared Documents/Reports"
            )
            assert result == payload
            assert "name_no_extension" not in result
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root:"
                "/Shared Documents/Reports"
            )

    def test_adds_name_parts_when_item_is_a_file(self):
        payload = {"id": "file-1", "name": "report.xlsx", "file": {}}
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json(payload),
        ):
            result = sharepoint.get_item_metadata(
                HEADERS, SITE_ID, "/Shared Documents/report.xlsx"
            )
            assert result["name_no_extension"] == "report"
            assert result["extension"] == "xlsx"

    def test_uses_drive_id_when_provided(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"id": "x"}),
        ) as mock_req:
            sharepoint.get_item_metadata(
                HEADERS, SITE_ID, "/a.txt", drive_id=DRIVE_ID
            )
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/a.txt"
            )

    def test_raises_value_error_without_file_path_or_item_id(self):
        with pytest.raises(ValueError):
            sharepoint.get_item_metadata(HEADERS, SITE_ID, None)

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.get_item_metadata(HEADERS, SITE_ID, "/x.txt")


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

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.get_file_content(HEADERS, SITE_ID, "/x.txt")


class TestGetFileContentByIdMode:
    """get_file_content's ID-based addressing mode (drive_id + item_id).

    get_file_content_by_id was merged into get_file_content in 7f6080e;
    these tests exercise the mode that replaced it.
    """
    def test_returns_bytes(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_bytes(b"raw"),
        ) as mock_req:
            result = sharepoint.get_file_content(
                HEADERS, None, None, drive_id=DRIVE_ID, item_id=ITEM_ID
            )
            assert result == b"raw"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/items/{ITEM_ID}/content"
            )

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.get_file_content(
                    HEADERS, None, None, drive_id=DRIVE_ID, item_id=ITEM_ID
                )


class TestUploadFile:
    # Graph path addressing is root:/{full-item-path}:/{action}, where the
    # item path includes the filename for an upload. 1.15.1 closed the path at
    # the folder and opened a second segment for the filename, which Graph
    # answers with 400, and the assertion in this class was updated to match
    # the broken URL rather than the working one. These two pin the shapes.
    def test_path_based_url_keeps_the_filename_inside_one_path_segment(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"parentReference": {"path": ""}}),
        ) as mock_req:
            sharepoint.upload_file(HEADERS, SITE_ID, "/Folder", "file.csv", b"data")

        url = _called_url(mock_req)
        assert "/root:/Folder/file.csv:/content" in url
        assert "/root:/Folder:/file.csv" not in url

    def test_id_based_url_addresses_a_new_child_of_the_parent_id(self):
        # The ID form is different on purpose: /items/{parent-id}:/{filename}
        # is Graph's documented simple upload for a new child of that folder.
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"parentReference": {"path": ""}}),
        ) as mock_req:
            sharepoint.upload_file(
                HEADERS, SITE_ID, None, "file.csv", b"data", item_id=ITEM_ID
            )

        assert f"/items/{ITEM_ID}:/file.csv:/content" in _called_url(mock_req)

    def test_drive_id_path_upload_keeps_one_path_segment(self):
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({"parentReference": {"path": ""}}),
        ) as mock_req:
            sharepoint.upload_file(
                HEADERS, SITE_ID, "/Folder", "file.csv", b"data", drive_id=DRIVE_ID
            )

        url = _called_url(mock_req)
        assert url.startswith(
            f"https://graph.microsoft.com/v1.0/drives/{DRIVE_ID}/root:/Folder/file.csv"
        )
        assert ":/file.csv" not in url.replace("/Folder/file.csv", "")

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

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.upload_file(
                    HEADERS, SITE_ID, "/Docs", "a.txt", b"x"
                )


class TestUploadMultipleFiles:
    @staticmethod
    def _fake_upload_file(
        headers,
        site_id,
        file_path,
        filename,
        content,
        conflict_behavior="rename",
        *,
        drive_id=None,
        item_id=None,
    ):
        return {"filename": filename, "path": file_path}

    def test_uploads_each_file_and_preserves_order(self):
        with patch(
            "wcp_library.graph.sharepoint.upload_file",
            side_effect=self._fake_upload_file,
        ) as mock_upload:
            files = [
                ("/Docs", "a.txt", b"a"),
                ("/Docs", "b.txt", b"b"),
                ("/Docs", "c.txt", b"c"),
            ]
            results = sharepoint.upload_multiple_files(HEADERS, SITE_ID, files)
            assert results == [
                {"filename": "a.txt", "path": "/Docs"},
                {"filename": "b.txt", "path": "/Docs"},
                {"filename": "c.txt", "path": "/Docs"},
            ]
            assert mock_upload.call_count == 3

    def test_one_failed_upload_still_attempts_the_rest_then_raises(self):
        def _fake_upload_file(
            headers, site_id, file_path, filename, content, conflict_behavior="rename",
            *, drive_id=None, item_id=None,
        ):
            if filename == "bad.txt":
                raise _http_error()
            return {"filename": filename}

        with patch(
            "wcp_library.graph.sharepoint.upload_file", side_effect=_fake_upload_file
        ) as mock_upload:
            files = [
                ("/Docs", "good1.txt", b"a"),
                ("/Docs", "bad.txt", b"b"),
                ("/Docs", "good2.txt", b"c"),
            ]
            with pytest.raises(ExceptionGroup) as exc_info:
                sharepoint.upload_multiple_files(HEADERS, SITE_ID, files)

        # The good files were still attempted; the batch is not cancelled.
        assert mock_upload.call_count == 3
        group = exc_info.value
        assert len(group.exceptions) == 1
        assert isinstance(group.exceptions[0], requests.RequestException)
        assert "file: /Docs/bad.txt" in group.exceptions[0].__notes__

    def test_type_error_from_unsupported_content_is_raised_in_the_group(self):
        def _fake_upload_file(
            headers, site_id, file_path, filename, content, conflict_behavior="rename",
            *, drive_id=None, item_id=None,
        ):
            if not isinstance(content, (bytes, bytearray, memoryview, str)):
                raise TypeError("unsupported content type")
            return {"filename": filename}

        with patch(
            "wcp_library.graph.sharepoint.upload_file", side_effect=_fake_upload_file
        ):
            files = [("/Docs", "ok.txt", b"a"), ("/Docs", "bad.txt", 12345)]
            with pytest.raises(ExceptionGroup) as exc_info:
                sharepoint.upload_multiple_files(HEADERS, SITE_ID, files)

        group = exc_info.value
        assert len(group.exceptions) == 1
        assert isinstance(group.exceptions[0], TypeError)
        assert "file: /Docs/bad.txt" in group.exceptions[0].__notes__

    def test_batch_uploads_use_the_path_based_url_shape(self):
        # The batch delegates to upload_file, so it inherits the URL shape.
        # Patching _request rather than upload_file is what makes that
        # visible; the other tests in this class patch upload_file itself.
        urls = []

        def capture(method, url, headers, **kwargs):
            urls.append(url)
            return _ok_json({"parentReference": {"path": ""}})

        with patch("wcp_library.graph.sharepoint._request", side_effect=capture):
            sharepoint.upload_multiple_files(
                HEADERS,
                SITE_ID,
                [("/Docs", "a.csv", b"a"), ("/Docs", "b.csv", b"b")],
            )

        assert len(urls) == 2
        for url in urls:
            assert "/root:/Docs/" in url
            assert "/root:/Docs:/" not in url

    def test_an_unexpected_exception_still_reaches_the_caller(self):
        # An exception left uncaught in a worker thread never reaches the
        # joiner: it prints a traceback and leaves that entry empty. Every
        # exception type is collected so this cannot fail silently.
        def _fake_upload_file(
            headers, site_id, file_path, filename, content, conflict_behavior="rename",
            *, drive_id=None, item_id=None,
        ):
            if filename == "bad.txt":
                raise ValueError("not a transport error and not a TypeError")
            return {"filename": filename}

        with patch(
            "wcp_library.graph.sharepoint.upload_file", side_effect=_fake_upload_file
        ):
            files = [("/Docs", "ok.txt", b"a"), ("/Docs", "bad.txt", b"b")]
            with pytest.raises(ExceptionGroup) as exc_info:
                sharepoint.upload_multiple_files(HEADERS, SITE_ID, files)

        group = exc_info.value
        assert len(group.exceptions) == 1
        assert isinstance(group.exceptions[0], ValueError)

    def test_every_failure_is_collected_not_just_the_first(self):
        def _fake_upload_file(
            headers, site_id, file_path, filename, content, conflict_behavior="rename",
            *, drive_id=None, item_id=None,
        ):
            raise _http_error()

        with patch(
            "wcp_library.graph.sharepoint.upload_file", side_effect=_fake_upload_file
        ):
            files = [("/Docs", f"f{i}.txt", b"x") for i in range(4)]
            with pytest.raises(ExceptionGroup) as exc_info:
                sharepoint.upload_multiple_files(HEADERS, SITE_ID, files)

        group = exc_info.value
        assert len(group.exceptions) == 4
        assert "4 of 4 uploads failed" in str(group)
        # Notes are ordered by the input list, not by thread completion.
        notes = [e.__notes__[0] for e in group.exceptions]
        assert notes == [f"file: /Docs/f{i}.txt" for i in range(4)]

    def test_uses_item_id_for_destination_when_given(self):
        with patch(
            "wcp_library.graph.sharepoint.upload_file", return_value={"ok": True}
        ) as mock_upload:
            files = [(None, "a.txt", b"a")]
            sharepoint.upload_multiple_files(HEADERS, SITE_ID, files, item_id=ITEM_ID)
            mock_upload.assert_called_once_with(
                HEADERS,
                SITE_ID,
                None,
                "a.txt",
                b"a",
                "rename",
                drive_id=None,
                item_id=ITEM_ID,
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

    def test_raises_on_request_exception(self, tmp_path):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.download_file(
                    HEADERS, SITE_ID, "/Docs/report.xlsx", tmp_path
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

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.move_file(HEADERS, SITE_ID, "/a.txt", "/b")


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
                new_filename="new.txt",
                drive_id=None,
                item_id=None,
            )

    def test_propagates_exception_when_move_fails(self):
        with patch(
            "wcp_library.graph.sharepoint.move_file", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.rename_file(
                    HEADERS, SITE_ID, "/Docs/old.txt", "new.txt"
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

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.copy_file(HEADERS, SITE_ID, "/a.txt", "/b")

    def test_returns_none_when_graph_accepts_and_defers(self):
        # 202 Accepted: Graph is still running the copy; no JSON body.
        with patch(
            "wcp_library.graph.sharepoint._request",
            return_value=_ok_json({}, status_code=202, content=b""),
        ):
            result = sharepoint.copy_file(HEADERS, SITE_ID, "/a.txt", "/b")
            assert result is None

    def test_raises_value_error_without_destination(self):
        with pytest.raises(ValueError):
            sharepoint.copy_file(HEADERS, SITE_ID, "/a.txt")


class TestRemoveFile:
    def test_calls_delete_on_success(self):
        response = MagicMock()
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=response
        ) as mock_req:
            assert sharepoint.remove_file(HEADERS, SITE_ID, "/a.txt") is None
            assert _called_method(mock_req) == "DELETE"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drive/root:/a.txt"
            )

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.remove_file(HEADERS, SITE_ID, "/a.txt")


# ======================= List functions ======================= #


class TestGetLists:
    def test_returns_all_lists(self):
        page = _ok_json({"value": [{"id": "l1"}, {"id": "l2"}]})
        with patch(
            "wcp_library.graph._request", return_value=page
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
            "wcp_library.graph._request", side_effect=[page1, page2]
        ) as mock_req:
            result = sharepoint.get_lists(HEADERS, SITE_ID)
            assert result == [{"id": "a"}, {"id": "b"}]
            assert mock_req.call_count == 2

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.get_lists(HEADERS, SITE_ID)


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

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.get_list_metadata(HEADERS, SITE_ID, LIST_ID)


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

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.create_list(HEADERS, SITE_ID, "N")


class TestRemoveList:
    def test_calls_delete_on_success(self):
        response = MagicMock()
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=response
        ) as mock_req:
            assert sharepoint.remove_list(HEADERS, SITE_ID, LIST_ID) is None
            assert _called_method(mock_req) == "DELETE"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/lists/{LIST_ID}"
            )

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.remove_list(HEADERS, SITE_ID, LIST_ID)


class TestGetListItems:
    def test_returns_items_without_filter(self):
        page = _ok_json({"value": [{"id": "i1"}]})
        with patch(
            "wcp_library.graph._request", return_value=page
        ) as mock_req:
            result = sharepoint.get_list_items(HEADERS, SITE_ID, LIST_ID)
            assert result == [{"id": "i1"}]
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/lists/{LIST_ID}/items"
            )

    def test_appends_filter_to_url(self):
        page = _ok_json({"value": []})
        with patch(
            "wcp_library.graph._request", return_value=page
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
            "wcp_library.graph._request", side_effect=[page1, page2]
        ) as mock_req:
            result = sharepoint.get_list_items(HEADERS, SITE_ID, LIST_ID)
            assert result == [{"id": "a"}, {"id": "b"}]
            assert mock_req.call_count == 2

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.get_list_items(HEADERS, SITE_ID, LIST_ID)


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

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.get_list_item_metadata(
                    HEADERS, SITE_ID, LIST_ID, ITEM_ID
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

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.create_list_item(HEADERS, SITE_ID, LIST_ID, {"Title": "x"})


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

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.update_list_item(
                    HEADERS, SITE_ID, LIST_ID, ITEM_ID, {"Status": "x"}
                )


class TestRemoveListItem:
    def test_calls_delete_on_success(self):
        response = MagicMock()
        with patch(
            "wcp_library.graph.sharepoint._request", return_value=response
        ) as mock_req:
            assert (
                sharepoint.remove_list_item(HEADERS, SITE_ID, LIST_ID, ITEM_ID)
                is None
            )
            assert _called_method(mock_req) == "DELETE"
            assert _called_url(mock_req) == (
                f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}"
                f"/lists/{LIST_ID}/items/{ITEM_ID}"
            )

    def test_raises_on_request_exception(self):
        with patch(
            "wcp_library.graph.sharepoint._request", side_effect=_http_error()
        ):
            with pytest.raises(requests.RequestException):
                sharepoint.remove_list_item(HEADERS, SITE_ID, LIST_ID, ITEM_ID)