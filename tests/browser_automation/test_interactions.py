"""Mock tests for the error-screenshot path in
``wcp_library/browser_automation/interactions.py``.

No real WebDriver and no real Graph calls: the driver is a mock and
``upload_file`` is patched.
"""
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from wcp_library.browser_automation.interactions import Interactions

MODULE = "wcp_library.browser_automation.interactions"
SITE_ID = "contoso.sharepoint.com,abc-123,def-456"
SCREENSHOT_FOLDER = "/Automation/.Execution Error Screenshots"
FILENAME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}\.png$")


def _driver(png=b"png-bytes"):
    driver = MagicMock()
    driver.get_screenshot_as_png.return_value = png
    return driver


class TestErrorScreenshotToSharePoint:
    def test_uploads_with_the_credentials_from_the_config(self):
        credentials = MagicMock(name="GraphCredentials")
        interactions = Interactions(
            driver=_driver(),
            sharepoint_config={"credentials": credentials, "site_id": SITE_ID},
        )

        with patch(f"{MODULE}.upload_file") as mock_upload:
            interactions._take_error_screenshot()

        mock_upload.assert_called_once()
        kwargs = mock_upload.call_args.kwargs
        # The credentials object is passed through, not a headers dict built
        # from secrets, so the upload gains token-expiry handling.
        assert kwargs["headers"] is credentials
        assert kwargs["site_id"] == SITE_ID
        assert kwargs["file_path"] == SCREENSHOT_FOLDER
        assert kwargs["content"] == b"png-bytes"
        assert FILENAME_PATTERN.match(kwargs["filename"])

    def test_missing_credentials_key_raises_keyerror(self):
        interactions = Interactions(
            driver=_driver(), sharepoint_config={"site_id": SITE_ID}
        )

        with patch(f"{MODULE}.upload_file") as mock_upload:
            with pytest.raises(KeyError, match="credentials"):
                interactions._take_error_screenshot()

        mock_upload.assert_not_called()

    def test_missing_site_id_raises_keyerror(self):
        interactions = Interactions(
            driver=_driver(),
            sharepoint_config={"credentials": MagicMock()},
        )

        with patch(f"{MODULE}.upload_file") as mock_upload:
            with pytest.raises(KeyError, match="site_id"):
                interactions._take_error_screenshot()

        mock_upload.assert_not_called()

    def test_upload_failure_propagates(self):
        # A screenshot upload that fails should not be swallowed; the caller is
        # already handling an error and needs to see this one too.
        interactions = Interactions(
            driver=_driver(),
            sharepoint_config={"credentials": MagicMock(), "site_id": SITE_ID},
        )

        with patch(f"{MODULE}.upload_file", side_effect=RuntimeError("graph down")):
            with pytest.raises(RuntimeError, match="graph down"):
                interactions._take_error_screenshot()


class TestErrorScreenshotToDisk:
    def test_saves_locally_when_no_sharepoint_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        driver = _driver()
        interactions = Interactions(driver=driver, sharepoint_config=None)

        with patch(f"{MODULE}.upload_file") as mock_upload:
            interactions._take_error_screenshot()

        mock_upload.assert_not_called()
        folder = tmp_path / "Execution Error Screenshots"
        assert folder.is_dir()

        driver.save_screenshot.assert_called_once()
        saved = driver.save_screenshot.call_args.args[0]
        # The path handed to the driver is relative to the working directory.
        assert saved.parent == Path("Execution Error Screenshots")
        assert FILENAME_PATTERN.match(saved.name)
        assert (folder / saved.name).parent == folder

    def test_no_screenshot_is_captured_for_the_local_path(self, tmp_path, monkeypatch):
        # The local path writes via the driver rather than pulling the PNG into
        # memory first, so get_screenshot_as_png is not used.
        monkeypatch.chdir(tmp_path)
        driver = _driver()
        Interactions(driver=driver, sharepoint_config=None)._take_error_screenshot()

        driver.get_screenshot_as_png.assert_not_called()
