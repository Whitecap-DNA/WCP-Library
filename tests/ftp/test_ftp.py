"""Tests for wcp_library.ftp.ftp."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _make_ftp_no_vault() -> "object":
    """Build an FTP instance without triggering a real connection."""
    from wcp_library.ftp.ftp import FTP

    with patch("wcp_library.ftp.ftp.ftputil.session.session_factory"):
        return FTP(host="ftp.example.com", port=21)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestFTPInit:
    def test_init_without_credentials_does_not_connect(self) -> None:
        from wcp_library.ftp.ftp import FTP

        with patch("wcp_library.ftp.ftp.ftputil.FTPHost") as mock_host, patch(
            "wcp_library.ftp.ftp.ftputil.session.session_factory"
        ):
            ftp = FTP(host="h", port=2121)

            assert ftp.host == "h"
            assert ftp.port == 2121
            assert ftp.ftp_connection is None
            mock_host.assert_not_called()

    def test_init_with_password_vault_dict_connects(self) -> None:
        from wcp_library.ftp.ftp import FTP

        vault = {
            "Host": "vault-host",
            "Port": 2121,
            "UserName": "u",
            "Password": "p",
        }

        with patch("wcp_library.ftp.ftp.ftputil.FTPHost") as mock_host_cls, patch(
            "wcp_library.ftp.ftp.ftputil.session.session_factory"
        ):
            ftp = FTP(password_vault_dict=vault)

            assert ftp.host == "vault-host"
            assert ftp.port == 2121
            mock_host_cls.assert_called_once_with("vault-host", "u", "p")
            assert ftp.ftp_connection is mock_host_cls.return_value


# ---------------------------------------------------------------------------
# login
# ---------------------------------------------------------------------------


class TestFTPLogin:
    def test_login_creates_ftphost(self) -> None:
        ftp = _make_ftp_no_vault()

        with patch("wcp_library.ftp.ftp.ftputil.FTPHost") as mock_host_cls:
            ftp.login("user", "pass")
            mock_host_cls.assert_called_once()
            # First 3 positional args should be host / user / pass
            args, _ = mock_host_cls.call_args
            assert args[0] == "ftp.example.com"
            assert args[1] == "user"
            assert args[2] == "pass"

    def test_login_propagates_errors(self) -> None:
        ftp = _make_ftp_no_vault()

        with patch("wcp_library.ftp.ftp.ftputil.FTPHost") as mock_host_cls:
            mock_host_cls.side_effect = RuntimeError("no route")
            with pytest.raises(RuntimeError):
                ftp.login("u", "p")


# ---------------------------------------------------------------------------
# download / upload
# ---------------------------------------------------------------------------


class TestFTPDownload:
    def test_download_calls_connection_download(self, tmp_path: Path) -> None:
        ftp = _make_ftp_no_vault()
        ftp.ftp_connection = MagicMock()

        local_file = tmp_path / "subdir" / "out.txt"
        remote_file = Path("/remote/in.txt")

        ftp.download(remote_file, local_file)

        assert local_file.parent.exists()
        ftp.ftp_connection.download.assert_called_once_with(remote_file, local_file)

    def test_download_error_propagates(self, tmp_path: Path) -> None:
        ftp = _make_ftp_no_vault()
        ftp.ftp_connection = MagicMock()
        ftp.ftp_connection.download.side_effect = OSError("nope")

        with pytest.raises(OSError):
            ftp.download(Path("/r"), tmp_path / "l.txt")


class TestFTPUpload:
    def test_upload_calls_connection_upload(self) -> None:
        ftp = _make_ftp_no_vault()
        ftp.ftp_connection = MagicMock()

        ftp.upload(Path("local.txt"), Path("/remote/out.txt"))
        ftp.ftp_connection.upload.assert_called_once_with(
            Path("local.txt"), Path("/remote/out.txt")
        )

    def test_upload_error_propagates(self) -> None:
        ftp = _make_ftp_no_vault()
        ftp.ftp_connection = MagicMock()
        ftp.ftp_connection.upload.side_effect = OSError("nope")

        with pytest.raises(OSError):
            ftp.upload(Path("l.txt"), Path("/r.txt"))


# ---------------------------------------------------------------------------
# list_files / list_dirs / change_dir
# ---------------------------------------------------------------------------


class TestFTPListing:
    def test_list_files_filters_via_isfile(self) -> None:
        ftp = _make_ftp_no_vault()
        conn = MagicMock()
        conn.curdir = "."
        conn.listdir.return_value = ["one.txt", "two.txt", "dir"]
        conn.path.isfile.side_effect = lambda x: x.endswith(".txt")
        ftp.ftp_connection = conn

        result = ftp.list_files()

        conn.listdir.assert_called_once_with(".")
        assert result == [Path("one.txt"), Path("two.txt")]

    def test_list_dirs_filters_via_isdir(self) -> None:
        ftp = _make_ftp_no_vault()
        conn = MagicMock()
        conn.curdir = "."
        conn.listdir.return_value = ["one.txt", "dir"]
        conn.path.isfile.side_effect = lambda x: x.endswith(".txt")
        conn.path.isdir.side_effect = lambda x: x == "dir"
        ftp.ftp_connection = conn

        result = ftp.list_dirs()

        assert result == [Path("dir")]

    def test_change_dir_forwards(self) -> None:
        ftp = _make_ftp_no_vault()
        ftp.ftp_connection = MagicMock()

        ftp.change_dir(Path("/foo"))
        ftp.ftp_connection.chdir.assert_called_once_with(Path("/foo"))


# ---------------------------------------------------------------------------
# download_files
# ---------------------------------------------------------------------------


class TestFTPDownloadFiles:
    def test_downloads_only_matching_files(self, tmp_path: Path) -> None:
        ftp = _make_ftp_no_vault()
        conn = MagicMock()
        conn.curdir = "."
        conn.listdir.return_value = ["a.csv", "b.txt", "c.csv"]
        conn.path.isfile.return_value = True
        ftp.ftp_connection = conn

        local_dir = tmp_path / "local"
        ftp.download_files(local_dir, regex_pattern=r".*\.csv$")

        # b.txt should not be downloaded
        downloaded_names = [
            call.args[0].name for call in conn.download.call_args_list
        ]
        assert "a.csv" in downloaded_names
        assert "c.csv" in downloaded_names
        assert "b.txt" not in downloaded_names
        assert local_dir.exists()

    def test_download_files_no_matches(self, tmp_path: Path) -> None:
        ftp = _make_ftp_no_vault()
        conn = MagicMock()
        conn.curdir = "."
        conn.listdir.return_value = ["a.csv"]
        conn.path.isfile.return_value = True
        ftp.ftp_connection = conn

        ftp.download_files(tmp_path / "loc", regex_pattern=r"zzz")
        conn.download.assert_not_called()


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


class TestFTPClose:
    def test_close_clears_connection(self) -> None:
        ftp = _make_ftp_no_vault()
        conn = MagicMock()
        ftp.ftp_connection = conn

        ftp.close()

        conn.close.assert_called_once()
        assert ftp.ftp_connection is None
