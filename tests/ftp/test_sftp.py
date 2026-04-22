"""Tests for wcp_library.ftp.sftp."""
from __future__ import annotations

import stat
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def _make_sftp_no_vault() -> "object":
    """Build an SFTP without credentials; no real connection is made."""
    from wcp_library.ftp.sftp import SFTP

    with patch("wcp_library.ftp.sftp.paramiko.SSHClient") as mock_ssh_cls:
        mock_ssh_cls.return_value = MagicMock()
        sftp = SFTP(host="sftp.example.com", port=22)
    return sftp


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestSFTPInit:
    def test_init_without_credentials(self) -> None:
        from wcp_library.ftp.sftp import SFTP

        with patch("wcp_library.ftp.sftp.paramiko.SSHClient") as mock_ssh_cls:
            mock_ssh = MagicMock()
            mock_ssh_cls.return_value = mock_ssh

            sftp = SFTP(host="h", port=22)

            mock_ssh_cls.assert_called_once()
            mock_ssh.set_missing_host_key_policy.assert_called_once()
            mock_ssh.connect.assert_not_called()
            assert sftp.sftp_connection is None

    def test_init_with_credentials_connects(self) -> None:
        from wcp_library.ftp.sftp import SFTP

        vault = {
            "Host": "vault-host",
            "Port": 2222,
            "UserName": "u",
            "Password": "p",
        }

        with patch("wcp_library.ftp.sftp.paramiko.SSHClient") as mock_ssh_cls:
            mock_ssh = MagicMock()
            mock_ssh_cls.return_value = mock_ssh
            mock_sftp = MagicMock()
            mock_ssh.open_sftp.return_value = mock_sftp

            sftp = SFTP(password_vault_dict=vault)

            mock_ssh.connect.assert_called_once_with("vault-host", 2222, "u", "p")
            mock_ssh.open_sftp.assert_called_once()
            assert sftp.sftp_connection is mock_sftp


# ---------------------------------------------------------------------------
# login
# ---------------------------------------------------------------------------


class TestSFTPLogin:
    def test_login_connects_and_opens_sftp(self) -> None:
        sftp = _make_sftp_no_vault()
        sftp.ssh = MagicMock()
        fake_sftp = MagicMock()
        sftp.ssh.open_sftp.return_value = fake_sftp

        sftp.login("user", "pw")

        sftp.ssh.connect.assert_called_once_with("sftp.example.com", 22, "user", "pw")
        sftp.ssh.open_sftp.assert_called_once()
        assert sftp.sftp_connection is fake_sftp

    def test_login_propagates_errors(self) -> None:
        sftp = _make_sftp_no_vault()
        sftp.ssh = MagicMock()
        sftp.ssh.connect.side_effect = RuntimeError("auth")

        with pytest.raises(RuntimeError):
            sftp.login("user", "pw")


# ---------------------------------------------------------------------------
# download / upload
# ---------------------------------------------------------------------------


class TestSFTPDownload:
    def test_download_calls_get(self, tmp_path: Path) -> None:
        sftp = _make_sftp_no_vault()
        sftp.sftp_connection = MagicMock()

        remote = Path("/remote/file.txt")
        local = tmp_path / "nested" / "file.txt"

        sftp.download(remote, local)

        assert local.parent.exists()
        sftp.sftp_connection.get.assert_called_once_with(str(remote), local)

    def test_download_error_propagates(self, tmp_path: Path) -> None:
        sftp = _make_sftp_no_vault()
        sftp.sftp_connection = MagicMock()
        sftp.sftp_connection.get.side_effect = IOError("boom")

        with pytest.raises(IOError):
            sftp.download(Path("/r"), tmp_path / "l.txt")


class TestSFTPUpload:
    def test_upload_calls_put(self) -> None:
        sftp = _make_sftp_no_vault()
        sftp.sftp_connection = MagicMock()

        sftp.upload(Path("local.txt"), Path("/remote/out.txt"))
        sftp.sftp_connection.put.assert_called_once_with(
            Path("local.txt"), str(Path("/remote/out.txt"))
        )

    def test_upload_error_propagates(self) -> None:
        sftp = _make_sftp_no_vault()
        sftp.sftp_connection = MagicMock()
        sftp.sftp_connection.put.side_effect = IOError("boom")

        with pytest.raises(IOError):
            sftp.upload(Path("l.txt"), Path("/r.txt"))


# ---------------------------------------------------------------------------
# list_files / list_dirs / change_dir
# ---------------------------------------------------------------------------


class TestSFTPListing:
    def _mk_stat(self, mode: int) -> MagicMock:
        s = MagicMock()
        s.st_mode = mode
        return s

    def test_list_files(self) -> None:
        sftp = _make_sftp_no_vault()
        conn = MagicMock()
        conn.listdir.return_value = ["a.txt", "some_dir", "b.txt"]

        stats = {
            "a.txt": self._mk_stat(stat.S_IFREG),
            "some_dir": self._mk_stat(stat.S_IFDIR),
            "b.txt": self._mk_stat(stat.S_IFREG),
        }
        conn.lstat.side_effect = lambda name: stats[name]
        sftp.sftp_connection = conn

        result = sftp.list_files()

        assert result == [Path("a.txt"), Path("b.txt")]

    def test_list_dirs(self) -> None:
        sftp = _make_sftp_no_vault()
        conn = MagicMock()
        conn.listdir.return_value = ["a.txt", "some_dir"]

        stats = {
            "a.txt": self._mk_stat(stat.S_IFREG),
            "some_dir": self._mk_stat(stat.S_IFDIR),
        }
        conn.lstat.side_effect = lambda name: stats[name]
        sftp.sftp_connection = conn

        result = sftp.list_dirs()
        assert result == [Path("some_dir")]

    def test_change_dir_uses_string_path(self) -> None:
        sftp = _make_sftp_no_vault()
        sftp.sftp_connection = MagicMock()

        sftp.change_dir(Path("/some/path"))
        sftp.sftp_connection.chdir.assert_called_once_with(str(Path("/some/path")))


# ---------------------------------------------------------------------------
# download_files
# ---------------------------------------------------------------------------


class TestSFTPDownloadFiles:
    def test_downloads_only_matching(self, tmp_path: Path) -> None:
        sftp = _make_sftp_no_vault()
        conn = MagicMock()
        conn.listdir.return_value = ["a.csv", "b.txt", "c.csv"]

        reg = MagicMock()
        reg.st_mode = stat.S_IFREG
        conn.lstat.return_value = reg
        sftp.sftp_connection = conn

        local_dir = tmp_path / "local"
        sftp.download_files(local_dir, regex_pattern=r".*\.csv$")

        downloaded_sources = [c.args[0] for c in conn.get.call_args_list]
        assert any("a.csv" in s for s in downloaded_sources)
        assert any("c.csv" in s for s in downloaded_sources)
        assert all("b.txt" not in s for s in downloaded_sources)

    def test_no_matches_no_downloads(self, tmp_path: Path) -> None:
        sftp = _make_sftp_no_vault()
        conn = MagicMock()
        conn.listdir.return_value = ["a.csv"]
        reg = MagicMock()
        reg.st_mode = stat.S_IFREG
        conn.lstat.return_value = reg
        sftp.sftp_connection = conn

        sftp.download_files(tmp_path / "local", regex_pattern=r"zzz")
        conn.get.assert_not_called()


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


class TestSFTPClose:
    def test_close_clears_connection(self) -> None:
        sftp = _make_sftp_no_vault()
        conn = MagicMock()
        sftp.sftp_connection = conn

        sftp.close()

        conn.close.assert_called_once()
        assert sftp.sftp_connection is None
