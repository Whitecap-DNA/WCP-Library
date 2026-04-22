"""Tests for wcp_library.logging.create_log."""
from __future__ import annotations

import logging as std_logging
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _preserve_root_logger():
    """Snapshot and restore root-logger state around each test."""
    root = std_logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    try:
        yield
    finally:
        for h in list(root.handlers):
            root.removeHandler(h)
        for h in original_handlers:
            root.addHandler(h)
        root.setLevel(original_level)


# ---------------------------------------------------------------------------
# create_log
# ---------------------------------------------------------------------------


class TestCreateLog:
    def test_zero_iterations_removes_existing_log(self, tmp_path: Path) -> None:
        log_path = tmp_path / "myproj.log"
        log_path.write_text("stale")

        from wcp_library.logging import create_log

        create_log(
            file_level=std_logging.DEBUG,
            console_level=std_logging.INFO,
            iterations=0,
            project_name="myproj",
            logging_dir=tmp_path,
        )

        # The .log file will exist because basicConfig re-creates it, but it should
        # be fresh (mode="w" truncates).
        assert log_path.exists()
        assert log_path.read_text() == ""

    def test_root_handlers_replaced(self, tmp_path: Path) -> None:
        from wcp_library.logging import create_log

        root = std_logging.getLogger()
        # Pre-seed with a garbage handler that should be dropped.
        root.addHandler(std_logging.NullHandler())

        create_log(
            file_level=std_logging.WARNING,
            console_level=std_logging.INFO,
            iterations=0,
            project_name="proj",
            logging_dir=tmp_path,
        )

        # There should be a file handler (from basicConfig) + stdout + stderr stream handlers
        stream_handlers = [
            h
            for h in root.handlers
            if isinstance(h, std_logging.StreamHandler)
            and not isinstance(h, std_logging.FileHandler)
        ]
        assert len(stream_handlers) == 2
        levels = sorted(h.level for h in stream_handlers)
        # stdout handler at INFO, stderr handler at max(INFO, WARNING) == WARNING
        assert std_logging.INFO in levels
        assert std_logging.WARNING in levels

    def test_stdout_filter_blocks_warning_and_above(self, tmp_path: Path) -> None:
        from wcp_library.logging import create_log

        create_log(
            file_level=std_logging.DEBUG,
            console_level=std_logging.DEBUG,
            iterations=0,
            project_name="filt",
            logging_dir=tmp_path,
        )

        root = std_logging.getLogger()
        # The stdout handler is the one pointed at sys.stdout; find any stream
        # handler with a level filter we can exercise.
        stream_handlers = [
            h
            for h in root.handlers
            if isinstance(h, std_logging.StreamHandler)
            and not isinstance(h, std_logging.FileHandler)
            and h.filters
        ]
        assert stream_handlers, "Expected at least one filtered stream handler"
        stdout_hdlr = stream_handlers[0]

        warning_record = std_logging.LogRecord(
            name="x",
            level=std_logging.WARNING,
            pathname=__file__,
            lineno=1,
            msg="warn",
            args=(),
            exc_info=None,
        )
        info_record = std_logging.LogRecord(
            name="x",
            level=std_logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="info",
            args=(),
            exc_info=None,
        )

        # Filter should accept INFO, reject WARNING.
        # stdlib logging accepts filters as either callables or objects with
        # .filter(record); call whichever shape we find.
        def _apply(filt, record):
            return filt(record) if callable(filt) and not hasattr(filt, "filter") else filt.filter(record)

        assert all(_apply(f, info_record) for f in stdout_hdlr.filters)
        assert not all(_apply(f, warning_record) for f in stdout_hdlr.filters)

    def test_iterations_rotate_existing_files(self, tmp_path: Path) -> None:
        (tmp_path / "proj.log").write_text("current")
        (tmp_path / "proj_1.log").write_text("older")

        from wcp_library.logging import create_log

        create_log(
            file_level=std_logging.INFO,
            console_level=std_logging.INFO,
            iterations=2,
            project_name="proj",
            logging_dir=tmp_path,
        )

        # old proj_1.log should now contain "current" (current got renamed to proj_1)
        assert (tmp_path / "proj_1.log").read_text() == "current"
        # proj_2.log should contain what was proj_1
        assert (tmp_path / "proj_2.log").read_text() == "older"
        # proj.log will be re-created fresh by basicConfig
        assert (tmp_path / "proj.log").exists()

    def test_reconfigure_unsupported_falls_back(self, tmp_path: Path) -> None:
        """If sys.stdout.reconfigure raises, the function should swallow it."""
        from wcp_library.logging import create_log

        with patch("wcp_library.logging.sys") as mock_sys:
            mock_sys.stdout.reconfigure.side_effect = AttributeError("nope")
            # Still need real streams for StreamHandler; provide them.
            import sys as real_sys

            mock_sys.stdout = real_sys.stdout
            mock_sys.stderr = real_sys.stderr
            # Make reconfigure raise again on the (now real) stdout.
            with patch.object(
                real_sys.stdout, "reconfigure", side_effect=AttributeError("nope"), create=True
            ):
                create_log(
                    file_level=std_logging.INFO,
                    console_level=std_logging.INFO,
                    iterations=0,
                    project_name="fb",
                    logging_dir=tmp_path,
                )

        assert (tmp_path / "fb.log").exists()
