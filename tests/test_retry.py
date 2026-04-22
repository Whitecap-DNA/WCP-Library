"""Tests for wcp_library.retry (tenacity strategy configs)."""
from unittest.mock import MagicMock

import pytest

from wcp_library.retry import _extract_full_code


class TestExtractFullCode:
    def test_returns_code_when_error_obj_has_full_code(self):
        err_obj = MagicMock()
        err_obj.full_code = "08001"
        exc = Exception(err_obj)
        assert _extract_full_code(exc) == "08001"

    def test_returns_none_when_args_is_a_string(self):
        assert _extract_full_code(Exception("plain string")) is None

    def test_returns_none_when_args_is_empty(self):
        assert _extract_full_code(Exception()) is None

    def test_returns_none_when_args_has_multiple_items(self):
        assert _extract_full_code(Exception("a", "b")) is None

    def test_returns_none_when_error_obj_has_no_full_code(self):
        err_obj = object()  # no full_code attr
        assert _extract_full_code(Exception(err_obj)) is None
