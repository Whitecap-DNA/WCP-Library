"""Mock tests for wcp_library.credentials shared helpers.

Covers generate_password and MissingCredentialsError in
``wcp_library/credentials/__init__.py``. No real I/O.
"""
import string

import pytest

from wcp_library.credentials import MissingCredentialsError, generate_password


class TestMissingCredentialsError:
    def test_inherits_key_error(self):
        assert issubclass(MissingCredentialsError, KeyError)

    def test_is_raisable(self):
        with pytest.raises(MissingCredentialsError):
            raise MissingCredentialsError("missing")

    def test_caught_as_key_error(self):
        with pytest.raises(KeyError):
            raise MissingCredentialsError("missing")


class TestGeneratePasswordHappyPath:
    def test_default_length(self):
        pwd = generate_password()
        assert len(pwd) == 12

    def test_custom_length(self):
        pwd = generate_password(length=20)
        assert len(pwd) == 20

    def test_first_char_never_digit(self):
        # Run enough times to catch accidental digit-leading outputs
        for _ in range(50):
            pwd = generate_password(length=8)
            assert not pwd[0].isdigit()

    def test_contains_digit_when_forced(self):
        pwd = generate_password(length=16, use_nums=True, force_num=True,
                                use_special=False, force_spec=False)
        assert any(c.isdigit() for c in pwd)

    def test_contains_special_when_forced(self):
        pwd = generate_password(length=16, use_nums=False, force_num=False,
                                use_special=True, force_spec=True)
        assert any(c in string.punctuation for c in pwd)

    def test_no_digit_when_not_allowed(self):
        pwd = generate_password(length=16, use_nums=False, force_num=False,
                                use_special=False, force_spec=False)
        assert not any(c.isdigit() for c in pwd)
        assert not any(c in string.punctuation for c in pwd)

    def test_special_chars_override_used(self):
        # Only allow a small custom charset of special characters
        override = "@#"
        pwd = generate_password(length=20, use_nums=False, force_num=False,
                                use_special=True, force_spec=True,
                                special_chars_override=override)
        # Every special char present must be in override (letters also allowed)
        for ch in pwd:
            if not ch.isalpha():
                assert ch in override
        # At least one special from the override
        assert any(c in override for c in pwd)

    def test_length_one_allowed(self):
        # length=1 with no forced constraints should still succeed
        pwd = generate_password(length=1, use_nums=False, force_num=False,
                                use_special=False, force_spec=False)
        assert len(pwd) == 1
        # First char can't be digit, we forbade digits anyway
        assert pwd.isalpha()


class TestGeneratePasswordErrors:
    def test_length_zero_raises(self):
        with pytest.raises(ValueError, match="at least 1"):
            generate_password(length=0)

    def test_length_negative_raises(self):
        with pytest.raises(ValueError, match="at least 1"):
            generate_password(length=-5)

    def test_force_num_without_nums_raises(self):
        with pytest.raises(ValueError, match="Cannot force"):
            generate_password(use_nums=False, force_num=True)

    def test_force_spec_without_special_raises(self):
        with pytest.raises(ValueError, match="Cannot force"):
            generate_password(use_special=False, force_spec=True)

    def test_unachievable_forces_exhaust_attempts(self):
        # length=1, force both digit and special -> impossible in one char
        with pytest.raises(ValueError, match="Unable to generate"):
            generate_password(length=1, use_nums=True, use_special=True,
                              force_num=True, force_spec=True,
                              max_attempts=10)
