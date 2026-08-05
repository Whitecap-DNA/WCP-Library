# Spec: add Windows credential manager

**Status:** Approved — ready for implementation plan
**Date:** 2026-08-05
**Author:** Mitch Petersen (with Claude)
**Scope:** new `wcp_library/credentials/windows.py`; new `tests/credentials/test_windows.py`; wiki doc updates for `Credentials - Expected Dictionaries`, `Credentials ‐ Synchronous`, `Credentials - Asynchronous`

## Motivation

The credentials package has a manager per Vault password list (`internet.py`, `ftp.py`, `postgres.py`, `oracle.py`, `api.py`), each a thin sync/async subclass pair over `CredentialManager` / `AsyncCredentialManager`. There is no manager for the Windows password list (Vault `PasswordListID` 91). This spec adds one, following the existing pattern exactly.

## Goals

- `WindowsCredentialManager` and `AsyncWindowsCredentialManager` in `wcp_library/credentials/windows.py`, `PasswordListID` 91.
- `new_credentials` payload carries only `PasswordListID`, `Title`, `Notes`, `UserName`, `Password` — no `GenericField*` entries, since Windows credentials have no extra fields (unlike FTP/Postgres/Oracle's Host/Port/etc.).
- Behavior identical to `internet.py` minus the `URL` field: `Title` defaults to `UserName.upper()` unless provided; `Notes` defaults to `None` unless provided; `UserName` is lowercased on write.
- Test coverage mirroring `tests/credentials/test_internet.py`.
- Wiki docs updated so Windows appears alongside the other five manager types.

## Non-goals

- No changes to `get_credentials`, `get_credential_from_id`, or `update_credential` — these are inherited unchanged from the base classes.
- No `GenericField*` fields for Windows now or anticipated later; if that changes it's a new spec.
- No changes to `wcp_library/credentials/__init__.py` — it exports only `MissingCredentialsError` and `generate_password`; individual managers are imported directly by consumers, matching the existing pattern (none of `internet`/`ftp`/etc. are re-exported either).

## Architecture

### `wcp_library/credentials/windows.py`

Direct copy of `internet.py`'s structure with the `URL` field removed and `PasswordListID` changed to 91:

```python
import logging

from wcp_library.credentials._credential_manager_asynchronous import AsyncCredentialManager
from wcp_library.credentials._credential_manager_synchronous import CredentialManager

logger = logging.getLogger(__name__)


class WindowsCredentialManager(CredentialManager):
    def __init__(self, api_key: str):
        super().__init__(api_key, 91)

    def new_credentials(self, credentials_dict: dict) -> bool:
        """
        Create a new credential entry

        Credentials dictionary must have the following keys:
            - UserName
            - Password

        :param credentials_dict:
        :return:
        """

        data = {
            "PasswordListID": self._password_list_id,
            "Title": credentials_dict['UserName'].upper() if "Title" not in credentials_dict else credentials_dict['Title'].upper(),
            "Notes": credentials_dict['Notes'] if 'Notes' in credentials_dict else None,
            "UserName": credentials_dict['UserName'].lower(),
            "Password": credentials_dict['Password'],
        }

        return self._publish_new_password(data)


class AsyncWindowsCredentialManager(AsyncCredentialManager):
    def __init__(self, api_key: str):
        super().__init__(api_key, 91)

    async def new_credentials(self, credentials_dict: dict) -> bool:
        """
        Create a new credential entry

        Credentials dictionary must have the following keys:
            - UserName
            - Password

        :param credentials_dict:
        :return:
        """

        data = {
            "PasswordListID": self._password_list_id,
            "Title": credentials_dict['UserName'].upper() if "Title" not in credentials_dict else credentials_dict['Title'].upper(),
            "Notes": credentials_dict['Notes'] if 'Notes' in credentials_dict else None,
            "UserName": credentials_dict['UserName'].lower(),
            "Password": credentials_dict['Password'],
        }

        return await self._publish_new_password(data)
```

### `tests/credentials/test_windows.py`

Mirrors `test_internet.py`'s four test classes (`TestWindowsConstruction`, `TestWindowsNewCredentials`, `TestAsyncWindowsConstruction`, `TestAsyncWindowsNewCredentials`), with `_sample()` carrying only `UserName`/`Password` (no `URL`), and assertions checking `_password_list_id == 91` and the payload keys above. Covers: password_list_id value, happy-path payload, title override, notes included, missing-password raises `KeyError`, publish-false propagates, async happy path, async missing-field raises.

### Wiki docs

- **Credentials - Expected Dictionaries**: add a `## Windows` section listing `PasswordID (not needed in new_credentials method)`, `UserName`, `Password`.
- **Credentials ‐ Synchronous** / **Credentials - Asynchronous**: add a `### Windows` subsection under "Manager Creation" with the corresponding import/instantiation snippet, matching the Internet entry's format.

## API changes

### Added

- `wcp_library/credentials/windows.py` exporting `WindowsCredentialManager`, `AsyncWindowsCredentialManager`.

### Unchanged

- Everything else in the credentials package.

## Testing

- New `tests/credentials/test_windows.py` passes.
- Full existing test suite stays green (no shared code is touched).

## Open questions

None.