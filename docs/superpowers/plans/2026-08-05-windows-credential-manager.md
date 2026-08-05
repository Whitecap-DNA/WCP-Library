# Windows Credential Manager Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `WindowsCredentialManager` / `AsyncWindowsCredentialManager` pair to `wcp_library.credentials`, following the exact pattern of the existing managers (`internet.py`, `ftp.py`, `postgres.py`, `oracle.py`, `api.py`).

**Architecture:** One new module, `wcp_library/credentials/windows.py`, with a sync class subclassing `CredentialManager` and an async class subclassing `AsyncCredentialManager`. Both hardcode Vault `PasswordListID` 91 and implement `new_credentials` with a payload of just `PasswordListID`, `Title`, `Notes`, `UserName`, `Password` — no `GenericField*` entries. Tests mirror `tests/credentials/test_internet.py`. Three wiki docs gain a "Windows" entry alongside the other five manager types.

**Tech Stack:** Python, pytest + pytest-asyncio (`asyncio_mode = "auto"`, no extra decorators needed on `async def test_*`), `unittest.mock.patch.object`.

## Global Constraints

- rST-style docstrings on all public methods (per project CLAUDE.md) — match the existing managers' docstring format exactly (`:param:`/`:return:` fields, blank line preserved after the closing `"""`).
- Vault `PasswordListID` for Windows is **91** (confirmed by user — do not use any other value).
- No `GenericField*` keys in the payload — Windows credentials only ever carry Title/UserName/Password/Notes.
- `wcp_library/credentials/__init__.py` is NOT modified — individual managers are imported directly by consumers (`from wcp_library.credentials.windows import ...`), matching every existing manager.

---

### Task 1: `WindowsCredentialManager` + `AsyncWindowsCredentialManager`

**Files:**
- Create: `wcp_library/credentials/windows.py`
- Test: `tests/credentials/test_windows.py`

**Interfaces:**
- Consumes: `wcp_library.credentials._credential_manager_synchronous.CredentialManager` (`__init__(self, api_key: str, password_list_id: int)`, `_publish_new_password(self, data: dict) -> bool`, `self._password_list_id`); `wcp_library.credentials._credential_manager_asynchronous.AsyncCredentialManager` (async equivalents). Both already exist and are unmodified.
- Produces: `WindowsCredentialManager(api_key: str)` and `AsyncWindowsCredentialManager(api_key: str)`, each with `new_credentials(credentials_dict: dict) -> bool` (async on the async class). `credentials_dict` must contain `UserName` and `Password`; `Title` and `Notes` are optional.

- [ ] **Step 1: Write the failing test file**

Create `tests/credentials/test_windows.py`:

```python
"""Mock tests for wcp_library.credentials.windows."""
from unittest.mock import patch

import pytest

from wcp_library.credentials.windows import (
    AsyncWindowsCredentialManager,
    WindowsCredentialManager,
)


def _sample(**overrides):
    base = {
        "UserName": "Alice",
        "Password": "hunter2",
    }
    base.update(overrides)
    return base


class TestWindowsConstruction:
    def test_password_list_id_is_91(self):
        mgr = WindowsCredentialManager("k")
        assert mgr._password_list_id == 91


class TestWindowsNewCredentials:
    def test_happy_path_payload(self):
        mgr = WindowsCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            ok = mgr.new_credentials(_sample())
        assert ok is True
        p = mock_pub.call_args.args[0]
        assert p["PasswordListID"] == 91
        assert p["Title"] == "ALICE"
        assert p["UserName"] == "alice"  # lowered
        assert p["Password"] == "hunter2"
        assert p["Notes"] is None

    def test_title_override(self):
        mgr = WindowsCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Title="workstation"))
        assert mock_pub.call_args.args[0]["Title"] == "WORKSTATION"

    def test_notes_included(self):
        mgr = WindowsCredentialManager("k")
        with patch.object(mgr, "_publish_new_password",
                          return_value=True) as mock_pub:
            mgr.new_credentials(_sample(Notes="some note"))
        assert mock_pub.call_args.args[0]["Notes"] == "some note"

    def test_missing_password_raises(self):
        mgr = WindowsCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=True):
            creds = _sample()
            creds.pop("Password")
            with pytest.raises(KeyError):
                mgr.new_credentials(creds)

    def test_publish_false_propagates(self):
        mgr = WindowsCredentialManager("k")
        with patch.object(mgr, "_publish_new_password", return_value=False):
            assert mgr.new_credentials(_sample()) is False


class TestAsyncWindowsConstruction:
    def test_password_list_id_is_91(self):
        assert AsyncWindowsCredentialManager("k")._password_list_id == 91


class TestAsyncWindowsNewCredentials:
    async def test_happy_path(self):
        mgr = AsyncWindowsCredentialManager("k")

        async def fake_publish(data):
            fake_publish.payload = data
            return True

        with patch.object(mgr, "_publish_new_password", fake_publish):
            ok = await mgr.new_credentials(_sample())
        assert ok is True
        p = fake_publish.payload
        assert p["Title"] == "ALICE"
        assert p["UserName"] == "alice"

    async def test_missing_password_raises(self):
        mgr = AsyncWindowsCredentialManager("k")

        async def fake_publish(data):
            return True

        creds = _sample()
        creds.pop("Password")
        with patch.object(mgr, "_publish_new_password", fake_publish):
            with pytest.raises(KeyError):
                await mgr.new_credentials(creds)
```

- [ ] **Step 2: Run tests to verify they fail on import**

Run: `poetry run pytest tests/credentials/test_windows.py -v`
Expected: FAIL/ERROR — `ModuleNotFoundError: No module named 'wcp_library.credentials.windows'`

- [ ] **Step 3: Write the implementation**

Create `wcp_library/credentials/windows.py`:

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

- [ ] **Step 4: Run tests to verify they pass**

Run: `poetry run pytest tests/credentials/test_windows.py -v`
Expected: all tests PASS (10 tests: 1 sync construction + 5 sync new_credentials + 1 async construction + 2 async new_credentials — count them against the file above; all green).

- [ ] **Step 5: Run the full test suite to confirm no regressions**

Run: `poetry run pytest`
Expected: all tests PASS, same pass count as before plus the new ones (this task touches no shared/base code, so no existing test should change behavior).

- [ ] **Step 6: Commit**

```bash
git add wcp_library/credentials/windows.py tests/credentials/test_windows.py
git commit -m "feat: add Windows credential manager"
```

---

### Task 2: Update wiki docs

**Files:**
- Modify: `docs/Wiki Docs/Credentials - Expected Dictionaries`
- Modify: `docs/Wiki Docs/Credentials ‐ Synchronous`
- Modify: `docs/Wiki Docs/Credentials - Asynchronous`

**Interfaces:**
- Consumes: `WindowsCredentialManager` / `AsyncWindowsCredentialManager` from Task 1 (names only, for the import snippets below).
- Produces: nothing consumed by later tasks — this is the last task in the plan.

- [ ] **Step 1: Add the Windows section to `Credentials - Expected Dictionaries`**

The file lists one `## <Type>` section per manager, each with a bullet list of dict keys, in this order: API, FTP/SFTP, Internet, Oracle, Postgres. Insert a new `## Windows` section. Alphabetically it sits after Postgres (last), so append it at the end of the file:

```markdown

## Windows

Dictionary:
* PasswordID (not needed in new_credentials method)
* UserName
* Password
```

(Add exactly one blank line before the `## Windows` heading, matching the spacing between the existing sections.)

- [ ] **Step 2: Add the Windows entry to `Credentials ‐ Synchronous`**

Under the `## Manager Creation` heading, the file has one `### <Type>` subsection per manager in this order: API, FTP/SFTP, Internet, Oracle, Postgres. Insert a new `### Windows` subsection after `### Postgres` (i.e., immediately before the `## Methods` heading):

````markdown
### Windows

```
from wcp_library.credentials.windows import WindowsCredentialManager

CredentialsManager = WindowsCredentialManager(<API_KEY>)
```
````

(Match the existing triple-backtick fencing style used by the other subsections — the code block is a plain fenced block with no language tag, exactly like `### Internet`'s.)

- [ ] **Step 3: Add the Windows entry to `Credentials - Asynchronous`**

Same placement rule as Step 2 (after `### Postgres`, before `## Methods`):

````markdown
### Windows

```
from wcp_library.credentials.windows import AsyncWindowsCredentialManager

CredentialsManager = AsyncWindowsCredentialManager(<API_KEY>)
```
````

- [ ] **Step 4: Diff-check the three files**

Run: `git diff "docs/Wiki Docs/Credentials - Expected Dictionaries" "docs/Wiki Docs/Credentials ‐ Synchronous" "docs/Wiki Docs/Credentials - Asynchronous"`
Expected: each diff shows only an added Windows section/subsection, formatted consistently with its neighboring sections (no accidental changes to existing content).

- [ ] **Step 5: Commit**

```bash
git add "docs/Wiki Docs/Credentials - Expected Dictionaries" "docs/Wiki Docs/Credentials ‐ Synchronous" "docs/Wiki Docs/Credentials - Asynchronous"
git commit -m "docs: add Windows credential manager to wiki docs"
```