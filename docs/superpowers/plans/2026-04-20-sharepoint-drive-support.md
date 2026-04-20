# SharePoint Drive-Scoped Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `wcp_library.graph.sharepoint` so every file-operation helper can target a non-default drive, and fix silent truncation in paginated list endpoints.

**Architecture:** Introduce two private helpers — `_drive_base(site_id, drive_id)` to switch the URL prefix between `/sites/{id}/drive` and `/drives/{id}`, and `_iter_pages(url, headers, page_size)` to follow `@odata.nextLink` to completion. Thread a keyword-only `drive_id: str | None = None` through every file helper and `_build_payload`. Add `get_drives` / `get_drive_id_by_name`. Bump to `1.10.0` (additive keyword args + pagination bugfix).

**Tech Stack:** Python 3.12, `requests`, Microsoft Graph v1.0 REST, Poetry.

**Project note:** The user has stated this repo has no test suite, so this plan has no test steps. Each task instead ends with a quick self-check you can perform by reading the diff against the "Expected URL shape" line.

**Backward compatibility contract:** Every new parameter defaults to the value that preserves today's behavior.

| New parameter | Default | Behavior when defaulted |
|---|---|---|
| `drive_id` (all file helpers) | `None` | URL routes through `/sites/{site_id}/drive` — byte-identical to pre-patch |
| `drive_id` (`_build_payload`) | `None` | `parentReference.path` stays `/drive/root:{destination_path}` — byte-identical |
| `page_size` (`list_folder`, `get_drives`, `get_lists`, `get_list_items`) | `None` | No `$top` appended — Graph uses its server-side default, matching pre-patch requests |

The only intentional behavior change is the pagination fix itself: `list_folder` / `get_lists` / `get_list_items` will now return items beyond the first ~200. The spec calls this out as a bugfix worthy of a minor version bump — callers who unknowingly relied on truncated results may see larger result sets. No signature changes to existing positional parameters.

**Logging normalization:** Per the user, everything under `wcp_library/graph/` should emit errors through `logger`, not `print`. A scan of the folder (`grep -n "print(" wcp_library/graph/`) identified three offenders, all in `sharepoint.py`:

- `list_folder` (line 96) — normalized in Task 3
- `get_file_content` (line 134) — normalized in Task 5
- `get_file_content_by_id` (line 153) — normalized in Task 13b

`mail.py` and `subscription.py` already use `logger` throughout — no changes needed there. The return contract (`None` / `[]` on error) is unchanged everywhere; only the error-output destination moves from stdout to the module logger. Callers relying on stdout capture of errors need to configure the `wcp_library.graph.sharepoint` logger instead.

---

## File Map

- **Modify:** `wcp_library/graph/sharepoint.py` — all changes land here.
- **Modify:** `pyproject.toml` — bump `version = "1.9.5"` → `"1.10.0"`.

No new files. Every helper lives in the existing module.

---

## Task 1: Add `_drive_base` URL helper

**Files:**
- Modify: `wcp_library/graph/sharepoint.py` — insert just above `get_site_metadata` (after the `logger = logging.getLogger(__name__)` line, before the `# ----- Site Functions -----` banner).

- [ ] **Step 1: Insert the helper**

Add this block after the `logger = ...` line (around line 54):

```python
_GRAPH_ROOT = "https://graph.microsoft.com/v1.0"


def _drive_base(site_id: str, drive_id: str | None) -> str:
    """Return the Graph URL prefix for file operations.

    If ``drive_id`` is provided it wins over ``site_id`` (targets a specific
    document library). Otherwise the site's default drive is used.
    """
    if drive_id:
        return f"{_GRAPH_ROOT}/drives/{drive_id}"
    return f"{_GRAPH_ROOT}/sites/{site_id}/drive"
```

Then replace the hardcoded `"https://graph.microsoft.com/v1.0/sites/..."` string in `get_site_metadata` with `f"{_GRAPH_ROOT}/sites/{url.host}:{url.path}"` so the constant is used consistently (do not otherwise touch `get_site_metadata`).

- [ ] **Step 2: Self-check**

Confirm via `grep -n "_drive_base\|_GRAPH_ROOT" wcp_library/graph/sharepoint.py` that both symbols exist exactly once as definitions. No callers yet.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): add _drive_base URL helper"
```

---

## Task 2: Add `_iter_pages` pagination helper

**Files:**
- Modify: `wcp_library/graph/sharepoint.py` — insert immediately after `_drive_base`.

- [ ] **Step 1: Insert the helper**

```python
def _iter_pages(
    url: str,
    headers: dict,
    page_size: int | None = None,
) -> list[dict]:
    """GET ``url`` and follow ``@odata.nextLink`` until exhausted.

    Returns the concatenated ``value`` arrays from every page. Raises
    ``requests.RequestException`` on HTTP failure — callers wrap this in
    their own try/except to preserve their existing error-reporting style
    (some use ``print``, some use ``logger``).

    :param page_size: If given, appended as ``$top`` on the first request.
        Graph echoes this on subsequent ``@odata.nextLink`` URLs, so we only
        need to set it once.
    """
    if page_size is not None:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}$top={page_size}"

    items: list[dict] = []
    next_url: str | None = url
    while next_url:
        response = requests.get(next_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        items.extend(data.get("value", []))
        next_url = data.get("@odata.nextLink")
    return items
```

- [ ] **Step 2: Self-check**

The helper must (a) accept an optional `page_size`, (b) loop on `@odata.nextLink`, (c) **not** catch `RequestException` — each caller owns its try/except and logs via the module logger. Read the function once and confirm.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): add _iter_pages helper for @odata.nextLink"
```

---

## Task 3: Refactor `list_folder` — drive_id, pagination, root-of-drive

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:82-97` (the current `list_folder` function).

- [ ] **Step 1: Replace the function body**

Spec constraints implemented here:
- `drive_id` keyword-only, default `None`.
- Root-of-drive: when `folder_path in ("", "/")` and `drive_id` is given, hit `/drives/{drive_id}/root/children` (no colon-delimited path). Same branch for default drive: `/sites/{id}/drive/root/children`. The colon syntax only works for non-root paths.
- Pagination via `_iter_pages`.
- Optional `page_size`.

Replace the entire function with:

```python
def list_folder(
    headers: dict,
    site_id: str,
    folder_path: str,
    *,
    drive_id: str | None = None,
    page_size: int | None = None,
) -> list | None:
    """Lists files in a SharePoint folder using the Microsoft Graph API.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param folder_path: The folder path (e.g. "/Shared Documents/My Folder").
        Use ``"/"`` or ``""`` to list the root of the drive.
    :param drive_id: Optional drive (document library) ID. If omitted, the
        site's default drive is used.
    :param page_size: Optional ``$top`` value passed on the initial request
        to tune Graph's page size. Default: let Graph decide.
    :return: A list of file/folder metadata objects across all pages, or
        ``None`` on error.
    """
    base = _drive_base(site_id, drive_id)
    if folder_path in ("", "/"):
        url = f"{base}/root/children"
    else:
        url = f"{base}/root:{folder_path}:/children"
    try:
        return _iter_pages(url, headers, page_size=page_size)
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None
```

Error branch is switched from the current `print(...)` to logger-based output, matching the rest of the module. Return value (`None`) is unchanged.

- [ ] **Step 2: Self-check URL shapes**

Mentally trace four calls and confirm the URL each produces:

| Call | Expected URL |
|---|---|
| `list_folder(h, "S", "/Reports")` | `https://graph.microsoft.com/v1.0/sites/S/drive/root:/Reports:/children` |
| `list_folder(h, "S", "/Reports", drive_id="D")` | `https://graph.microsoft.com/v1.0/drives/D/root:/Reports:/children` |
| `list_folder(h, "S", "/", drive_id="D")` | `https://graph.microsoft.com/v1.0/drives/D/root/children` |
| `list_folder(h, "S", "/Reports", page_size=500)` | `.../children?$top=500` |

The first row must be byte-identical to the pre-patch URL (back-compat).

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): drive_id + pagination for list_folder"
```

---

## Task 4: Thread `drive_id` through `get_file_metadata`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:100-117`.

- [ ] **Step 1: Replace URL construction**

Change the signature and URL line; leave the error handling alone.

New signature:

```python
def get_file_metadata(
    headers: dict,
    site_id: str,
    file_path: str,
    *,
    drive_id: str | None = None,
) -> dict | None:
```

New URL line (replacing the current `url = ...`):

```python
url = f"{_drive_base(site_id, drive_id)}/root:{file_path}"
```

Update the docstring with a `:param drive_id:` line matching the one in Task 3. Do not modify the except branch.

- [ ] **Step 2: Self-check**

Default-drive URL must still be `https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path}` (byte-identical).

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): drive_id on get_file_metadata"
```

---

## Task 5: Thread `drive_id` through `get_file_content`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:120-135`.

- [ ] **Step 1: Replace URL construction**

New signature:

```python
def get_file_content(
    headers: dict,
    site_id: str,
    file_path: str,
    *,
    drive_id: str | None = None,
) -> bytes | None:
```

New URL line:

```python
url = f"{_drive_base(site_id, drive_id)}/root:{file_path}:/content"
```

Update docstring with `:param drive_id:`. Replace the `print(...)` in the except branch with logger-based output to match the module convention:

```python
except requests.RequestException as e:
    logger.error("Error: %s", e)
    if hasattr(e, "response") and e.response is not None:
        logger.debug("Response text: %s", e.response.text)
    return None
```

- [ ] **Step 2: Self-check**

Default-drive URL must be `https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path}:/content`.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): drive_id on get_file_content"
```

---

## Task 6: Thread `drive_id` through `upload_file`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:157-200`.

- [ ] **Step 1: Replace URL construction**

New signature (keep `conflict_behavior` where it is, add `drive_id` after it — both keyword-only is fine since Python will accept that):

```python
def upload_file(
    headers: dict,
    site_id: str,
    file_path: str,
    filename: str,
    content: bytes | bytearray | memoryview | str,
    conflict_behavior: str = "rename",
    *,
    drive_id: str | None = None,
) -> dict | None:
```

New URL assignment (replacing the current three-line `url = (...)` expression):

```python
url = (
    f"{_drive_base(site_id, drive_id)}/root:"
    f"{file_path}/{filename}:/content"
    f"?@microsoft.graph.conflictBehavior={conflict_behavior}"
)
```

Update docstring with `:param drive_id:`.

- [ ] **Step 2: Self-check**

Default-drive URL: `https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path}/{filename}:/content?@microsoft.graph.conflictBehavior={conflict_behavior}` — byte-identical to the pre-patch URL.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): drive_id on upload_file"
```

---

## Task 7: Thread `drive_id` through `download_file`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:213-237`.

- [ ] **Step 1: Replace URL construction**

New signature:

```python
def download_file(
    headers: dict,
    site_id: str,
    file_path: str,
    download_folder: Path,
    *,
    drive_id: str | None = None,
) -> Path | None:
```

New URL line:

```python
url = f"{_drive_base(site_id, drive_id)}/root:{file_path}:/content"
```

Update docstring with `:param drive_id:`. (Error branch here already uses `logger.error`; no print to clean up.)

- [ ] **Step 2: Self-check**

Default-drive URL: `https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path}:/content`.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): drive_id on download_file"
```

---

## Task 8: Update `_build_payload` to accept `drive_id`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:348-354`.

The move/copy payload currently embeds `/drive/root:{destination_path}`. When targeting a non-default drive it needs to be `/drives/{drive_id}/root:{destination_path}`. Note the prefix is **without** a leading Graph host (Graph consumes the `parentReference.path` as a drive-relative reference, not an absolute URL).

- [ ] **Step 1: Replace the function**

```python
def _build_payload(
    destination_path: str | None,
    new_filename: str | None = None,
    drive_id: str | None = None,
) -> dict:
    if drive_id:
        parent_path = f"/drives/{drive_id}/root:{destination_path}"
    else:
        parent_path = f"/drive/root:{destination_path}"
    payload = {"parentReference": {"path": parent_path}}
    if new_filename:
        payload["name"] = new_filename
    return payload
```

- [ ] **Step 2: Self-check**

| Call | Expected `payload["parentReference"]["path"]` |
|---|---|
| `_build_payload("/Archive")` | `/drive/root:/Archive` |
| `_build_payload("/Archive", drive_id="D")` | `/drives/D/root:/Archive` |
| `_build_payload("/Archive", "new.pdf")` | `/drive/root:/Archive` and `payload["name"] == "new.pdf"` |

First row must be byte-identical to the pre-patch output.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): drive_id on _build_payload"
```

---

## Task 9: Thread `drive_id` through `move_file`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:240-281`.

- [ ] **Step 1: Replace signature, URL, and payload call**

New signature:

```python
def move_file(
    headers: dict,
    site_id: str,
    source_path: str,
    destination_path: str,
    new_filename: str | None = None,
    *,
    drive_id: str | None = None,
) -> dict | None:
```

Replace the URL line:

```python
url = f"{_drive_base(site_id, drive_id)}/root:{source_path}"
```

Replace the payload call:

```python
payload = _build_payload(destination_path, new_filename, drive_id=drive_id)
```

Update docstring with `:param drive_id:`.

- [ ] **Step 2: Self-check**

Default-drive URL: `https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{source_path}`. Payload `parentReference.path` unchanged when `drive_id is None`.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): drive_id on move_file"
```

---

## Task 10: Thread `drive_id` through `rename_file`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:284-307`.

`rename_file` is a pass-through to `move_file`; the change is purely signature-plus-forwarding.

- [ ] **Step 1: Update signature and call**

```python
def rename_file(
    headers: dict,
    site_id: str,
    file_path: str,
    new_filename: str,
    *,
    drive_id: str | None = None,
) -> dict | None:
    ...
    return move_file(
        headers,
        site_id,
        file_path,
        destination_path=file_path,
        new_filename=new_filename,
        drive_id=drive_id,
    )
```

Update docstring with `:param drive_id:`.

- [ ] **Step 2: Self-check**

Reading the diff: `drive_id` flows into `move_file` unchanged. No other behavior change.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): drive_id on rename_file"
```

---

## Task 11: Thread `drive_id` through `copy_file`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:310-345`.

- [ ] **Step 1: Replace signature, URL, and payload call**

New signature:

```python
def copy_file(
    headers: dict,
    site_id: str,
    source_path: str,
    destination_path: str,
    new_filename: str | None = None,
    *,
    drive_id: str | None = None,
) -> dict | None:
```

Replace URL:

```python
url = f"{_drive_base(site_id, drive_id)}/root:{source_path}:/copy"
```

Replace payload call:

```python
payload = _build_payload(destination_path, new_filename, drive_id=drive_id)
```

Update docstring with `:param drive_id:`.

- [ ] **Step 2: Self-check**

Default-drive URL: `https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{source_path}:/copy`.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): drive_id on copy_file"
```

---

## Task 12: Thread `drive_id` through `remove_file`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:357-377`.

- [ ] **Step 1: Replace signature and URL**

```python
def remove_file(
    headers: dict,
    site_id: str,
    file_path: str,
    *,
    drive_id: str | None = None,
) -> bool:
    ...
    url = f"{_drive_base(site_id, drive_id)}/root:{file_path}"
```

Update docstring with `:param drive_id:`.

- [ ] **Step 2: Self-check**

Default-drive URL: `https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{file_path}`.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): drive_id on remove_file"
```

---

## Task 13: Add `get_drives` and `get_drive_id_by_name`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py` — insert immediately after `get_site_metadata` and before the `# ----- File Functions -----` banner. Drives are site-scoped metadata; this keeps them with the site helper.

- [ ] **Step 1: Add both helpers**

```python
def get_drives(
    headers: dict,
    site_id: str,
    *,
    page_size: int | None = None,
) -> list[dict] | None:
    """List document libraries (drives) on a SharePoint site.

    API Reference: https://learn.microsoft.com/en-us/graph/api/drive-list

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param page_size: Optional ``$top`` override.
    :return: A list of drive metadata objects across all pages, or ``None``
        on error.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/drives"
    try:
        return _iter_pages(url, headers, page_size=page_size)
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None


def get_drive_id_by_name(
    headers: dict,
    site_id: str,
    drive_name: str,
) -> str | None:
    """Resolve a drive ID by display name. Case-sensitive exact match on
    ``name``. Returns ``None`` if the drive is not found or the request fails.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param drive_name: The display name of the drive.
    :return: The drive ID, or ``None`` if no match.
    """
    drives = get_drives(headers, site_id)
    if drives is None:
        return None
    for drive in drives:
        if drive.get("name") == drive_name:
            return drive.get("id")
    return None
```

- [ ] **Step 2: Self-check**

Confirm URL shape: `https://graph.microsoft.com/v1.0/sites/{site_id}/drives`. Confirm `get_drive_id_by_name` is case-sensitive (no `.lower()` normalization — the spec explicitly says case-sensitive).

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "feat(sharepoint): add get_drives and get_drive_id_by_name"
```

---

## Task 13b: Normalize `get_file_content_by_id` logging

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:138-154` (the existing `get_file_content_by_id`).

`get_file_content_by_id` is functionally out of scope (already drive-scoped, already correct), but its except branch uses `print(...)` — the last remaining offender in the module. Per the user's directive to convert all print statements to logger, update it.

- [ ] **Step 1: Replace the except branch**

Keep the signature and URL line exactly as-is. Replace only the except branch:

```python
except requests.RequestException as e:
    logger.error("Error: %s", e)
    if hasattr(e, "response") and e.response is not None:
        logger.debug("Response text: %s", e.response.text)
    return None
```

- [ ] **Step 2: Self-check**

```
grep -n "print(" wcp_library/graph/sharepoint.py
```

Expected: no matches. (`list_folder`, `get_file_content`, `get_file_content_by_id` were the only three; all converted by Tasks 3, 5, 13b.)

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "refactor(sharepoint): convert remaining print calls to logger"
```

---

## Task 14: Paginate `get_lists`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:383-401`.

Return contract stays `list[dict]` (empty on failure) — this is a silent-data-loss fix, not a signature change. `_iter_pages` returns `None` on error; coerce to `[]` here to preserve the current error contract.

- [ ] **Step 1: Replace the function body**

```python
def get_lists(
    headers: dict,
    site_id: str,
    *,
    page_size: int | None = None,
) -> list[dict]:
    """Retrieves the lists from a SharePoint site using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/list-list

    Follows ``@odata.nextLink`` to completion. Returns ``[]`` on error
    (unchanged contract).

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param page_size: Optional ``$top`` override.
    :return: A list of SharePoint lists as JSON objects across all pages.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists"
    try:
        return _iter_pages(url, headers, page_size=page_size)
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return []
```

- [ ] **Step 2: Self-check**

Default URL: `https://graph.microsoft.com/v1.0/sites/{site_id}/lists` (byte-identical). Error path returns `[]` (unchanged).

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "fix(sharepoint): paginate get_lists across @odata.nextLink"
```

---

## Task 15: Paginate `get_list_items`

**Files:**
- Modify: `wcp_library/graph/sharepoint.py:477-501`.

The current function returns `[]` on success and `None` on exception (inconsistent, but matches what callers see today). Preserve that: `_iter_pages` returns `None` on error, `list` on success — which happens to match the existing observed behavior exactly, so no coercion needed.

- [ ] **Step 1: Replace the function body**

```python
def get_list_items(
    headers: dict,
    site_id: str,
    list_id: str,
    odata_filter: str | None = None,
    *,
    page_size: int | None = None,
) -> list[dict] | None:
    """Retrieves the items from a SharePoint list using the Microsoft Graph API.
    API Reference: https://learn.microsoft.com/en-us/graph/api/listitem-list

    Follows ``@odata.nextLink`` to completion.

    :param headers: The headers containing the Authorization token.
    :param site_id: The ID of the SharePoint site.
    :param list_id: The ID of the SharePoint list.
    :param odata_filter: An optional OData filter string to filter the list items.
    :param page_size: Optional ``$top`` override.
    :return: A list of SharePoint list items as JSON objects across all pages,
        or ``None`` on error.
    """
    url = f"{_GRAPH_ROOT}/sites/{site_id}/lists/{list_id}/items"
    if odata_filter:
        url += f"?$filter={odata_filter}"
    try:
        return _iter_pages(url, headers, page_size=page_size)
    except requests.RequestException as e:
        logger.error("Error: %s", e)
        if hasattr(e, "response") and e.response is not None:
            logger.debug("Response text: %s", e.response.text)
        return None
```

Also update the return type annotation from `list[dict]` to `list[dict] | None` (the old annotation was a lie — it already returned `None` on error).

- [ ] **Step 2: Self-check**

With `odata_filter`: URL is `.../items?$filter={f}`, and `_iter_pages` appends `&$top={n}` when `page_size` is set (note the `"?" in url` branch in `_iter_pages` — that's exactly this case).

Without `odata_filter`: URL is `.../items`, and `_iter_pages` appends `?$top={n}` when `page_size` is set.

- [ ] **Step 3: Commit**

```bash
git add wcp_library/graph/sharepoint.py
git commit -m "fix(sharepoint): paginate get_list_items across @odata.nextLink"
```

---

## Task 16: Version bump to 1.10.0

**Files:**
- Modify: `pyproject.toml:3` — change `version = "1.9.5"` to `version = "1.10.0"`.

Semver rationale: additive keyword args (drive_id, page_size) + pagination bugfix that could surprise callers who unknowingly relied on truncated results → minor bump.

- [ ] **Step 1: Edit `pyproject.toml`**

```toml
version = "1.10.0"
```

- [ ] **Step 2: Self-check**

```
grep -n '^version' pyproject.toml
```

Expected: `3:version = "1.10.0"`.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "chore: bump version to 1.10.0 (drive-scoped sharepoint + pagination fix)"
```

---

## Self-Review

Traced against the spec:

| Spec requirement | Covered by |
|---|---|
| `drive_id` keyword-only on `list_folder` | Task 3 |
| `drive_id` on `get_file_metadata` | Task 4 |
| `drive_id` on `get_file_content` | Task 5 |
| `drive_id` on `upload_file` | Task 6 |
| `drive_id` on `download_file` | Task 7 |
| `drive_id` on `move_file` | Task 9 |
| `drive_id` pass-through on `rename_file` | Task 10 |
| `drive_id` on `copy_file` | Task 11 |
| `drive_id` on `remove_file` | Task 12 |
| `_build_payload` drive-aware `parentReference.path` | Task 8 |
| `get_drives` + `get_drive_id_by_name` | Task 13 |
| Paginate `list_folder` | Task 3 |
| Paginate `get_lists` | Task 14 |
| Paginate `get_list_items` | Task 15 |
| Root-of-drive branch (`"/"` → `/root/children`) | Task 3 |
| `drive_id` wins over `site_id` when both given | Task 1 (`_drive_base`) |
| Byte-identical URLs when `drive_id is None` | Self-check rows in Tasks 3–12 |
| Optional `page_size` → `$top` | Tasks 2, 3, 13, 14, 15 |
| Version bump for minor release | Task 16 |
| Convert `print(...)` to logger (user directive) | Tasks 3, 5, 13b |

Non-goals (per spec) left alone: `get_site_metadata` (only touched to use `_GRAPH_ROOT` constant — no behavior change), `get_file_content_by_id` body (only the except branch is normalized to logger per user directive), all list-item CRUD other than `get_list_items`, throttling/retry/batch/delta.