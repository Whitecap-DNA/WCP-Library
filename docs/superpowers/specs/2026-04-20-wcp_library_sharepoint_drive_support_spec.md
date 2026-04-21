# Spec: Drive-scoped support in `wcp_library.graph.sharepoint`

## Problem

All file-operation helpers hardcode `/sites/{site_id}/drive/root:...`, which resolves to the site's **default** document library. Sites with multiple libraries (e.g. `APFileCaptureRepository` has one drive per company) cannot be targeted at all.

Additionally, `list_folder`, `get_lists`, and `get_list_items` consume `response.json().get("value", [])` once — they silently drop every item past the first `@odata.nextLink` page (~200 items). For large folders this is a data-loss bug, not just a limitation.

Precedent for drive-scoped access already exists in the module: `get_file_content_by_id(headers, drive_id, item_id)` hits `/drives/{drive_id}/items/{item_id}/content`. The fix extends that pattern to the rest of the file API.

## Goals

1. Every file-operation helper can target either the site's default drive (current behavior) **or** an explicit non-default drive.
2. List-returning helpers paginate `@odata.nextLink` to completion.
3. All existing callers keep working unchanged — additions are keyword-only, no positional signature changes.

## Non-goals

- List/list-item functions (`get_lists`, `create_list_item`, etc.) — SharePoint *lists* are a different resource from *drives* and don't share this problem. Leave alone except for the pagination fix in `get_lists` / `get_list_items`.
- `get_site_metadata`, `get_file_content_by_id` — already correct.
- Throttling/retry, batching, delta queries — out of scope for this patch.

## API changes

### New helpers

```python
def get_drives(headers: dict, site_id: str) -> list[dict]:
    """List document libraries (drives) on a site. Paginated."""
    # GET /sites/{site_id}/drives — follow @odata.nextLink

def get_drive_id_by_name(headers: dict, site_id: str, drive_name: str) -> str | None:
    """Resolve a drive id by display name. Returns None if not found."""
    # Built on get_drives; case-sensitive match on the `name` field
```

### Modified file helpers

Add a keyword-only `drive_id: str | None = None` parameter to each of:

- `list_folder`
- `get_file_metadata`
- `get_file_content`
- `upload_file`
- `download_file`
- `move_file`
- `rename_file` (delegates, so pass-through)
- `copy_file`
- `remove_file`

Semantics: if `drive_id` is passed, URL base becomes `/drives/{drive_id}`; otherwise `/sites/{site_id}/drive` (unchanged). `site_id` remains positional and required — callers still pass it even when it's not the URL base, because it keeps the signature stable and callers typically have it on hand. (Alternative: make `site_id` optional when `drive_id` is given — I'd avoid this to keep the signature stable and callers predictable.)

Implementation pattern:

```python
def _drive_base(site_id: str, drive_id: str | None) -> str:
    if drive_id:
        return f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
    return f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive"

def list_folder(
    headers: dict,
    site_id: str,
    folder_path: str,
    *,
    drive_id: str | None = None,
) -> list | None:
    url = f"{_drive_base(site_id, drive_id)}/root:{folder_path}:/children"
    # ... then paginate @odata.nextLink ...
```

### `_build_payload` for move/copy

The payload currently constructs `"/drive/root:{destination_path}"`. When `drive_id` is given it must become `"/drives/{drive_id}/root:{destination_path}"`. Extend `_build_payload` to accept an optional `drive_id` and switch the prefix.

### Pagination fix

In `list_folder`, `get_lists`, and `get_list_items`, replace the one-shot `.get("value", [])` with a loop that follows `@odata.nextLink` until exhausted. Same pattern as `discover_sharepoint.py` already uses. Consider exposing an optional `page_size: int | None = None` that maps to `$top` so callers can tune (default: let Graph decide).

### Edge cases to specify

- Root of a drive: `list_folder(..., folder_path="/")` must hit `/drives/{drive_id}/root/children` (no colon), not `/root:/:/children` — the latter 400s. Detect `folder_path in ("", "/")` and branch.
- If both `site_id` and `drive_id` are provided, `drive_id` wins; don't raise.

## Tests

Minimum coverage (mock `requests`):

1. **Drive-scoped URL shape** — every modified function: assert the URL contains `/drives/{drive_id}/...` when `drive_id` is passed, and `/sites/{site_id}/drive/...` when it isn't.
2. **Pagination** — `list_folder` with two-page fixture returns both pages concatenated; stops when `@odata.nextLink` is absent.
3. **Root-of-drive** — `list_folder(..., "/")` with `drive_id` hits `/drives/{id}/root/children`, not `/root:/:/children`.
4. **`get_drive_id_by_name`** — returns correct id on match, `None` on miss, paginates if the site has >200 drives (unlikely but free to cover).
5. **Move/copy payload** — with `drive_id`, payload `parentReference.path` begins with `/drives/{drive_id}/root:`; without, it's `/drive/root:` (unchanged).
6. **Back-compat** — existing call sites without `drive_id` produce byte-identical URLs to the current implementation.

## Rollout

- Semver: additive keyword args + a pagination bugfix → **minor** bump (e.g. `1.9.5` → `1.10.0`). The pagination change could surprise callers who unknowingly relied on truncated results, but fixing silent data loss justifies it — call it out in the changelog.
- This project can then pin `wcp-library >= 1.10.0` in `requirements.txt` and use the new API directly in `main.py`.

## Example usage (post-patch)

```python
from wcp_library.graph import get_headers
from wcp_library.graph.sharepoint import (
    get_site_metadata, get_drive_id_by_name, list_folder, upload_file,
)

headers = get_headers(app_id, app_secret, tenant_id)
site = get_site_metadata(headers, "https://whitecap.sharepoint.com/sites/APFileCaptureRepository")
drive_id = get_drive_id_by_name(headers, site["id"], "01_Whitecap")

# List a year/month/day folder in a non-default drive, with pagination
files = list_folder(headers, site["id"], "/2025/01/31", drive_id=drive_id)

# Upload to the same drive
upload_file(headers, site["id"], "/2025/04/20", "_API12345_20260420T1200.pdf",
            file_bytes, drive_id=drive_id)
```