# Tenacity Retry Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace three divergent retry implementations with a single tenacity-backed strategy layer in a new `wcp_library/retry.py` module, and add HTTP retry coverage to every helper in `wcp_library/graph/`.

**Architecture:** Single `wcp_library/retry.py` exports strategy-kwargs dicts (`postgres_retry_kwargs`, `oracle_retry_kwargs`, `graph_retry_kwargs`) plus a `make_generic_retry(exceptions, ...)` factory. SQL connection primitives decorate themselves with `@tenacity.retry(**postgres_retry_kwargs)` etc.; `graph` modules call a new module-private `_request` helper in `wcp_library/graph/__init__.py` that handles HTTP retry. `retry_transaction` becomes a thin wrapper around `tenacity.Retrying` / `AsyncRetrying`.

**Tech Stack:** Python 3.12, tenacity `~=9.1.4`, psycopg3, oracledb, requests, pandas (for existing composites).

**Spec:** `docs/superpowers/specs/2026-04-22-tenacity-retry-consolidation-design.md`

---

## File Structure

**New:**
- `wcp_library/retry.py` — single module, all policy. ~200 lines.
- `tests/test_retry.py` — unit tests for the new module.

**Modify:**
- `pyproject.toml` — add `tenacity = "~9.1.4"` dep; bump version to `1.12.0` (last task).
- `wcp_library/__init__.py` — remove `retry`, `async_retry`, constants (last task).
- `wcp_library/sql/__init__.py` — remove `retry`, `async_retry`, `classify_db_exception`, `_log_retry`, `_log_giveup`, `_RETRIABLE_DB_ERRORS`, `_RETRY_SLEEP_SECONDS` (last task).
- `wcp_library/sql/postgres.py` — decorator swap on 5 sync primitives + 5 async primitives + `remove_matching_data` (both); rewire `retry_transaction` (both).
- `wcp_library/sql/oracle.py` — decorator swap, same pattern.
- `wcp_library/graph/__init__.py` — add `_request` helper.
- `wcp_library/graph/sharepoint.py` — swap ~25 inline `requests.*` calls + pagination helper.
- `wcp_library/graph/mail.py` — swap inline calls.
- `wcp_library/graph/subscription.py` — swap inline calls.
- `wcp_library/browser_automation/browser.py` — single decorator swap.
- Multiple wiki pages under `docs/Wiki Docs/`.

**Test updates:**
- `tests/sql/test_postgres_retry_transaction.py`, `test_postgres_retry_transaction_sync.py` — update sleep patches.
- `tests/sql/test_postgres_autocommit.py` — construct-guard test references `self.retry_error_codes`; remove.
- `tests/graph/*` — add a handful of retry-behavior tests.

---

### Task 1: Add tenacity dependency + create module scaffold

**Files:**
- Modify: `pyproject.toml`
- Create: `wcp_library/retry.py`
- Create: `tests/test_retry.py`

- [ ] **Step 1: Add tenacity to pyproject.toml**

Open `pyproject.toml` and find the `[tool.poetry.dependencies]` section. Add `tenacity = "~9.1.4"` in alphabetical order (between `selenium` and `webdriver-manager`):

```toml
[tool.poetry.dependencies]
aiofiles = "^25.1.0"
# ... existing entries ...
selenium = "^4.39.0"
tenacity = "~9.1.4"
webdriver-manager = "^4.0.2"
yarl = "^1.20.0"
```

- [ ] **Step 2: Refresh lockfile**

Run: `poetry lock`
Expected: `Writing lock file` with no errors.

- [ ] **Step 3: Create the retry module scaffold**

Create `wcp_library/retry.py`:

```python
"""Retry policies and tenacity strategy configs for wcp_library.

Every retry policy in the library is defined here. Consumers apply
them via tenacity's decorator/`Retrying`/`AsyncRetrying` surface:

    from tenacity import retry as tenacity_retry
    from wcp_library.retry import postgres_retry_kwargs

    @tenacity_retry(**postgres_retry_kwargs)
    def execute(self, query): ...

Four public policies:
* ``postgres_retry_kwargs`` — tiered SQL retry for psycopg errors.
* ``oracle_retry_kwargs`` — tiered SQL retry for oracledb errors.
* ``graph_retry_kwargs`` — HTTP retry for Microsoft Graph calls.
* ``make_generic_retry(exceptions, ...)`` — factory for arbitrary
  exception-list retry with exp backoff + jitter.
"""
import logging
import random

logger = logging.getLogger(__name__)

# Module-private sentinel wrapper for Graph retry; kept here so the
# strategy can reference it. graph/__init__.py imports this symbol
# when building retryable exceptions.
class _GraphRetriable(Exception):
    """Raised inside wcp_library.graph._request to signal tenacity to retry.

    Module-private -- graph callers rely on `requests.exceptions.*` and the
    None-on-error contract of public helpers, not this sentinel.
    """
    def __init__(self, response=None, underlying=None):
        self.response = response
        self.underlying = underlying


# Public error-code constants (frozen sets for set-membership checks).
_POSTGRES_CONNECTION_LOSS = frozenset({"08001", "08004"})
_POSTGRES_TRANSIENT       = frozenset({"40P01"})
POSTGRES_RETRY_CODES      = _POSTGRES_CONNECTION_LOSS | _POSTGRES_TRANSIENT

_ORACLE_CONNECTION_LOSS   = frozenset({"ORA-01033", "DPY-6005", "DPY-4011"})
_ORACLE_TRANSIENT         = frozenset({"ORA-08103", "ORA-04021", "ORA-01652"})
ORACLE_RETRY_CODES        = _ORACLE_CONNECTION_LOSS | _ORACLE_TRANSIENT

GRAPH_RETRIABLE_STATUSES  = frozenset({429, 503, 504})
```

(Subsequent tasks fill in the classifier, strategies, and factory. Scaffold gives us a target to import from.)

- [ ] **Step 4: Create empty test module**

Create `tests/test_retry.py`:

```python
"""Tests for wcp_library.retry (tenacity strategy configs)."""
```

- [ ] **Step 5: Verify file parses + run suite**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: existing 405 tests still pass. No new tests yet.

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml poetry.lock wcp_library/retry.py tests/test_retry.py
git commit -m "$(cat <<'EOF'
chore(retry): add tenacity dep and wcp_library/retry.py scaffold

Introduces the tenacity dependency (pinned ~9.1.4 to match other D&A
projects) and creates the empty wcp_library/retry.py module with
constants (POSTGRES_RETRY_CODES, ORACLE_RETRY_CODES,
GRAPH_RETRIABLE_STATUSES) and the module-private _GraphRetriable
sentinel. Subsequent commits fill in the classifier, strategies, and
factory.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Classifier helper + shared logging hook

**Files:**
- Modify: `wcp_library/retry.py`
- Modify: `tests/test_retry.py`

- [ ] **Step 1: Write failing test for `_extract_full_code`**

Append to `tests/test_retry.py`:

```python
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
```

- [ ] **Step 2: Run to verify failure**

Run: `venv/Scripts/pytest tests/test_retry.py -v`
Expected: `ImportError: cannot import name '_extract_full_code'` or similar.

- [ ] **Step 3: Implement `_extract_full_code` and `_before_sleep_log`**

Append to `wcp_library/retry.py` (after the constants block):

```python
def _extract_full_code(exc: BaseException) -> str | None:
    """Pull ``full_code`` off the driver's error object if present.

    psycopg and oracledb both put a structured error object as
    ``exc.args[0]`` with a ``full_code`` attribute. Returns None when
    the exception doesn't fit that shape.
    """
    try:
        (error_obj,) = exc.args
    except (ValueError, TypeError):
        return None
    if isinstance(error_obj, str):
        return None
    return getattr(error_obj, "full_code", None)


def _before_sleep_log(retry_state) -> None:
    """Shared tenacity before_sleep hook; logs retry attempts at INFO."""
    exc = retry_state.outcome.exception()
    logger.info(
        "retry %d: %s — %s",
        retry_state.attempt_number,
        type(exc).__name__,
        exc,
    )
```

- [ ] **Step 4: Run tests**

Run: `venv/Scripts/pytest tests/test_retry.py -v`
Expected: 5/5 tests pass.

- [ ] **Step 5: Commit**

```bash
git add wcp_library/retry.py tests/test_retry.py
git commit -m "feat(retry): add _extract_full_code classifier + shared before_sleep hook"
```

---

### Task 3: SQL tiered retry factory + `postgres_retry_kwargs` / `oracle_retry_kwargs`

**Files:**
- Modify: `wcp_library/retry.py`
- Modify: `tests/test_retry.py`

- [ ] **Step 1: Write failing tests for SQL strategy shape + tiering**

Append to `tests/test_retry.py`:

```python
import psycopg
import oracledb
from tenacity import RetryError

from wcp_library.retry import (
    postgres_retry_kwargs,
    oracle_retry_kwargs,
    POSTGRES_RETRY_CODES,
    ORACLE_RETRY_CODES,
)


def _mk_error(driver_exc_cls, full_code: str):
    obj = MagicMock()
    obj.full_code = full_code
    obj.message = "simulated"
    return driver_exc_cls(obj)


class TestPostgresRetryStrategy:
    def test_kwargs_are_complete(self):
        for key in ("retry", "wait", "stop", "before_sleep", "reraise"):
            assert key in postgres_retry_kwargs, key
        assert postgres_retry_kwargs["reraise"] is True

    def test_connection_loss_waits_300_seconds(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(psycopg.OperationalError, "08001")
        retry_state.attempt_number = 1
        wait = postgres_retry_kwargs["wait"](retry_state)
        assert wait == 300.0

    def test_transient_conflict_waits_sub_minute(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(psycopg.OperationalError, "40P01")
        retry_state.attempt_number = 1
        wait = postgres_retry_kwargs["wait"](retry_state)
        # Exp backoff at attempt 1 = 2^0 = 1 + up to 3s jitter
        assert 1.0 <= wait <= 4.0

    def test_retry_predicate_matches_retriable_codes(self):
        for code in POSTGRES_RETRY_CODES:
            retry_state = MagicMock()
            retry_state.outcome.exception.return_value = _mk_error(psycopg.OperationalError, code)
            assert postgres_retry_kwargs["retry"](retry_state), code

    def test_retry_predicate_rejects_non_retriable_code(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(psycopg.OperationalError, "99999")
        assert not postgres_retry_kwargs["retry"](retry_state)

    def test_retry_predicate_rejects_wrong_exception_type(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = ValueError("not a db error")
        assert not postgres_retry_kwargs["retry"](retry_state)


class TestOracleRetryStrategy:
    def test_connection_loss_waits_300_seconds(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(oracledb.OperationalError, "ORA-01033")
        retry_state.attempt_number = 1
        wait = oracle_retry_kwargs["wait"](retry_state)
        assert wait == 300.0

    def test_transient_exp_backoff(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _mk_error(oracledb.OperationalError, "ORA-08103")
        retry_state.attempt_number = 1
        wait = oracle_retry_kwargs["wait"](retry_state)
        assert 1.0 <= wait <= 4.0

    def test_all_retry_codes_match(self):
        for code in ORACLE_RETRY_CODES:
            retry_state = MagicMock()
            retry_state.outcome.exception.return_value = _mk_error(oracledb.OperationalError, code)
            assert oracle_retry_kwargs["retry"](retry_state), code
```

- [ ] **Step 2: Run to verify failure**

Run: `venv/Scripts/pytest tests/test_retry.py::TestPostgresRetryStrategy -v`
Expected: `ImportError: cannot import name 'postgres_retry_kwargs'`.

- [ ] **Step 3: Implement factory + strategies**

Append to `wcp_library/retry.py`:

```python
import oracledb
import psycopg
from tenacity import retry_if_exception, stop_after_attempt


def _make_sql_retry(
    catchable: tuple[type, ...],
    connection_loss_codes: frozenset[str],
    transient_codes: frozenset[str],
    name: str,
) -> dict:
    """Build tenacity kwargs for tiered SQL retry.

    * Connection-loss codes: fixed 300s wait (tolerate DB maintenance).
    * Transient codes: exp backoff + jitter (deadlocks / lock-busy
      resolve in milliseconds to seconds).
    """
    retriable_codes = connection_loss_codes | transient_codes

    def _should_retry(retry_state) -> bool:
        exc = retry_state.outcome.exception()
        if not isinstance(exc, catchable):
            return False
        return _extract_full_code(exc) in retriable_codes

    def _wait(retry_state) -> float:
        code = _extract_full_code(retry_state.outcome.exception())
        if code in connection_loss_codes:
            return 300.0
        return min(2 ** (retry_state.attempt_number - 1), 30) + random.uniform(0, 3)

    def _before_sleep(retry_state) -> None:
        code = _extract_full_code(retry_state.outcome.exception())
        logger.info(
            "%s retry %d: code=%s",
            name, retry_state.attempt_number, code,
        )

    return dict(
        retry=retry_if_exception(_should_retry),
        wait=_wait,
        stop=stop_after_attempt(50),
        before_sleep=_before_sleep,
        reraise=True,
    )


postgres_retry_kwargs = _make_sql_retry(
    catchable=(psycopg.OperationalError, psycopg.DatabaseError),
    connection_loss_codes=_POSTGRES_CONNECTION_LOSS,
    transient_codes=_POSTGRES_TRANSIENT,
    name="postgres",
)

oracle_retry_kwargs = _make_sql_retry(
    catchable=(oracledb.OperationalError, oracledb.DatabaseError),
    connection_loss_codes=_ORACLE_CONNECTION_LOSS,
    transient_codes=_ORACLE_TRANSIENT,
    name="oracle",
)
```

- [ ] **Step 4: Run tests**

Run: `venv/Scripts/pytest tests/test_retry.py -v`
Expected: all tests pass (5 from Task 2 + 8 new).

- [ ] **Step 5: Commit**

```bash
git add wcp_library/retry.py tests/test_retry.py
git commit -m "feat(retry): add tiered postgres_retry_kwargs + oracle_retry_kwargs"
```

---

### Task 4: Graph retry strategy (`graph_retry_kwargs`)

**Files:**
- Modify: `wcp_library/retry.py`
- Modify: `tests/test_retry.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_retry.py`:

```python
import requests

from wcp_library.retry import graph_retry_kwargs, _GraphRetriable


def _mk_graph_retry_response(status_code: int, retry_after: str | None = None):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.headers = {"Retry-After": retry_after} if retry_after else {}
    return resp


class TestGraphRetryStrategy:
    def test_kwargs_are_complete(self):
        for key in ("retry", "wait", "stop", "before_sleep", "reraise"):
            assert key in graph_retry_kwargs, key

    def test_retry_after_header_honored(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _GraphRetriable(
            response=_mk_graph_retry_response(429, retry_after="42")
        )
        retry_state.attempt_number = 1
        wait = graph_retry_kwargs["wait"](retry_state)
        assert wait == 42.0

    def test_exp_backoff_when_no_retry_after(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _GraphRetriable(
            response=_mk_graph_retry_response(503)
        )
        retry_state.attempt_number = 1
        wait = graph_retry_kwargs["wait"](retry_state)
        assert 1.0 <= wait <= 4.0

    def test_exp_backoff_on_network_error(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _GraphRetriable(
            underlying=requests.ConnectionError("boom")
        )
        retry_state.attempt_number = 2
        wait = graph_retry_kwargs["wait"](retry_state)
        assert 2.0 <= wait <= 5.0

    def test_non_numeric_retry_after_falls_back_to_backoff(self):
        """HTTP-date Retry-After falls back to exp backoff."""
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _GraphRetriable(
            response=_mk_graph_retry_response(429, retry_after="Wed, 21 Oct 2015 07:28:00 GMT")
        )
        retry_state.attempt_number = 1
        wait = graph_retry_kwargs["wait"](retry_state)
        assert 1.0 <= wait <= 4.0

    def test_retry_predicate_matches_graph_retriable(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = _GraphRetriable()
        assert graph_retry_kwargs["retry"](retry_state)

    def test_retry_predicate_rejects_other_exceptions(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = requests.HTTPError("500 Internal")
        assert not graph_retry_kwargs["retry"](retry_state)
```

- [ ] **Step 2: Run to verify failure**

Run: `venv/Scripts/pytest tests/test_retry.py::TestGraphRetryStrategy -v`
Expected: import error on `graph_retry_kwargs`.

- [ ] **Step 3: Implement graph strategy**

Append to `wcp_library/retry.py`:

```python
from tenacity import retry_if_exception_type


def _graph_wait(retry_state) -> float:
    exc = retry_state.outcome.exception()
    if isinstance(exc, _GraphRetriable) and exc.response is not None:
        retry_after = exc.response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return float(retry_after)
    return min(2 ** (retry_state.attempt_number - 1), 60) + random.uniform(0, 3)


graph_retry_kwargs = dict(
    retry=retry_if_exception_type(_GraphRetriable),
    wait=_graph_wait,
    stop=stop_after_attempt(5),
    before_sleep=_before_sleep_log,
    reraise=True,
)
```

- [ ] **Step 4: Run tests**

Run: `venv/Scripts/pytest tests/test_retry.py -v`
Expected: all tests pass (13 from prior + 7 new).

- [ ] **Step 5: Commit**

```bash
git add wcp_library/retry.py tests/test_retry.py
git commit -m "feat(retry): add graph_retry_kwargs honoring Retry-After header"
```

---

### Task 5: Generic retry factory (`make_generic_retry`)

**Files:**
- Modify: `wcp_library/retry.py`
- Modify: `tests/test_retry.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_retry.py`:

```python
from wcp_library.retry import make_generic_retry


class TestMakeGenericRetry:
    def test_defaults_produce_valid_kwargs(self):
        kwargs = make_generic_retry(exceptions=ValueError)
        for key in ("retry", "wait", "stop", "before_sleep", "reraise"):
            assert key in kwargs

    def test_exp_backoff_grows_with_attempt(self):
        kwargs = make_generic_retry(exceptions=ValueError, delay=1, backoff=2, jitter=0)

        rs_1 = MagicMock(); rs_1.attempt_number = 1
        rs_2 = MagicMock(); rs_2.attempt_number = 2
        rs_3 = MagicMock(); rs_3.attempt_number = 3

        # delay * backoff**(attempt-1) with jitter=0
        assert kwargs["wait"](rs_1) == 1   # 1 * 2^0
        assert kwargs["wait"](rs_2) == 2   # 1 * 2^1
        assert kwargs["wait"](rs_3) == 4   # 1 * 2^2

    def test_jitter_adds_randomness(self):
        kwargs = make_generic_retry(exceptions=ValueError, delay=10, backoff=1, jitter=5)
        rs = MagicMock(); rs.attempt_number = 1
        waits = {kwargs["wait"](rs) for _ in range(20)}
        # Expect multiple distinct values due to jitter
        assert len(waits) > 1

    def test_retries_specified_exception(self):
        kwargs = make_generic_retry(exceptions=(ValueError, KeyError))
        rs_val = MagicMock(); rs_val.outcome.exception.return_value = ValueError()
        rs_key = MagicMock(); rs_key.outcome.exception.return_value = KeyError()
        rs_type = MagicMock(); rs_type.outcome.exception.return_value = TypeError()

        assert kwargs["retry"](rs_val)
        assert kwargs["retry"](rs_key)
        assert not kwargs["retry"](rs_type)
```

- [ ] **Step 2: Run to verify failure**

Run: `venv/Scripts/pytest tests/test_retry.py::TestMakeGenericRetry -v`
Expected: import error on `make_generic_retry`.

- [ ] **Step 3: Implement factory**

Append to `wcp_library/retry.py`:

```python
def make_generic_retry(
    exceptions: type[BaseException] | tuple[type[BaseException], ...],
    max_attempts: int = 5,
    delay: int = 2,
    backoff: int = 2,
    jitter: int = 3,
) -> dict:
    """Build tenacity kwargs for arbitrary-exception retry with exp backoff + jitter.

    Policy-identical to the pre-1.12 ``wcp_library.retry`` decorator.

    :param exceptions: exception class or tuple to catch.
    :param max_attempts: total attempts before giving up.
    :param delay: initial delay (seconds).
    :param backoff: multiplier applied to delay each attempt.
    :param jitter: max random seconds added per retry.
    """
    def _wait(retry_state) -> float:
        return delay * (backoff ** (retry_state.attempt_number - 1)) + random.uniform(0, jitter)

    return dict(
        retry=retry_if_exception_type(exceptions),
        wait=_wait,
        stop=stop_after_attempt(max_attempts),
        before_sleep=_before_sleep_log,
        reraise=True,
    )
```

- [ ] **Step 4: Run tests**

Run: `venv/Scripts/pytest tests/test_retry.py -v`
Expected: all tests pass (20 from prior + 4 new).

- [ ] **Step 5: Commit**

```bash
git add wcp_library/retry.py tests/test_retry.py
git commit -m "feat(retry): add make_generic_retry factory"
```

---

### Task 6: Migrate `PostgresConnection` + `AsyncPostgresConnection` primitives

**Files:**
- Modify: `wcp_library/sql/postgres.py`
- Modify: `tests/sql/test_postgres_retry_transaction.py`
- Modify: `tests/sql/test_postgres_retry_transaction_sync.py`
- Modify: `tests/sql/test_postgres_autocommit.py`

Replaces every `@retry` / `@async_retry` (from `wcp_library.sql`) with `@tenacity.retry(**postgres_retry_kwargs)` on the 5 primitives and `remove_matching_data`, for both sync and async classes. Removes `self.retry_error_codes`, `self.retry_limit`, `self._retry_count` attributes (they're unused now).

- [ ] **Step 1: Baseline**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: 405 tests pass (+ 24 new in `test_retry.py` = 429).

- [ ] **Step 2: Update imports in `wcp_library/sql/postgres.py`**

Find the existing import:

```python
from wcp_library.sql import (
    _RETRY_SLEEP_SECONDS,
    _log_giveup,
    _log_retry,
    async_retry,
    classify_db_exception,
    retry,
)
```

Replace with:

```python
from tenacity import AsyncRetrying, Retrying, retry as tenacity_retry

from wcp_library.retry import postgres_retry_kwargs
```

Also drop the module-top imports of `asyncio`, `time`, `random` if they're only used by old retry logic (verify via grep before deleting — `asyncio.sleep` is used inside `retry_transaction`, so keep `asyncio`). After this task, nothing should reference the old `_log_retry` / `_log_giveup` / `classify_db_exception` / `_RETRY_SLEEP_SECONDS` / `time.sleep` inside `postgres.py`. Remove those lines.

- [ ] **Step 3: Swap decorators on all 10 primitives + 2 `remove_matching_data`**

For each of these methods, replace `@retry` with `@tenacity_retry(**postgres_retry_kwargs)`:

- `PostgresConnection.execute` (line ~535 — search for `def execute`)
- `PostgresConnection.safe_execute`
- `PostgresConnection.execute_multiple`
- `PostgresConnection.execute_many`
- `PostgresConnection.fetch_data`
- `PostgresConnection.remove_matching_data`

And for async, replace `@async_retry` with `@tenacity_retry(**postgres_retry_kwargs)`:

- `AsyncPostgresConnection.execute`
- `AsyncPostgresConnection.safe_execute`
- `AsyncPostgresConnection.execute_multiple`
- `AsyncPostgresConnection.execute_many`
- `AsyncPostgresConnection.fetch_data`
- `AsyncPostgresConnection.remove_matching_data`

Also `@retry` on `PostgresConnection._connect` and `@async_retry` on `AsyncPostgresConnection._connect` — replace identically.

Example (sync):

```python
# Before
@retry
def execute(self, query: SQL | Composed | str) -> int:
    ...

# After
@tenacity_retry(**postgres_retry_kwargs)
def execute(self, query: SQL | Composed | str) -> int:
    ...
```

- [ ] **Step 4: Remove instance state that only existed for the old retry**

In both `PostgresConnection.__init__` and `AsyncPostgresConnection.__init__`, delete these three lines:

```python
self._retry_count = 0
self.retry_limit = 50
self.retry_error_codes = postgres_retry_codes
```

Also delete the module-top `postgres_retry_codes = ['08001', '08004', '40P01']` at line 26 — it's unused now (the codes live in `wcp_library.retry`).

- [ ] **Step 5: Rewire `PostgresConnection.retry_transaction` (sync)**

Find the sync `retry_transaction` method (search: `def retry_transaction`). Replace its body with:

```python
def retry_transaction(
    self,
    fn: Callable[..., _T],
    *args: Any,
    **kwargs: Any,
) -> _T:
    """Run ``fn(tx, *args, **kwargs)`` inside a transaction with retry at
    the transaction boundary.

    Uses :data:`wcp_library.retry.postgres_retry_kwargs` — same tiered
    policy as the per-primitive retry, but applied to the whole
    ``with self.transaction() as tx: fn(tx, ...)`` block. Extra
    positional and keyword args are forwarded to ``fn`` after ``tx``.

    :param fn: callable; first arg is the :class:`Transaction` handle.
    :return: the return value of the successful ``fn`` invocation.
    :raises: the final exception if all attempts fail.
    """
    def _block():
        with self.transaction() as tx:
            return fn(tx, *args, **kwargs)
    return Retrying(**postgres_retry_kwargs)(_block)
```

- [ ] **Step 6: Rewire `AsyncPostgresConnection.retry_transaction` (async)**

Replace its body:

```python
async def retry_transaction(
    self,
    fn: Callable[..., Awaitable[_T]],
    *args: Any,
    **kwargs: Any,
) -> _T:
    """Run ``await fn(tx, *args, **kwargs)`` inside a transaction with
    retry at the transaction boundary.

    Uses :data:`wcp_library.retry.postgres_retry_kwargs` — same tiered
    policy as the per-primitive retry.

    :param fn: async callable; first arg is the :class:`AsyncTransaction`.
    :return: the return value of the successful ``fn`` invocation.
    """
    async def _block():
        async with self.transaction() as tx:
            return await fn(tx, *args, **kwargs)
    return await AsyncRetrying(**postgres_retry_kwargs)(_block)
```

- [ ] **Step 7: Update autocommit test that references `retry_error_codes`**

Open `tests/sql/test_postgres_autocommit.py`. Find any reference to `conn.retry_error_codes` or `conn.retry_limit` (there was a test in `TestAsyncAutocommitKwarg` and `TestSyncAutocommitKwarg` that might assert on these). Delete those assertions — the attributes no longer exist.

Actually — check by running:

```bash
grep -n "retry_error_codes\|retry_limit\|_retry_count" tests/sql/test_postgres_autocommit.py
```

If there are hits, remove/adjust. If none, skip this step.

- [ ] **Step 8: Update retry_transaction tests (sleep patch target)**

Open `tests/sql/test_postgres_retry_transaction.py`. The current tests patch `asyncio.sleep` to skip the 5-minute wait. Under tenacity, the sleep happens inside tenacity itself.

Replace the patch:

```python
# Before
with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
    ...

# After
with patch("tenacity.nap.sleep_async", new_callable=AsyncMock) as mock_sleep:
    ...
```

**Important detail:** tenacity passes the wait-time as a float argument. The existing test asserts `mock_sleep.assert_awaited_once_with(300)`. Tenacity calls `sleep_async(seconds)`. After the patch swap, check the assertion still holds — tenacity may call it with `float(300.0)` not `int(300)`. Update the assertion to use `pytest.approx(300, abs=0.1)` or change to `300.0` if needed.

Tests to update:
- `test_retries_on_retriable_error_then_succeeds` — the `mock_sleep.assert_awaited_once_with(300)` call. For postgres, connection-loss codes (08001) get 300s wait, so that's still true.
- `test_retry_limit_respected` — uses the same patch. Update patch target.

Also: tenacity doesn't call `conn._retry_count` (that attribute was removed). Tests that inspected `conn._retry_count` must change. Inspect `retry_state.attempt_number` via the `before_sleep` hook, or check via mock captures on `mock_sleep.await_count`.

Concretely, change any `assert conn._retry_count == N` to `assert mock_sleep.await_count == N` (number of sleeps equals number of retries taken).

- [ ] **Step 9: Update sync retry_transaction test (same pattern)**

Open `tests/sql/test_postgres_retry_transaction_sync.py`. Replace `patch("time.sleep")` with `patch("tenacity.nap.sleep")`.

Same `_retry_count` → `mock_sleep.call_count` swap.

- [ ] **Step 10: Remove postgres_retry_codes reference in transaction module**

If `wcp_library/sql/postgres.py` still imports or references `postgres_retry_codes` anywhere (it shouldn't after Step 4), remove those references.

- [ ] **Step 11: Run full suite**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: all tests pass. New tenacity-backed retry should pass the same retry_transaction tests (with updated patch targets). Existing test counts preserved.

- [ ] **Step 12: Commit**

```bash
git add wcp_library/sql/postgres.py tests/sql/test_postgres_retry_transaction.py tests/sql/test_postgres_retry_transaction_sync.py tests/sql/test_postgres_autocommit.py
git commit -m "$(cat <<'EOF'
refactor(postgres): migrate primitives + retry_transaction to tenacity

Swaps the old @retry / @async_retry decorators for
@tenacity_retry(**postgres_retry_kwargs). Removes
self.retry_error_codes / self.retry_limit / self._retry_count
instance attributes (policy now lives in wcp_library.retry).
Rewires retry_transaction (sync + async) as thin wrappers around
tenacity.Retrying / AsyncRetrying. Updates the retry-transaction
mock tests to patch tenacity's sleep instead of asyncio.sleep /
time.sleep.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Migrate `OracleConnection` + `AsyncOracleConnection` primitives

**Files:**
- Modify: `wcp_library/sql/oracle.py`
- Modify: `tests/sql/test_oracle.py`

Same pattern as Task 6 applied to oracle.

- [ ] **Step 1: Baseline**

Run: `venv/Scripts/pytest tests/sql -q`
Expected: green.

- [ ] **Step 2: Update imports**

In `wcp_library/sql/oracle.py`, replace:

```python
from wcp_library.sql import retry, async_retry
```

with:

```python
from tenacity import retry as tenacity_retry

from wcp_library.retry import oracle_retry_kwargs
```

Also drop `oracle_retry_codes = [...]` at line 13 — unused now.

- [ ] **Step 3: Swap every `@retry` → `@tenacity_retry(**oracle_retry_kwargs)` and `@async_retry` → `@tenacity_retry(**oracle_retry_kwargs)`**

Grep to find them:

```bash
grep -n "@retry\|@async_retry" wcp_library/sql/oracle.py
```

For EACH match, replace as above. Expected methods (both classes): `_connect`, `execute`, `safe_execute`, `execute_multiple`, `execute_many`, `fetch_data`, `remove_matching_data`, `export_df_to_warehouse`, `truncate_table`, `empty_table`, and any others decorated today.

- [ ] **Step 4: Remove instance state in both `__init__` methods**

Delete these three lines from `OracleConnection.__init__` AND `AsyncOracleConnection.__init__`:

```python
self._retry_count = 0
self.retry_limit = 50
self.retry_error_codes = oracle_retry_codes
```

- [ ] **Step 5: Update `tests/sql/test_oracle.py`**

Grep for references that will break:

```bash
grep -n "retry_error_codes\|retry_limit\|_retry_count\|asyncio.sleep\|wcp_library.sql.sleep\|wcp_library.sql.asyncio" tests/sql/test_oracle.py
```

For each hit:
- Patches of `wcp_library.sql.sleep` / `wcp_library.sql.asyncio.sleep` in retry tests → patch `tenacity.nap.sleep` / `tenacity.nap.sleep_async` instead.
- Assertions on `conn._retry_count` → use `mock_sleep.call_count` (sync) or `mock_sleep.await_count` (async).
- Assertions on `conn.retry_limit` / `conn.retry_error_codes` → remove or replace with imports from `wcp_library.retry` (`ORACLE_RETRY_CODES`).

- [ ] **Step 6: Run full suite**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add wcp_library/sql/oracle.py tests/sql/test_oracle.py
git commit -m "refactor(oracle): migrate primitives to tenacity retry"
```

---

### Task 8: Add `wcp_library/graph/__init__.py::_request` helper

**Files:**
- Modify: `wcp_library/graph/__init__.py`
- Modify: `tests/graph/__init__.py` (if needed for `conftest.py`)
- Create: `tests/graph/test_request.py`

- [ ] **Step 1: Baseline**

Run: `venv/Scripts/pytest tests/graph -q`
Expected: green.

- [ ] **Step 2: Write failing test for `_request`**

Create `tests/graph/test_request.py`:

```python
"""Mock tests for the shared _request helper in wcp_library.graph."""
from unittest.mock import MagicMock, patch

import pytest
import requests
from tenacity import RetryError

from wcp_library.graph import _request
from wcp_library.retry import _GraphRetriable


def _ok_response(status=200, payload=b""):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status
    resp.content = payload
    resp.headers = {}
    return resp


class TestRequestHappyPath:
    def test_returns_response_on_2xx(self):
        with patch("wcp_library.graph.requests.request") as mock_req:
            mock_req.return_value = _ok_response(200)
            result = _request("GET", "https://example.com", {"X": "1"})
            assert result.status_code == 200
            mock_req.assert_called_once()


class TestRequestRetries:
    def test_retries_on_429_then_succeeds(self):
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("tenacity.nap.sleep") as mock_sleep:
            bad = _ok_response(429); bad.headers = {"Retry-After": "1"}
            good = _ok_response(200)
            mock_req.side_effect = [bad, good]
            result = _request("GET", "https://example.com", {})
            assert result.status_code == 200
            assert mock_req.call_count == 2
            mock_sleep.assert_called_once_with(1.0)

    def test_retries_on_503(self):
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("tenacity.nap.sleep"):
            bad = _ok_response(503); good = _ok_response(200)
            mock_req.side_effect = [bad, good]
            result = _request("GET", "https://example.com", {})
            assert result.status_code == 200
            assert mock_req.call_count == 2

    def test_retries_on_connection_error(self):
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("tenacity.nap.sleep"):
            mock_req.side_effect = [requests.ConnectionError("boom"), _ok_response(200)]
            result = _request("GET", "https://example.com", {})
            assert result.status_code == 200

    def test_gives_up_after_5_attempts(self):
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("tenacity.nap.sleep"):
            mock_req.return_value = _ok_response(503)
            with pytest.raises(_GraphRetriable):
                _request("GET", "https://example.com", {})
            assert mock_req.call_count == 5


class TestRequestNonRetryable:
    def test_500_raises_http_error_no_retry(self):
        resp = _ok_response(500)
        resp.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
        with patch("wcp_library.graph.requests.request") as mock_req, \
             patch("tenacity.nap.sleep") as mock_sleep:
            mock_req.return_value = resp
            with pytest.raises(requests.HTTPError):
                _request("GET", "https://example.com", {})
            assert mock_req.call_count == 1
            mock_sleep.assert_not_called()

    def test_404_raises_http_error_no_retry(self):
        resp = _ok_response(404)
        resp.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        with patch("wcp_library.graph.requests.request") as mock_req:
            mock_req.return_value = resp
            with pytest.raises(requests.HTTPError):
                _request("GET", "https://example.com", {})
```

- [ ] **Step 3: Run to verify failure**

Run: `venv/Scripts/pytest tests/graph/test_request.py -v`
Expected: `ImportError: cannot import name '_request' from 'wcp_library.graph'`.

- [ ] **Step 4: Implement `_request`**

Open `wcp_library/graph/__init__.py`. Add at the bottom (after the existing `get_headers` function):

```python
from tenacity import retry as tenacity_retry

from wcp_library.retry import GRAPH_RETRIABLE_STATUSES, _GraphRetriable, graph_retry_kwargs


@tenacity_retry(**graph_retry_kwargs)
def _request(method: str, url: str, headers: dict, **kwargs) -> requests.Response:
    """Execute a Graph HTTP request with retry on 429/503/504 and network errors.

    Module-private. Callers in :mod:`wcp_library.graph.sharepoint`,
    :mod:`wcp_library.graph.mail`, :mod:`wcp_library.graph.subscription`
    invoke it instead of ``requests.*`` directly. ``timeout`` and
    ``raise_for_status()`` are handled here.

    :param method: HTTP verb ("GET", "POST", "PATCH", "PUT", "DELETE").
    :param url: absolute URL.
    :param headers: request headers (including Authorization).
    :param kwargs: forwarded to :func:`requests.request`
        (e.g. ``json=``, ``data=``).
    :raises requests.HTTPError: for non-retryable 4xx/5xx responses.
    :raises _GraphRetriable: for 429/503/504 or network errors when
        tenacity has exhausted its retry budget.
    :return: :class:`requests.Response` for status < 400 outside of the
        retryable set.
    """
    try:
        response = requests.request(
            method, url, headers=headers, timeout=REQUEST_TIMEOUT, **kwargs,
        )
    except (requests.ConnectionError, requests.Timeout) as e:
        raise _GraphRetriable(underlying=e) from e
    if response.status_code in GRAPH_RETRIABLE_STATUSES:
        raise _GraphRetriable(response=response)
    response.raise_for_status()
    return response
```

- [ ] **Step 5: Run tests**

Run: `venv/Scripts/pytest tests/graph/test_request.py -v`
Expected: 6 tests pass.

- [ ] **Step 6: Run full suite**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: still green (nothing else uses `_request` yet).

- [ ] **Step 7: Commit**

```bash
git add wcp_library/graph/__init__.py tests/graph/test_request.py
git commit -m "feat(graph): add _request helper with tenacity retry for 429/503/504 + network errors"
```

---

### Task 9: Migrate `wcp_library/graph/sharepoint.py` call sites

**Files:**
- Modify: `wcp_library/graph/sharepoint.py`
- Modify: `tests/graph/test_sharepoint.py` (only if tests patch `requests.*` directly — update patch targets).

- [ ] **Step 1: Baseline**

Run: `venv/Scripts/pytest tests/graph -q`
Expected: green.

- [ ] **Step 2: Add import for `_request`**

At the top of `wcp_library/graph/sharepoint.py`, change:

```python
import requests
```

Keep that line (still need `requests.RequestException` for the outer except clauses), and ADD:

```python
from wcp_library.graph import _request
```

- [ ] **Step 3: Swap inline requests calls for `_request`**

For every function in `sharepoint.py` that calls `requests.get`, `requests.post`, `requests.patch`, `requests.put`, `requests.delete`:

Replace the inline call pattern

```python
response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
response.raise_for_status()
```

with

```python
response = _request("GET", url, headers)
```

and similarly for other verbs. For calls that pass JSON/data:

```python
# Before
response = requests.patch(
    url,
    headers={**headers, "Content-Type": "application/json"},
    json=payload,
    timeout=REQUEST_TIMEOUT,
)
response.raise_for_status()

# After
response = _request(
    "PATCH",
    url,
    {**headers, "Content-Type": "application/json"},
    json=payload,
)
```

Functions that need updates (grep will confirm exact list):

- `get_site_metadata`
- `get_drives`
- `list_folder`
- `get_file_metadata`
- `get_file_content`
- `get_file_content_by_id`
- `upload_file`
- `download_file`
- `move_file`
- `rename_file` (wraps `move_file` — should not need changes)
- `copy_file`
- `remove_file`
- `get_lists`
- `get_list_metadata`
- `create_list`
- `remove_list`
- `get_list_items`
- `get_list_item_metadata`
- `create_list_item`
- `update_list_item`
- `remove_list_item`

And the private `_iter_pages` helper — its `requests.get(next_url, ...)` call inside the loop needs the same swap:

```python
# Before
response = requests.get(next_url, headers=headers, timeout=REQUEST_TIMEOUT)
response.raise_for_status()

# After
response = _request("GET", next_url, headers)
```

Keep each public helper's outer `try/except requests.RequestException → return None` exactly as it is. Only the HTTP call + the `raise_for_status` swap changes; the surrounding error handling stays.

- [ ] **Step 4: Run full suite**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: sharepoint tests may now fail because the mock patch target changed (tests patched `wcp_library.graph.sharepoint.requests.get` previously; now all HTTP goes through `wcp_library.graph._request`).

- [ ] **Step 5: Update tests to patch `_request` instead of `requests.*`**

In `tests/graph/test_sharepoint.py`, find every `patch("wcp_library.graph.sharepoint.requests.X", ...)` and replace with `patch("wcp_library.graph.sharepoint._request", ...)`.

The mock return-value shape changes: instead of returning a mock response that has `.raise_for_status()` + `.json()`, `_request` already did the status-check and returns the response. So the mock is simpler:

```python
# Before
def _ok_json(payload):
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.json.return_value = payload
    return mock

# After
def _ok_json(payload):
    mock = MagicMock()
    mock.json.return_value = payload
    return mock
```

For error-path tests that previously simulated `requests.RequestException`, now simulate `_request` raising:

```python
# Before
def _http_error(status=500):
    err = requests.exceptions.RequestException("boom")
    err.response = MagicMock(status_code=status, text="error body")
    return err

# After (same shape — _request can raise any RequestException subtype)
def _http_error(status=500):
    err = requests.exceptions.HTTPError(f"{status} Error")
    err.response = MagicMock(status_code=status, text="error body")
    return err
```

And the `side_effect=_http_error()` pattern works identically on `_request` mocks.

- [ ] **Step 6: Run suite again**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add wcp_library/graph/sharepoint.py tests/graph/test_sharepoint.py
git commit -m "refactor(sharepoint): route all HTTP through _request for retry coverage"
```

---

### Task 10: Migrate `wcp_library/graph/mail.py` + `wcp_library/graph/subscription.py`

**Files:**
- Modify: `wcp_library/graph/mail.py`
- Modify: `wcp_library/graph/subscription.py`
- Modify: `tests/graph/test_mail.py`
- Modify: `tests/graph/test_subscription.py`

Same pattern as Task 9 applied to mail + subscription.

- [ ] **Step 1: Baseline**

Run: `venv/Scripts/pytest tests/graph -q`
Expected: green.

- [ ] **Step 2: Update `mail.py`**

In `wcp_library/graph/mail.py`, add `from wcp_library.graph import _request` at the top. Replace every `requests.get/post/...` inline call with `_request("METHOD", url, headers, **kwargs)`. Drop the `response.raise_for_status()` calls (now inside `_request`).

Public functions to update (grep to confirm):
- `get_mailbox_folders`
- `get_email_metadata`
- `get_emails`
- `get_attachments`

`save_attachment` uses aiofiles + an existing dict, not `requests.*` — leave alone.

- [ ] **Step 3: Update `subscription.py`**

Same treatment. Functions:
- `create_subscription`
- `get_subscription`
- `list_subscriptions`
- `update_subscription_expiration`
- `delete_subscription`
- `reauthorize_subscription`
- `recreate_subscription`
- `update_notification_url`

- [ ] **Step 4: Update `tests/graph/test_mail.py` + `test_subscription.py`**

Swap patch targets from `wcp_library.graph.mail.requests.X` to `wcp_library.graph.mail._request` (same for subscription). Adjust mock shapes as in Task 9 Step 5 (no `raise_for_status` on the mock).

- [ ] **Step 5: Run suite**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add wcp_library/graph/mail.py wcp_library/graph/subscription.py tests/graph/test_mail.py tests/graph/test_subscription.py
git commit -m "refactor(graph): route mail + subscription HTTP through _request"
```

---

### Task 11: Migrate `wcp_library/browser_automation/browser.py`

**Files:**
- Modify: `wcp_library/browser_automation/browser.py`

- [ ] **Step 1: Update the single `@retry` call site**

Open `wcp_library/browser_automation/browser.py`. Find:

```python
from wcp_library import retry

@retry(exceptions=(selenium_exceptions.WebDriverException,))
```

Replace the import:

```python
from tenacity import retry as tenacity_retry

from wcp_library.retry import make_generic_retry
```

Replace the decorator:

```python
@tenacity_retry(**make_generic_retry(exceptions=(selenium_exceptions.WebDriverException,)))
```

- [ ] **Step 2: Verify no other references**

Run: `grep -n "from wcp_library import retry\|@retry\|async_retry" wcp_library/browser_automation/`
Expected: no matches.

- [ ] **Step 3: Run suite**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: all tests pass (browser_automation doesn't have tests, but nothing should have regressed).

- [ ] **Step 4: Commit**

```bash
git add wcp_library/browser_automation/browser.py
git commit -m "refactor(browser_automation): migrate to tenacity via make_generic_retry"
```

---

### Task 12: Delete old retry code

**Files:**
- Modify: `wcp_library/__init__.py`
- Modify: `wcp_library/sql/__init__.py`

All callers have migrated. Delete the old implementations.

- [ ] **Step 1: Verify no remaining callers**

Run:

```bash
grep -rn "from wcp_library import retry\|from wcp_library import async_retry\|from wcp_library.sql import retry\|from wcp_library.sql import async_retry\|classify_db_exception\|_log_retry\|_log_giveup\|_RETRY_SLEEP_SECONDS\|_RETRIABLE_DB_ERRORS" wcp_library/ tests/
```

Expected: no matches in `wcp_library/`. May still match in `tests/` if any test imports these for direct unit testing — those tests must also be removed or rewritten for the new location.

- [ ] **Step 2: Remove old retry code from `wcp_library/__init__.py`**

Open `wcp_library/__init__.py`. Delete:
- The `MAX_ATTEMPTS`, `DELAY`, `BACKOFF`, `JITTER` module-level constants.
- The entire `def retry(...)` function + its body.
- The entire `def async_retry(...)` function + its body.
- The `import asyncio`, `import random`, `import time` imports IF they're only used by the deleted retry functions (grep the remaining file to confirm — keep them only if something else uses them).

Keep: `APPLICATION_PATH`, `divide_chunks`, all other public symbols.

- [ ] **Step 3: Remove old retry code from `wcp_library/sql/__init__.py`**

Open `wcp_library/sql/__init__.py`. Delete:
- `_RETRIABLE_DB_ERRORS`
- `_RETRY_SLEEP_SECONDS`
- `classify_db_exception`
- `_log_retry`
- `_log_giveup`
- `retry` decorator function
- `async_retry` decorator function
- The unused `asyncio`, `functools.wraps`, `time.sleep` imports (keep only what's still used — likely just `import logging` + `logger = logging.getLogger(__name__)` survive).

After this step `wcp_library/sql/__init__.py` should be nearly empty (possibly just the `logger` line or empty).

- [ ] **Step 4: Run suite**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: all tests pass. If anything fails here it means a caller still imports the deleted names — fix those imports before proceeding.

- [ ] **Step 5: Commit**

```bash
git add wcp_library/__init__.py wcp_library/sql/__init__.py
git commit -m "$(cat <<'EOF'
refactor: delete old retry implementations from wcp_library and wcp_library.sql

All call sites migrated to tenacity-backed strategies in
wcp_library.retry. This removes:
- wcp_library.retry / async_retry (generic exp-backoff decorator)
- wcp_library.sql.retry / async_retry (DB-specific decorator)
- wcp_library.sql.classify_db_exception, _log_retry, _log_giveup
- Supporting constants (MAX_ATTEMPTS, DELAY, BACKOFF, JITTER,
  _RETRY_SLEEP_SECONDS, _RETRIABLE_DB_ERRORS)

BREAKING: `from wcp_library import retry` no longer works — retry
is now a module (wcp_library.retry), not a function. Downstream
consumers that imported the old decorators must use
`from wcp_library.retry import make_generic_retry` +
`tenacity.retry(**make_generic_retry(...))` instead.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 13: Update wiki docs

**Files:**
- Modify: `docs/Wiki Docs/Connection - Postgres`
- Modify: `docs/Wiki Docs/Connection - Oracle`
- Modify: `docs/Wiki Docs/Helper - Microsoft Graph API`
- Modify: `docs/Wiki Docs/Helper - Browser Automation`
- Modify: `docs/Wiki Docs/#Extra Uses`

- [ ] **Step 1: Update `Connection - Postgres` retry paragraph**

Find the paragraph that currently reads (~line 16):

> The five primitives (execute, safe_execute, execute_multiple, execute_many, fetch_data) and remove_matching_data are retry-decorated. commit(), rollback(), transaction(), retry_transaction(), close_connection(), and the warehouse composites ... are NOT retry-decorated ...

Replace with:

> The five primitives (execute, safe_execute, execute_multiple, execute_many, fetch_data) and remove_matching_data are retry-decorated via `@tenacity.retry(**postgres_retry_kwargs)` (policy defined in `wcp_library.retry`). commit(), rollback(), transaction(), retry_transaction(), close_connection(), and the warehouse composites are NOT retry-decorated on the connection class — but the composites dispatch through the retry-decorated primitives, so retry still covers them transitively. Transaction / AsyncTransaction primitives are intentionally undecorated; use retry_transaction() for transaction-boundary retry.
>
> The retry policy is tiered by error code:
> * Connection-loss codes (`08001`, `08004`) — fixed 300s wait, up to 50 attempts.
> * Deadlock / transient codes (`40P01`) — exponential backoff with jitter (1–30s range), up to 50 attempts.
>
> Retriable codes: `['08001', '08004', '40P01']`. Other psycopg `OperationalError` / `DatabaseError` codes propagate immediately.

Apply the same tiered-paragraph update to the `AsyncPostgresConnection` section.

- [ ] **Step 2: Update `Connection - Oracle` retry paragraph**

Similar update for oracle — tiered by code, reference the new strategy module. Retriable codes stay as-is: `['ORA-01033', 'DPY-6005', 'DPY-4011', 'ORA-08103', 'ORA-04021', 'ORA-01652']`.

Split them into two tiers in the doc:
* Connection loss: `ORA-01033`, `DPY-6005`, `DPY-4011` — fixed 300s wait.
* Transient: `ORA-08103`, `ORA-04021`, `ORA-01652` — exp backoff.

- [ ] **Step 3: Update `Helper - Microsoft Graph API` with retry coverage**

Add a new section near the top (under "Key Features"):

```
### Retry

Every Graph HTTP call (across sharepoint, mail, subscription) goes
through a shared internal helper that retries on:
- HTTP 429 Too Many Requests — honors the `Retry-After` response
  header exactly.
- HTTP 503 / 504 — exponential backoff up to 60 seconds.
- Network errors (`requests.ConnectionError`, `requests.Timeout`) —
  exponential backoff up to 60 seconds.

Up to 5 attempts total. Retries are transparent to callers; the
public helpers still return `None` on non-retryable failure.
```

- [ ] **Step 4: Update `Helper - Browser Automation` retry example**

The wiki likely doesn't document the exact decorator usage, but if it does (grep for `@retry` or `retry(exceptions`), update to show the new form:

```python
from tenacity import retry as tenacity_retry
from wcp_library.retry import make_generic_retry

@tenacity_retry(**make_generic_retry(exceptions=(selenium_exceptions.WebDriverException,)))
def some_browser_op(self): ...
```

If the wiki doesn't mention retry in browser automation, skip this step.

- [ ] **Step 5: Update `#Extra Uses` — remove stale `retry`/`async_retry` doc**

The `#Extra Uses` page documents the generic `retry`/`async_retry` decorators. Remove those sections entirely (the decorators no longer exist at `wcp_library.retry` as callables). Replace with a short note pointing to `wcp_library.retry.make_generic_retry`:

```
### retry strategies (moved)

Retry strategies now live in `wcp_library.retry`. Apply them via
tenacity:

```python
from tenacity import retry as tenacity_retry
from wcp_library.retry import make_generic_retry

@tenacity_retry(**make_generic_retry(exceptions=(SomeError,), max_attempts=5))
def something(): ...
```

See `wcp_library/retry.py` for the full set of policies
(`postgres_retry_kwargs`, `oracle_retry_kwargs`, `graph_retry_kwargs`,
`make_generic_retry`).
```

- [ ] **Step 6: Commit**

```bash
git add "docs/Wiki Docs/"
git commit -m "docs(wiki): document tenacity-backed retry policies"
```

---

### Task 14: Version bump to 1.12.0 + CHANGELOG entry

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Bump version**

Open `pyproject.toml`. Change:

```toml
version = "1.11.0"
```

to:

```toml
version = "1.12.0"
```

- [ ] **Step 2: Final test run**

Run: `venv/Scripts/pytest tests/ --no-header -q`
Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "$(cat <<'EOF'
chore: bump version to 1.12.0 (tenacity retry consolidation)

Consolidates three retry implementations into wcp_library.retry
(tenacity-backed strategy configs). Adds HTTP retry coverage to
every wcp_library.graph helper (honors Retry-After on 429, exp
backoff on 503/504/network).

Breaking: removes wcp_library.retry / async_retry (function), and
wcp_library.sql.retry / async_retry. Call sites must use
`tenacity.retry(**<strategy>)` with strategies from
wcp_library.retry. See PR description for migration notes.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review

**Spec coverage:**
- [x] `wcp_library/retry.py` module with all constants, classifier, strategies, generic factory → Tasks 1-5
- [x] Tiered SQL retry (connection-loss fixed 300s, transient exp backoff) → Task 3
- [x] Graph retry for 429/503/504/network (honors Retry-After) → Task 4, 8
- [x] `_GraphRetriable` internal sentinel → Tasks 1, 4, 8
- [x] `retry_transaction` rewired (sync + async) → Task 6
- [x] SQL primitive decorator migration → Tasks 6, 7
- [x] Graph helper migration (sharepoint + mail + subscription) → Tasks 8, 9, 10
- [x] `_iter_pages` uses `_request` → Task 9 Step 3
- [x] Browser automation migration → Task 11
- [x] Delete old retry code (wcp_library + wcp_library.sql) → Task 12
- [x] Remove instance attributes `retry_error_codes`, `retry_limit`, `_retry_count` → Tasks 6, 7
- [x] Wiki doc updates → Task 13
- [x] Version bump + CHANGELOG callout → Task 14

**Placeholder scan:** No "TBD", "TODO", "implement later", "fill in details", etc. Every step has concrete code or exact file/grep commands.

**Type consistency:**
- `postgres_retry_kwargs` / `oracle_retry_kwargs` / `graph_retry_kwargs`: all dicts, used identically at call sites.
- `make_generic_retry(exceptions, max_attempts, delay, backoff, jitter) -> dict` — signature matches both the spec and the consumers (Task 11).
- `_GraphRetriable(response=None, underlying=None)` — matches Task 1, Task 8 (raise sites), Task 4 (test construction).
- `_request(method, url, headers, **kwargs) -> requests.Response` — consistent across Tasks 8, 9, 10.

**Spec adherence on version:** Plan uses `1.12.0` (matches spec). Plan uses `tenacity = "~9.1.4"` (matches spec after user correction).

No gaps identified.
