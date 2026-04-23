# Spec: consolidate retry policies onto tenacity + add retry to `wcp_library.graph`

**Status:** Approved — ready for implementation plan
**Date:** 2026-04-22
**Author:** Mitch Petersen (with Claude)
**Scope:** `wcp_library` (new `wcp_library.retry` module); call-site updates in `wcp_library.sql.{postgres,oracle}`, `wcp_library.graph.{__init__,sharepoint,mail,subscription}`, `wcp_library.browser_automation.browser`

## Motivation

The library currently has three unrelated retry implementations:

1. `wcp_library.retry` / `async_retry` — a generic exponential-backoff + jitter decorator factory. Used by `browser_automation.browser`.
2. `wcp_library.sql.retry` / `async_retry` — a DB-specific decorator. Reads `self.retry_error_codes` and `self.retry_limit` off the connection instance; fixed 300s sleep × 50 attempts; catches both `oracledb` and `psycopg` errors regardless of which DB the connection serves. Used by `sql.postgres` and `sql.oracle` primitives.
3. `PostgresConnection.retry_transaction` / `AsyncPostgresConnection.retry_transaction` — a ~40-line hand-rolled retry loop that duplicates the policy of #2 at transaction granularity.

`wcp_library.graph` (SharePoint, Mail, Subscription) has **no** retry at all despite Microsoft Graph's aggressive throttling (HTTP 429 with `Retry-After`) and occasional 503/504/gateway timeouts.

The three existing impls have diverged over time: parity is coincidental, not enforced. The DB retry catches cross-library exception types unnecessarily. The fixed 300s wait is inappropriate for deadlocks (resolves in milliseconds) and appropriate for connection loss (DB maintenance windows). The generic retry's arguments (`max_attempts`, `delay`, `backoff`, `jitter`) are a subset of tenacity's surface.

This spec replaces all three with a single layer of tenacity-backed strategy configs and adds retry to every `wcp_library.graph` HTTP call.

## Goals

- One module (`wcp_library.retry`) that owns every retry-policy decision in the library.
- Tenacity is the retry engine; we ship strategy configs (dicts of tenacity kwargs) and a small number of helpers.
- SQL retry gains **tiered** wait behavior: connection-loss codes keep the current patient 300s fixed wait; transient/conflict codes switch to exponential backoff with jitter.
- `wcp_library.graph` gets retry on HTTP 429 (honoring `Retry-After`), 503, 504, and network errors (`ConnectionError`, `Timeout`).
- `retry_transaction` becomes a ~5-line wrapper around `tenacity.Retrying` / `AsyncRetrying` with the postgres strategy.
- Zero behavior change for existing call sites *at the public API level* beyond the retry-policy tiering: connection primitives still return rowcount; composites still return record counts; graph helpers still return `None` on non-retryable error.

## Non-goals

- No 401 handling / token refresh in `wcp_library.graph`. Expired tokens surface as before (caller reconstructs headers and retries at their level).
- No change to the psycopg3 `connection.transaction()` semantics or the `Transaction` / `AsyncTransaction` surface.
- No new retry configuration knobs for consumers. The strategies in `wcp_library.retry` are the only supported policies; per-call overrides are via tenacity's own kwargs if needed.
- Oracle retry policy keeps its current error code set (no new codes added or removed).

## Architecture

### `wcp_library/retry.py` — single module, all policy

Contains:

**Error code constants** (frozen sets):

```python
_POSTGRES_CONNECTION_LOSS = frozenset({"08001", "08004"})
_POSTGRES_TRANSIENT       = frozenset({"40P01"})
POSTGRES_RETRY_CODES      = _POSTGRES_CONNECTION_LOSS | _POSTGRES_TRANSIENT

_ORACLE_CONNECTION_LOSS   = frozenset({"ORA-01033", "DPY-6005", "DPY-4011"})
_ORACLE_TRANSIENT         = frozenset({"ORA-08103", "ORA-04021", "ORA-01652"})
ORACLE_RETRY_CODES        = _ORACLE_CONNECTION_LOSS | _ORACLE_TRANSIENT

GRAPH_RETRIABLE_STATUSES  = frozenset({429, 503, 504})
```

**Classification helper** (for SQL):

```python
def _extract_full_code(exc: BaseException) -> str | None:
    """Pull `full_code` off the driver's error object if present.
    Returns None when the exception doesn't fit the expected shape."""
    try:
        (error_obj,) = exc.args
    except (ValueError, TypeError):
        return None
    if isinstance(error_obj, str):
        return None
    return getattr(error_obj, "full_code", None)
```

**SQL strategy factory** — produces a dict of tenacity kwargs:

```python
def _make_sql_retry(
    catchable: tuple[type, ...],
    connection_loss_codes: frozenset[str],
    transient_codes: frozenset[str],
    name: str,
) -> dict:
    retriable_codes = connection_loss_codes | transient_codes

    def _should_retry(retry_state) -> bool:
        exc = retry_state.outcome.exception()
        if not isinstance(exc, catchable):
            return False
        return _extract_full_code(exc) in retriable_codes

    def _wait(retry_state) -> float:
        code = _extract_full_code(retry_state.outcome.exception())
        if code in connection_loss_codes:
            return 300.0                               # patient — DB maintenance window
        return min(2 ** (retry_state.attempt_number - 1), 30) + random.uniform(0, 3)

    def _before_sleep(retry_state):
        code = _extract_full_code(retry_state.outcome.exception())
        logger.info(
            "%s retry %d: code=%s, waiting %.1fs",
            name, retry_state.attempt_number, code, _wait(retry_state),
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

**Graph strategy + internal exception**:

```python
class _GraphRetriable(Exception):
    """Signals that tenacity should retry a Graph request.
    Carries either the retryable Response or the underlying network error.
    Module-private; callers rely on ``requests.exceptions.HTTPError`` and the
    None-on-error contract of public helpers.
    """
    def __init__(self, response=None, underlying=None):
        self.response = response
        self.underlying = underlying


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

**Generic retry factory** (replacement for the deleted `wcp_library.retry` function):

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

**Shared logging hook** — `_before_sleep_log(retry_state)` logs `"retry N: <exc_type> — waiting X.Xs"` at INFO level.

### `wcp_library/graph/__init__.py` — shared HTTP helper

A single `_request` helper replaces ~25 inline `requests.*` calls across `sharepoint.py`, `mail.py`, `subscription.py`:

```python
from tenacity import retry as tenacity_retry
from wcp_library.retry import _GraphRetriable, GRAPH_RETRIABLE_STATUSES, graph_retry_kwargs


@tenacity_retry(**graph_retry_kwargs)
def _request(method: str, url: str, headers: dict, **kwargs) -> requests.Response:
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

Each graph helper swaps `requests.get(url, headers=..., timeout=REQUEST_TIMEOUT)` for `_request("GET", url, headers)`, and drops the explicit `response.raise_for_status()` (now inside `_request`). Each helper keeps its outer `try/except requests.RequestException → return None` so the public `None`-on-error contract is preserved. `_iter_pages` uses `_request` so paginated calls retry per-page.

### SQL primitive decoration

Each primitive on `PostgresConnection` / `AsyncPostgresConnection` / `OracleConnection` / `AsyncOracleConnection` swaps its `@retry` / `@async_retry` decorator for the equivalent tenacity decorator:

```python
# wcp_library/sql/postgres.py
from tenacity import retry as tenacity_retry
from wcp_library.retry import postgres_retry_kwargs

class PostgresConnection(SyncExecutor):
    @tenacity_retry(**postgres_retry_kwargs)
    def execute(self, query): ...
```

Same for `safe_execute`, `execute_multiple`, `execute_many`, `fetch_data`, `remove_matching_data` — on both sync and async, for both postgres and oracle. The `self.retry_error_codes` and `self.retry_limit` instance attributes are deleted (policy is now module-level); `self._retry_count` is also deleted (tenacity tracks `retry_state.attempt_number` internally).

### `retry_transaction` rewired

Sync:

```python
def retry_transaction(self, fn, *args, **kwargs):
    def _block():
        with self.transaction() as tx:
            return fn(tx, *args, **kwargs)
    return Retrying(**postgres_retry_kwargs)(_block)
```

Async:

```python
async def retry_transaction(self, fn, *args, **kwargs):
    async def _block():
        async with self.transaction() as tx:
            return await fn(tx, *args, **kwargs)
    return await AsyncRetrying(**postgres_retry_kwargs)(_block)
```

Signature unchanged (`fn, *args, **kwargs`). Return semantics unchanged.

### Browser automation decoration

`browser_automation/browser.py` changes one decorator at one call site:

```python
# Before
from wcp_library import retry
@retry(exceptions=(selenium_exceptions.WebDriverException,))

# After
from tenacity import retry as tenacity_retry
from wcp_library.retry import make_generic_retry
@tenacity_retry(**make_generic_retry(exceptions=(selenium_exceptions.WebDriverException,)))
```

## API changes

### Added

- `wcp_library/retry.py` module exporting:
  - `postgres_retry_kwargs: dict` — tenacity kwargs for Postgres tiered retry.
  - `oracle_retry_kwargs: dict` — tenacity kwargs for Oracle tiered retry.
  - `graph_retry_kwargs: dict` — tenacity kwargs for Graph HTTP retry.
  - `make_generic_retry(exceptions, max_attempts=5, delay=2, backoff=2, jitter=3) -> dict` — factory replacing the deleted `wcp_library.retry` decorator.
  - `POSTGRES_RETRY_CODES`, `ORACLE_RETRY_CODES`, `GRAPH_RETRIABLE_STATUSES` — frozen sets (public for inspection/extension).
- `wcp_library/graph/__init__.py` adds a module-private `_request` helper. Public callers don't need it directly.

### Removed

- `wcp_library/__init__.py`: `retry(exceptions, ...)`, `async_retry(exceptions, ...)`, constants `MAX_ATTEMPTS`, `DELAY`, `BACKOFF`, `JITTER`. `wcp_library.retry` is now a module, not a function — `from wcp_library import retry` now returns the module object, which breaks any caller that expected a callable.
- `wcp_library/sql/__init__.py`: `retry`, `async_retry`, `classify_db_exception`, `_log_retry`, `_log_giveup`, `_RETRIABLE_DB_ERRORS`, `_RETRY_SLEEP_SECONDS`. Responsibility moves to `wcp_library/retry.py`.
- Instance attributes `self.retry_error_codes`, `self.retry_limit`, `self._retry_count` on `PostgresConnection`, `AsyncPostgresConnection`, `OracleConnection`, `AsyncOracleConnection`.

### Unchanged (public contract preserved)

- Every SQL connection primitive signature + return value.
- Every graph helper signature + return value (`None` on non-retryable error; rows/dicts/bytes on success).
- `retry_transaction(fn, *args, **kwargs)` signature and return.

## Migration + testing

**Implementation order** (plan will refine):

1. Add `tenacity = "~9.1.4"` (matches the version already pinned in other D&A projects) to `pyproject.toml`; refresh `poetry.lock`.
2. Create `wcp_library/retry.py` with strategy configs, factories, classifier. Pure addition — no existing call sites change yet.
3. Introduce `wcp_library/graph/__init__.py::_request`; migrate sharepoint/mail/subscription call sites one module at a time.
4. Migrate `wcp_library/sql/postgres.py` primitives + `retry_transaction`.
5. Migrate `wcp_library/sql/oracle.py` primitives.
6. Migrate `wcp_library/browser_automation/browser.py`.
7. Delete the now-unused `retry` / `async_retry` / classifier / constants from `wcp_library/__init__.py` and `wcp_library/sql/__init__.py`.
8. Update wiki docs that touch retry policy or `wcp_library.retry` imports (`Connection - Postgres`, `Connection - Oracle`, `Helper - Microsoft Graph API`, `Helper - Browser Automation`, `#Extra Uses`).
9. Bump `pyproject.toml` version to `1.12.0`; update CHANGELOG with the breaking-change callout.

**Testing:**

- Existing 405-test suite must remain green.
- Retry tests (`tests/sql/test_postgres_retry_transaction*.py`) currently patch `asyncio.sleep` / `time.sleep` to skip the 300s wait. Under tenacity, the sleep path lives in tenacity's `sleep` / `sleep_async` — update patch targets accordingly, or inject `sleep=lambda _: None` into the test-local retry kwargs.
- No new tests required for behavior parity. Optionally add one new test verifying tiered wait: deadlock error (40P01) retries with exp backoff under a second, connection-loss error (08001) retries with ≥300s wait (mocked sleep captures the requested duration).
- New graph retry behavior gets a handful of mock tests: 429 with `Retry-After` header waits exactly that duration; 503 exp-backoffs; 500 does NOT retry (propagates as HTTPError through `_request`, swallowed to None by caller); `ConnectionError` retries.

## Rollout

- **Version**: `1.12.0`. Accepts the semver bend (technically a breaking change to `wcp_library.retry` and `wcp_library.sql.retry` imports) because:
  - Downstream consumers (the D&A team's projects) have been writing retry logic inline rather than importing from this library — breakage risk is near-zero in practice.
  - All internal call sites migrate atomically in this PR.
  - 2.0.0 would be the disciplined call; 1.12.0 is the pragmatic one.
- **PR description** must prominently flag:
  - `from wcp_library import retry, async_retry` no longer works; use `wcp_library.retry.make_generic_retry` + `tenacity.retry`.
  - `from wcp_library.sql import retry, async_retry` no longer works; SQL primitives handle retry internally.
  - SQL retry policy is now tiered — deadlocks recover in seconds rather than 5-minute intervals. Behavior improvement, not regression.
- **CHANGELOG entry** under the `[1.12.0]` heading lists the three removals, the new module, the graph retry addition, and the tiered-wait behavior change.

## Open questions

None at spec-writing time. Any ambiguity that surfaces during plan authoring or implementation should come back to this spec as an amendment rather than being resolved ad-hoc.
