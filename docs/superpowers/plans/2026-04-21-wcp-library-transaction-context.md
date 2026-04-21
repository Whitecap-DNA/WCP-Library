# wcp_library — Transaction Context + Executor Factoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `autocommit` kwarg, a `transaction()` context manager, and a shared `Executor` abstract base to `wcp_library/sql/postgres.py` — both sync and async — without breaking any existing consumer.

**Architecture:** Three-layer factoring: an `Executor` ABC with abstract primitives (`execute`, `safe_execute`, `execute_many`, `execute_multiple`, `fetch_data`) and concrete composites (`export_df_to_warehouse`, `upsert_df_to_warehouse`, `truncate_table`, `empty_table`). `PostgresConnection`/`AsyncPostgresConnection` implement the primitives via per-call pool checkout (autocommit controlled); `Transaction`/`AsyncTransaction` implement the primitives against a held connection with no per-call commit. Connection classes add `autocommit: bool = True` kwarg, `commit()`/`rollback()` manual control, `transaction()` context manager, and `retry_transaction(fn)` helper.

**Tech Stack:** Python 3.12+, psycopg3 (`psycopg`, `psycopg_pool`), tenacity (for retry decorators), pandas (for warehouse composites). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-04-21-wcp-library-transaction-context-design.md`

---

## File Structure

This plan assumes the library layout:

```
wcp_library/
├── sql/
│   ├── __init__.py
│   ├── postgres.py        # main file — modified heavily
│   └── oracle.py          # untouched
└── ...
tests/
├── sql/
│   └── test_postgres.py   # existing — extended
└── ...
```

If the library's real layout differs, translate paths accordingly (e.g., `src/wcp_library/...`). The relative structure of which classes/methods change is what matters.

**Modify:**
- `wcp_library/sql/postgres.py` — primary file for all changes.
- `tests/sql/test_postgres.py` (or whatever the library's test location is) — new tests per task.

**New (inside `wcp_library/sql/postgres.py`):**
- `Executor` abstract base class
- `AsyncTransaction` class (async transaction handle)
- `Transaction` class (sync transaction handle)

No new files — all classes co-located in `postgres.py` because they're tightly coupled and the file is already the single authoritative place for Postgres access.

---

### Task 1: Introduce `Executor` ABC + relocate composite methods (no behaviour change)

**Files:**
- Modify: `wcp_library/sql/postgres.py`
- Test: `tests/sql/test_postgres.py`

Pure relocation — move `export_df_to_warehouse`, `upsert_df_to_warehouse`, `truncate_table`, `empty_table` from `AsyncPostgresConnection` and `PostgresConnection` to a shared `Executor` ABC. Connection classes inherit. Existing tests must pass unchanged.

- [ ] **Step 1: Baseline**

Run: the library's test command (e.g., `pytest tests/sql -q`).
Expected: all tests pass. Record the count.

- [ ] **Step 2: Add `Executor` ABC above `PostgresConnection`**

In `wcp_library/sql/postgres.py`, after the existing imports and `_connect_warehouse` / `_async_connect_warehouse` module functions, add:

```python
from abc import ABC, abstractmethod

import numpy as np
from psycopg.sql import SQL, Identifier, Placeholder


class Executor(ABC):
    """Abstract executor for Postgres operations.

    Concrete subclasses implement the five primitives (execute,
    safe_execute, execute_many, execute_multiple, fetch_data).  The
    composite data-manipulation methods (export_df_to_warehouse,
    upsert_df_to_warehouse, truncate_table, empty_table) live here
    and call self.<primitive> — dispatch is correct on both
    AsyncPostgresConnection (per-call pool checkout) and
    AsyncTransaction (single held connection) because both implement
    the same primitive surface.
    """

    @abstractmethod
    async def execute(self, query): ...

    @abstractmethod
    async def safe_execute(self, query, packed_values): ...

    @abstractmethod
    async def execute_many(self, query, dictionary): ...

    @abstractmethod
    async def execute_multiple(self, queries): ...

    @abstractmethod
    async def fetch_data(self, query, packed_data=None): ...

    async def export_df_to_warehouse(
        self, dfObj, outputTableName, columns, remove_nan=False,
    ):
        """Export the DataFrame to the warehouse."""
        col = ", ".join(columns)
        params = ", ".join(f"%({c})s" for c in columns)
        if remove_nan:
            dfObj = dfObj.replace({np.nan: None})
        main_dict = dfObj.to_dict("records")
        for record in main_dict:
            for key in record:
                if record[key] == "":
                    record[key] = None
        query = f"INSERT INTO {outputTableName} ({col}) VALUES ({params})"
        await self.execute_many(query, main_dict)

    async def upsert_df_to_warehouse(
        self, df, table_name, columns, match_cols, remove_nan=False,
    ):
        """Upsert the DataFrame to the warehouse."""
        if not columns:
            raise ValueError("columns cannot be empty")
        if not match_cols:
            raise ValueError("match_cols cannot be empty")
        if not set(match_cols).issubset(set(columns)):
            raise ValueError("match_cols must be a subset of columns")
        if df.empty:
            return 0

        update_cols = [c for c in columns if c not in match_cols]
        col_ids = SQL(", ").join(Identifier(c) for c in columns)
        match_ids = SQL(", ").join(Identifier(c) for c in match_cols)
        placeholders = SQL(", ").join(Placeholder() for _ in columns)
        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)

        if update_cols:
            updates = SQL(", ").join(
                SQL("{} = EXCLUDED.{}").format(Identifier(c), Identifier(c))
                for c in update_cols
            )
            conflict_action = SQL("DO UPDATE SET {}").format(updates)
        else:
            conflict_action = SQL("DO NOTHING")

        query = SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {}"
        ).format(table_id, col_ids, placeholders, match_ids, conflict_action)

        df_copy = df[columns].copy()
        if remove_nan:
            df_copy = df_copy.replace({np.nan: None, pd.NaT: None})
        df_copy = df_copy.replace({"": None})

        records = list(df_copy.itertuples(index=False, name=None))
        await self.execute_many(query, records)
        return len(records)

    async def truncate_table(self, tableName):
        """Truncate the table."""
        await self.execute(f"TRUNCATE TABLE {tableName}")

    async def empty_table(self, tableName):
        """Empty the table."""
        await self.execute(f"DELETE FROM {tableName}")
```

Notes:
- Bodies copy the existing async implementations verbatim. The sync mirror (Task 5) will need a matching sync ABC since async methods can't be inherited by sync code.
- Actually — to avoid doubling the ABC, we'll use a single `Executor` ABC whose abstract methods have async signatures. `PostgresConnection` (sync) will need a DIFFERENT ABC: `SyncExecutor`. Create it in Task 5.
- For now Task 1 only introduces the async `Executor` and migrates the async path. Sync stays on its own methods until Task 5.

Rename the class you just added to `AsyncExecutor` (clearer given the sync twin coming later):

```python
class AsyncExecutor(ABC):
    # ... (as above)
```

- [ ] **Step 3: Make `AsyncPostgresConnection` inherit from `AsyncExecutor`**

Find the existing `class AsyncPostgresConnection(object):` line (around line 433) and change to:

```python
class AsyncPostgresConnection(AsyncExecutor):
```

- [ ] **Step 4: Delete the composite methods from `AsyncPostgresConnection`**

In `wcp_library/sql/postgres.py`, locate the four async composite methods on `AsyncPostgresConnection`:

- `async def export_df_to_warehouse(...)` (~line 643)
- `async def upsert_df_to_warehouse(...)` (~line 672)
- `async def truncate_table(...)` (~line 725)
- `async def empty_table(...)` (~line 737)

Delete all four method definitions (including their `@async_retry` decorators). They're now inherited from `AsyncExecutor`.

Warning: the `@async_retry` decorator was applied to these in the old code. The new `AsyncExecutor.upsert_df_to_warehouse` etc. are NOT retry-decorated, but they CALL `self.execute_many`, which IS retry-decorated on the connection class. So per-call retry of the underlying primitive is preserved. Don't add retry decorators to the `AsyncExecutor` composites — the primitives handle it.

- [ ] **Step 5: Run existing tests**

Run: `pytest tests/sql -q`
Expected: baseline count unchanged. If any test fails, the issue is almost certainly a subtle import or method-resolution-order change — read the traceback before guessing.

- [ ] **Step 6: Add a minimal smoke test for the inheritance**

In `tests/sql/test_postgres.py` (append if exists; create if not):

```python
import pytest

from wcp_library.sql.postgres import AsyncPostgresConnection, AsyncExecutor


class TestAsyncExecutorInheritance:
    def test_connection_is_executor(self):
        assert issubclass(AsyncPostgresConnection, AsyncExecutor)

    def test_connection_has_composites_via_inheritance(self):
        # Attributes exist on the class (not just instances)
        assert hasattr(AsyncPostgresConnection, "upsert_df_to_warehouse")
        assert hasattr(AsyncPostgresConnection, "export_df_to_warehouse")
        assert hasattr(AsyncPostgresConnection, "truncate_table")
        assert hasattr(AsyncPostgresConnection, "empty_table")
```

Run: `pytest tests/sql/test_postgres.py::TestAsyncExecutorInheritance -v`
Expected: both tests pass.

- [ ] **Step 7: Commit**

```bash
git add wcp_library/sql/postgres.py tests/sql/test_postgres.py
git commit -m "$(cat <<'EOF'
refactor: relocate async warehouse composites onto AsyncExecutor ABC

export_df_to_warehouse, upsert_df_to_warehouse, truncate_table,
empty_table move from AsyncPostgresConnection to a new AsyncExecutor
abstract base that also declares the five primitives.  Connection
class now inherits.  No behaviour change; prep for the transaction
context manager that will also inherit from AsyncExecutor.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Add `autocommit` kwarg to `AsyncPostgresConnection`

**Files:**
- Modify: `wcp_library/sql/postgres.py`
- Test: `tests/sql/test_postgres.py`

Add the `autocommit: bool = True` kwarg. When True (default), primitives commit per call as today. When False, primitives do NOT commit — caller drives via new `commit()` / `rollback()` methods. Guard `use_pool=True, autocommit=False` at construction time.

- [ ] **Step 1: Baseline**

Run: `pytest tests/sql -q`
Expected: green (post-Task-1 count).

- [ ] **Step 2: Add `autocommit` kwarg to `__init__`**

In `wcp_library/sql/postgres.py`, find `AsyncPostgresConnection.__init__` (~line 440):

Current signature:
```python
def __init__(self, use_pool: bool = False, min_connections: int = 2, max_connections: int = 5):
```

Change to:
```python
def __init__(
    self,
    use_pool: bool = False,
    min_connections: int = 2,
    max_connections: int = 5,
    autocommit: bool = True,
):
    if use_pool and not autocommit:
        raise ValueError(
            "use_pool=True with autocommit=False is unsupported — "
            "pool-per-call checkout combined with caller-driven commit "
            "leaves held connections dangling.  Use conn.transaction() "
            "for transactional work on a pooled connection."
        )
    self._autocommit = autocommit
    # ... existing body follows
```

Keep all existing assignments.

- [ ] **Step 3: Make primitives conditional on `autocommit`**

In the same file, locate the async primitives (each ~5-15 lines):
- `async def execute(self, query)` (~line 521)
- `async def safe_execute(self, query, packed_values)` (~line 537)
- `async def execute_multiple(self, queries)` (~line 553)
- `async def execute_many(self, query, dictionary)` (~line 575)
- `async def fetch_data(self, query, packed_data=None)` (~line 594)

For each, find the `await connection.commit()` line and gate it:

**Before** (`execute`):
```python
async def execute(self, query):
    connection = await self._get_connection()
    await connection.execute(query)
    await connection.commit()
    if self.use_pool:
        await self._session_pool.putconn(connection)
```

**After**:
```python
async def execute(self, query):
    connection = await self._get_connection()
    await connection.execute(query)
    if self._autocommit:
        await connection.commit()
    if self.use_pool and self._autocommit:
        await self._session_pool.putconn(connection)
```

Same treatment for `safe_execute`, `execute_multiple`, `execute_many`.

For `fetch_data`, the commit isn't currently issued (it's a read), but the pool's `putconn` IS issued at the end. Read the whole function body — if it ends with `putconn`, gate that:

```python
if self.use_pool and self._autocommit:
    await self._session_pool.putconn(connection)
```

(Non-autocommit mode keeps the connection on `self` for the caller's next operation. The caller eventually calls `commit()` / `rollback()` which handles `putconn`.)

- [ ] **Step 4: Add `commit()` and `rollback()` methods**

In `AsyncPostgresConnection`, after the primitive methods, add:

```python
async def commit(self):
    """Commit the current transaction.  Meaningful only when
    autocommit=False; no-op when the connection is None or already
    committed.
    """
    if self._connection is None:
        return
    await self._connection.commit()
    # With use_pool=False this keeps the same connection open; caller
    # can continue issuing statements in a new transaction.

async def rollback(self):
    """Rollback the current transaction.  Meaningful only when
    autocommit=False.
    """
    if self._connection is None:
        return
    await self._connection.rollback()
```

Note: when `use_pool=False, autocommit=False`, `self._connection` holds the persistent connection. When `use_pool=True`, `autocommit=False` is rejected at construction, so `self._connection` isn't the path — the `transaction()` context manager (Task 3) handles it.

- [ ] **Step 5: Write tests**

Append to `tests/sql/test_postgres.py`:

```python
import pytest

from wcp_library.sql.postgres import AsyncPostgresConnection


class TestAsyncAutocommitKwarg:
    def test_default_is_true(self):
        conn = AsyncPostgresConnection()
        assert conn._autocommit is True

    def test_can_be_false(self):
        conn = AsyncPostgresConnection(autocommit=False)
        assert conn._autocommit is False

    def test_pool_plus_no_autocommit_rejected(self):
        with pytest.raises(ValueError, match="use_pool=True with autocommit=False"):
            AsyncPostgresConnection(use_pool=True, autocommit=False)

    def test_pool_plus_default_autocommit_accepted(self):
        conn = AsyncPostgresConnection(use_pool=True)
        assert conn._autocommit is True

    def test_nopool_plus_no_autocommit_accepted(self):
        conn = AsyncPostgresConnection(use_pool=False, autocommit=False)
        assert conn._autocommit is False
        assert conn.use_pool is False
```

Run: `pytest tests/sql/test_postgres.py::TestAsyncAutocommitKwarg -v`
Expected: all 5 pass.

- [ ] **Step 6: Run full suite to confirm no regressions**

Run: `pytest tests/sql -q`
Expected: baseline + 5 new tests.

- [ ] **Step 7: Commit**

```bash
git add wcp_library/sql/postgres.py tests/sql/test_postgres.py
git commit -m "$(cat <<'EOF'
feat: add autocommit kwarg to AsyncPostgresConnection

Default True — existing consumers see no behaviour change.  When
False, primitives skip the per-call commit; caller drives via new
commit() / rollback() methods.  use_pool=True combined with
autocommit=False is rejected with a clear error because pool-per-
call + caller-driven commit is a foot-gun.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Add `AsyncTransaction` class + `transaction()` context manager

**Files:**
- Modify: `wcp_library/sql/postgres.py`
- Test: `tests/sql/test_postgres.py`

Implement `AsyncTransaction` — an `AsyncExecutor` subclass whose primitives use a held connection without per-call commit. Add `conn.transaction()` async context manager that checks out one physical connection, enters a psycopg3 native transaction, yields the `AsyncTransaction`, and commits/rolls back on exit.

- [ ] **Step 1: Baseline**

Run: `pytest tests/sql -q`
Expected: green.

- [ ] **Step 2: Add `AsyncTransaction` class**

In `wcp_library/sql/postgres.py`, after `AsyncExecutor` and before `AsyncPostgresConnection`, add:

```python
class AsyncTransaction(AsyncExecutor):
    """Handle yielded by AsyncPostgresConnection.transaction().

    All primitives run on the held connection with no per-call commit.
    The transaction is committed on normal context exit, rolled back
    on exception.  Manual commit() / rollback() available for early
    termination.
    """

    def __init__(self, parent, connection):
        self._parent = parent
        self._connection = connection
        self._completed_manually = False

    @property
    def connection(self):
        """Underlying psycopg3 AsyncConnection."""
        return self._connection

    async def execute(self, query):
        await self._connection.execute(query)

    async def safe_execute(self, query, packed_values):
        await self._connection.execute(query, packed_values)

    async def execute_many(self, query, dictionary):
        cursor = self._connection.cursor()
        await cursor.executemany(query, dictionary, returning=False)

    async def execute_multiple(self, queries):
        for item in queries:
            query = item[0]
            packed_values = item[1] if len(item) > 1 else None
            if packed_values:
                await self._connection.execute(query, packed_values)
            else:
                await self._connection.execute(query)

    async def fetch_data(self, query, packed_data=None):
        cursor = self._connection.cursor()
        if packed_data:
            await cursor.execute(query, packed_data)
        else:
            await cursor.execute(query)
        return await cursor.fetchall()

    async def commit(self):
        """Commit the transaction early.  Context manager exit becomes
        a no-op."""
        await self._connection.commit()
        self._completed_manually = True

    async def rollback(self):
        """Rollback the transaction early.  Context manager exit
        becomes a no-op."""
        await self._connection.rollback()
        self._completed_manually = True
```

- [ ] **Step 3: Add `transaction()` context manager to `AsyncPostgresConnection`**

In `AsyncPostgresConnection`, add:

```python
from contextlib import asynccontextmanager  # Add to module imports if not present

# ... inside AsyncPostgresConnection class ...

@asynccontextmanager
async def transaction(self):
    """Enter a transactional context on one held physical connection.

    Usage:

        async with conn.transaction() as tx:
            await tx.execute("CREATE TABLE ...")
            await tx.execute("ALTER TABLE ...")
        # Commit on normal exit, rollback on exception.

    The connection's autocommit is temporarily set to False for the
    block and restored on exit.  If use_pool=True, the connection is
    returned to the pool at exit.
    """
    connection = await self._get_connection()
    prior_autocommit = connection.autocommit
    try:
        await connection.set_autocommit(False)
        tx = AsyncTransaction(self, connection)
        try:
            async with connection.transaction():
                yield tx
                if tx._completed_manually:
                    # Caller already committed/rolled back; psycopg's
                    # ctx manager will see no active transaction and
                    # its own commit becomes a no-op.
                    pass
        except Exception:
            # psycopg's transaction() ctx manager already rolled back.
            raise
    finally:
        try:
            await connection.set_autocommit(prior_autocommit)
        finally:
            if self.use_pool:
                await self._session_pool.putconn(connection)
```

- [ ] **Step 4: Write tests — basic commit path**

Append to `tests/sql/test_postgres.py`. These tests require a real Postgres test fixture; follow the library's existing convention (e.g., a `pg_test_conn` fixture). If no integration fixture exists, the library should have one — defer to the library's test helpers.

```python
import pytest

pytestmark = pytest.mark.asyncio  # Or whatever the library uses


class TestAsyncTransactionCommit:
    async def test_commit_on_normal_exit(self, pg_test_conn):
        # pg_test_conn is an AsyncPostgresConnection fixture bound to a
        # test schema / table.  Assume a scratch table `tx_test(id INT, v TEXT)`
        await pg_test_conn.execute("DELETE FROM tx_test")

        async with pg_test_conn.transaction() as tx:
            await tx.execute("INSERT INTO tx_test VALUES (1, 'a')")
            await tx.execute("INSERT INTO tx_test VALUES (2, 'b')")

        rows = await pg_test_conn.fetch_data("SELECT id, v FROM tx_test ORDER BY id")
        assert rows == [(1, "a"), (2, "b")]

    async def test_rollback_on_exception(self, pg_test_conn):
        await pg_test_conn.execute("DELETE FROM tx_test")

        with pytest.raises(RuntimeError):
            async with pg_test_conn.transaction() as tx:
                await tx.execute("INSERT INTO tx_test VALUES (3, 'c')")
                raise RuntimeError("simulated failure")

        rows = await pg_test_conn.fetch_data("SELECT id, v FROM tx_test")
        assert rows == []

    async def test_manual_commit_early(self, pg_test_conn):
        await pg_test_conn.execute("DELETE FROM tx_test")

        async with pg_test_conn.transaction() as tx:
            await tx.execute("INSERT INTO tx_test VALUES (4, 'd')")
            await tx.commit()
            assert tx._completed_manually is True

        rows = await pg_test_conn.fetch_data("SELECT id, v FROM tx_test")
        assert rows == [(4, "d")]

    async def test_manual_rollback_early(self, pg_test_conn):
        await pg_test_conn.execute("DELETE FROM tx_test")

        async with pg_test_conn.transaction() as tx:
            await tx.execute("INSERT INTO tx_test VALUES (5, 'e')")
            await tx.rollback()
            assert tx._completed_manually is True

        rows = await pg_test_conn.fetch_data("SELECT id, v FROM tx_test")
        assert rows == []
```

Run: `pytest tests/sql/test_postgres.py::TestAsyncTransactionCommit -v`
Expected: 4 pass.

If the library doesn't have a `pg_test_conn` fixture, create a minimal one in `tests/sql/conftest.py`:

```python
import os
import pytest
import pytest_asyncio

from wcp_library.sql.postgres import AsyncPostgresConnection


@pytest_asyncio.fixture
async def pg_test_conn():
    conn = AsyncPostgresConnection(use_pool=False)
    await conn.set_user({
        "username": os.environ["PG_TEST_USER"],
        "password": os.environ["PG_TEST_PASSWORD"],
        "hostname": os.environ.get("PG_TEST_HOST", "localhost"),
        "port": int(os.environ.get("PG_TEST_PORT", "5432")),
        "database": os.environ["PG_TEST_DB"],
    })
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS tx_test (id INT PRIMARY KEY, v TEXT)"
    )
    yield conn
    await conn.execute("DROP TABLE IF EXISTS tx_test")
    await conn.close_connection()
```

(Adapt the credential shape to whatever the library's `set_user` expects.)

- [ ] **Step 5: Write tests — composite methods through transaction**

Append to `tests/sql/test_postgres.py`:

```python
import pandas as pd


class TestAsyncTransactionComposites:
    async def test_upsert_through_transaction(self, pg_test_conn):
        await pg_test_conn.execute("DELETE FROM tx_test")

        async with pg_test_conn.transaction() as tx:
            df = pd.DataFrame([{"id": 10, "v": "x"}, {"id": 11, "v": "y"}])
            await tx.upsert_df_to_warehouse(
                df, "tx_test", columns=["id", "v"], match_cols=["id"],
            )

        rows = await pg_test_conn.fetch_data("SELECT id, v FROM tx_test ORDER BY id")
        assert rows == [(10, "x"), (11, "y")]

    async def test_upsert_rollback_on_exception(self, pg_test_conn):
        await pg_test_conn.execute("DELETE FROM tx_test")

        with pytest.raises(RuntimeError):
            async with pg_test_conn.transaction() as tx:
                df = pd.DataFrame([{"id": 20, "v": "z"}])
                await tx.upsert_df_to_warehouse(
                    df, "tx_test", columns=["id", "v"], match_cols=["id"],
                )
                raise RuntimeError("fail after upsert")

        rows = await pg_test_conn.fetch_data("SELECT id, v FROM tx_test")
        assert rows == []

    async def test_truncate_through_transaction(self, pg_test_conn):
        await pg_test_conn.execute("DELETE FROM tx_test")
        await pg_test_conn.execute("INSERT INTO tx_test VALUES (30, 'preexisting')")

        async with pg_test_conn.transaction() as tx:
            await tx.truncate_table("tx_test")

        rows = await pg_test_conn.fetch_data("SELECT id, v FROM tx_test")
        assert rows == []

    async def test_truncate_rollback(self, pg_test_conn):
        await pg_test_conn.execute("DELETE FROM tx_test")
        await pg_test_conn.execute("INSERT INTO tx_test VALUES (40, 'should-survive')")

        with pytest.raises(RuntimeError):
            async with pg_test_conn.transaction() as tx:
                await tx.truncate_table("tx_test")
                raise RuntimeError("fail after truncate")

        rows = await pg_test_conn.fetch_data("SELECT id, v FROM tx_test")
        assert rows == [(40, "should-survive")]
```

Run: `pytest tests/sql/test_postgres.py::TestAsyncTransactionComposites -v`
Expected: 4 pass. Final test is the big one — proves full-load truncate+insert atomicity works.

- [ ] **Step 6: Run full suite**

Run: `pytest tests/sql -q`
Expected: baseline + 8 new tests (4 commit path + 4 composite).

- [ ] **Step 7: Commit**

```bash
git add wcp_library/sql/postgres.py tests/sql/test_postgres.py tests/sql/conftest.py
git commit -m "$(cat <<'EOF'
feat: add AsyncTransaction + AsyncPostgresConnection.transaction()

Context manager checks out one held connection, disables autocommit
for the block, wraps psycopg3's native transaction(), yields an
AsyncTransaction executor.  On normal exit: commit.  On exception:
rollback.  Manual tx.commit() / tx.rollback() available.

Composites (upsert_df_to_warehouse, truncate_table, etc.) work
transactionally via Executor inheritance — rollback verifies no
partial state persists.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Add `retry_transaction` helper

**Files:**
- Modify: `wcp_library/sql/postgres.py`
- Test: `tests/sql/test_postgres.py`

Add `AsyncPostgresConnection.retry_transaction(fn)` that applies the existing retry policy at transaction granularity. On retriable error, roll back and re-enter a fresh transaction.

- [ ] **Step 1: Baseline**

Run: `pytest tests/sql -q`
Expected: green.

- [ ] **Step 2: Add `retry_transaction` method**

In `wcp_library/sql/postgres.py`, inside `AsyncPostgresConnection`, add:

```python
async def retry_transaction(self, fn):
    """Run ``fn(tx)`` inside a transaction with retry at the transaction
    boundary.

    On retriable database errors (psycopg errors matching the codes in
    ``postgres_retry_codes``), rolls back and re-enters a fresh
    transaction up to ``retry_limit`` times.  Non-retriable errors
    propagate without retry.

    :param fn: async callable taking an ``AsyncTransaction`` and returning
        any value
    :return: the return value of the final successful ``fn`` invocation
    :raises: the last retriable error if all attempts fail, or any
        non-retriable error immediately
    """
    import psycopg
    last_exc = None
    for attempt in range(self.retry_limit):
        try:
            async with self.transaction() as tx:
                return await fn(tx)
        except psycopg.Error as exc:
            sqlstate = getattr(exc, "sqlstate", None)
            if sqlstate not in self.retry_error_codes:
                raise
            last_exc = exc
            logger.warning(
                "retry_transaction_retry",
                extra={"attempt": attempt + 1, "max": self.retry_limit,
                       "sqlstate": sqlstate, "error": str(exc)},
            )
    raise last_exc
```

Note: uses the existing `self.retry_limit` (already on the connection class) and `self.retry_error_codes` (already on the connection class). Verify those attributes are set on the async class; if the retry state is only on the sync class today, lift it to a module-level constant or set it in the async `__init__` — follow the library's existing patterns.

- [ ] **Step 3: Write tests**

Append to `tests/sql/test_postgres.py`. Retriable-error simulation is tricky without a real failure — use a mock-style approach:

```python
class TestRetryTransaction:
    async def test_succeeds_first_attempt(self, pg_test_conn):
        await pg_test_conn.execute("DELETE FROM tx_test")
        call_count = 0

        async def do_insert(tx):
            nonlocal call_count
            call_count += 1
            await tx.execute("INSERT INTO tx_test VALUES (50, 'retry-test')")
            return call_count

        result = await pg_test_conn.retry_transaction(do_insert)
        assert result == 1
        assert call_count == 1
        rows = await pg_test_conn.fetch_data("SELECT id FROM tx_test")
        assert rows == [(50,)]

    async def test_non_retriable_error_propagates(self, pg_test_conn):
        await pg_test_conn.execute("DELETE FROM tx_test")
        await pg_test_conn.execute("INSERT INTO tx_test VALUES (60, 'existing')")

        async def do_duplicate_insert(tx):
            # Primary key collision — sqlstate 23505, NOT retriable
            await tx.execute("INSERT INTO tx_test VALUES (60, 'dup')")

        import psycopg
        with pytest.raises(psycopg.errors.UniqueViolation):
            await pg_test_conn.retry_transaction(do_duplicate_insert)

    async def test_retry_on_retriable_error(self, pg_test_conn, monkeypatch):
        # Simulate a retriable error the first attempt, success the second.
        attempts = []

        async def do_insert(tx):
            attempts.append(len(attempts) + 1)
            if len(attempts) == 1:
                import psycopg
                # Construct an error with a retriable sqlstate
                exc = psycopg.OperationalError("simulated connection loss")
                exc.sqlstate = list(pg_test_conn.retry_error_codes)[0]
                raise exc
            await tx.execute("INSERT INTO tx_test VALUES (70, 'retried')")

        await pg_test_conn.execute("DELETE FROM tx_test")
        await pg_test_conn.retry_transaction(do_insert)
        assert len(attempts) == 2
        rows = await pg_test_conn.fetch_data("SELECT id FROM tx_test")
        assert rows == [(70,)]
```

Run: `pytest tests/sql/test_postgres.py::TestRetryTransaction -v`
Expected: 3 pass.

- [ ] **Step 4: Run full suite**

Run: `pytest tests/sql -q`
Expected: baseline + 3 new tests.

- [ ] **Step 5: Commit**

```bash
git add wcp_library/sql/postgres.py tests/sql/test_postgres.py
git commit -m "$(cat <<'EOF'
feat: add AsyncPostgresConnection.retry_transaction(fn)

Wraps fn in a transaction with the library's standard retry policy
applied at the transaction boundary.  Retriable errors (matching
postgres_retry_codes) roll back and re-enter a fresh transaction.
Non-retriable errors propagate immediately.  Use for DDL blocks and
other transactional work that benefits from atomic retry.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Add `Executor` (sync ABC) + relocate sync warehouse composites

**Files:**
- Modify: `wcp_library/sql/postgres.py`
- Test: `tests/sql/test_postgres.py`

Mirror Task 1 for the sync path. Add a `SyncExecutor` ABC with sync-signature primitives and composites. `PostgresConnection` inherits.

- [ ] **Step 1: Baseline**

Run: `pytest tests/sql -q`
Expected: green.

- [ ] **Step 2: Add `SyncExecutor` ABC**

In `wcp_library/sql/postgres.py`, immediately after `AsyncExecutor`, add:

```python
class SyncExecutor(ABC):
    """Sync mirror of AsyncExecutor.  Same surface, synchronous
    signatures.
    """

    @abstractmethod
    def execute(self, query): ...

    @abstractmethod
    def safe_execute(self, query, packed_values): ...

    @abstractmethod
    def execute_many(self, query, dictionary): ...

    @abstractmethod
    def execute_multiple(self, queries): ...

    @abstractmethod
    def fetch_data(self, query, packed_data=None): ...

    def export_df_to_warehouse(self, dfObj, outputTableName, columns, remove_nan=False):
        col = ", ".join(columns)
        params = ", ".join(f"%({c})s" for c in columns)
        if remove_nan:
            dfObj = dfObj.replace({np.nan: None})
        main_dict = dfObj.to_dict("records")
        for record in main_dict:
            for key in record:
                if record[key] == "":
                    record[key] = None
        query = f"INSERT INTO {outputTableName} ({col}) VALUES ({params})"
        self.execute_many(query, main_dict)

    def upsert_df_to_warehouse(self, df, table_name, columns, match_cols, remove_nan=False):
        if not columns:
            raise ValueError("columns cannot be empty")
        if not match_cols:
            raise ValueError("match_cols cannot be empty")
        if not set(match_cols).issubset(set(columns)):
            raise ValueError("match_cols must be a subset of columns")
        if df.empty:
            return 0

        update_cols = [c for c in columns if c not in match_cols]
        col_ids = SQL(", ").join(Identifier(c) for c in columns)
        match_ids = SQL(", ").join(Identifier(c) for c in match_cols)
        placeholders = SQL(", ").join(Placeholder() for _ in columns)
        table_parts = table_name.split(".")
        table_id = Identifier(*table_parts)

        if update_cols:
            updates = SQL(", ").join(
                SQL("{} = EXCLUDED.{}").format(Identifier(c), Identifier(c))
                for c in update_cols
            )
            conflict_action = SQL("DO UPDATE SET {}").format(updates)
        else:
            conflict_action = SQL("DO NOTHING")

        query = SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {}"
        ).format(table_id, col_ids, placeholders, match_ids, conflict_action)

        df_copy = df[columns].copy()
        if remove_nan:
            df_copy = df_copy.replace({np.nan: None, pd.NaT: None})
        df_copy = df_copy.replace({"": None})

        records = list(df_copy.itertuples(index=False, name=None))
        self.execute_many(query, records)
        return len(records)

    def truncate_table(self, tableName):
        self.execute(f"TRUNCATE TABLE {tableName}")

    def empty_table(self, tableName):
        self.execute(f"DELETE FROM {tableName}")
```

- [ ] **Step 3: Make `PostgresConnection` inherit from `SyncExecutor`**

Change `class PostgresConnection(object):` to `class PostgresConnection(SyncExecutor):`.

- [ ] **Step 4: Delete sync composite methods from `PostgresConnection`**

Locate the four sync composite methods on `PostgresConnection`:
- `export_df_to_warehouse` (~line 313)
- `upsert_df_to_warehouse` (~line 342)
- `truncate_table` (~line 395)
- `empty_table` (around ~line 405)

Delete all four method definitions (including any `@retry` decorators).

- [ ] **Step 5: Write inheritance test**

Append to `tests/sql/test_postgres.py`:

```python
from wcp_library.sql.postgres import PostgresConnection, SyncExecutor


class TestSyncExecutorInheritance:
    def test_sync_connection_is_executor(self):
        assert issubclass(PostgresConnection, SyncExecutor)

    def test_sync_connection_has_composites(self):
        assert hasattr(PostgresConnection, "upsert_df_to_warehouse")
        assert hasattr(PostgresConnection, "export_df_to_warehouse")
        assert hasattr(PostgresConnection, "truncate_table")
        assert hasattr(PostgresConnection, "empty_table")
```

Run: `pytest tests/sql/test_postgres.py::TestSyncExecutorInheritance -v`
Expected: 2 pass.

- [ ] **Step 6: Run full suite**

Run: `pytest tests/sql -q`
Expected: baseline + 2.

- [ ] **Step 7: Commit**

```bash
git add wcp_library/sql/postgres.py tests/sql/test_postgres.py
git commit -m "$(cat <<'EOF'
refactor: relocate sync warehouse composites onto SyncExecutor ABC

Mirrors the async path from an earlier commit.  Sync composites move
from PostgresConnection to a new SyncExecutor base.  Connection class
inherits.  No behaviour change.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Add `Transaction` (sync) + `PostgresConnection.transaction()` + `retry_transaction`

**Files:**
- Modify: `wcp_library/sql/postgres.py`
- Test: `tests/sql/test_postgres.py`

Mirror Tasks 2-4 for the sync path.

- [ ] **Step 1: Baseline**

Run: `pytest tests/sql -q`
Expected: green.

- [ ] **Step 2: Add `autocommit` kwarg to `PostgresConnection.__init__`**

Same pattern as Task 2 for the sync `PostgresConnection`:

```python
def __init__(
    self,
    use_pool: bool = False,
    min_connections: int = 2,
    max_connections: int = 5,
    autocommit: bool = True,
):
    if use_pool and not autocommit:
        raise ValueError(
            "use_pool=True with autocommit=False is unsupported — "
            "use conn.transaction() for transactional work on a "
            "pooled connection."
        )
    self._autocommit = autocommit
    # ... existing body
```

Gate each primitive's commit on `self._autocommit`, identical to Task 2 but for sync method bodies.

Add `commit()` and `rollback()` methods (sync signatures).

- [ ] **Step 3: Add `Transaction` class**

In `wcp_library/sql/postgres.py`, mirror `AsyncTransaction`:

```python
from contextlib import contextmanager  # Add to module imports


class Transaction(SyncExecutor):
    """Sync mirror of AsyncTransaction."""

    def __init__(self, parent, connection):
        self._parent = parent
        self._connection = connection
        self._completed_manually = False

    @property
    def connection(self):
        return self._connection

    def execute(self, query):
        self._connection.execute(query)

    def safe_execute(self, query, packed_values):
        self._connection.execute(query, packed_values)

    def execute_many(self, query, dictionary):
        cursor = self._connection.cursor()
        cursor.executemany(query, dictionary, returning=False)

    def execute_multiple(self, queries):
        for item in queries:
            query = item[0]
            packed_values = item[1] if len(item) > 1 else None
            if packed_values:
                self._connection.execute(query, packed_values)
            else:
                self._connection.execute(query)

    def fetch_data(self, query, packed_data=None):
        cursor = self._connection.cursor()
        if packed_data:
            cursor.execute(query, packed_data)
        else:
            cursor.execute(query)
        return cursor.fetchall()

    def commit(self):
        self._connection.commit()
        self._completed_manually = True

    def rollback(self):
        self._connection.rollback()
        self._completed_manually = True
```

- [ ] **Step 4: Add `transaction()` context manager to `PostgresConnection`**

Inside `PostgresConnection`:

```python
@contextmanager
def transaction(self):
    connection = self._get_connection()
    prior_autocommit = connection.autocommit
    try:
        connection.autocommit = False
        tx = Transaction(self, connection)
        try:
            with connection.transaction():
                yield tx
                if tx._completed_manually:
                    pass
        except Exception:
            raise
    finally:
        try:
            connection.autocommit = prior_autocommit
        finally:
            if self.use_pool:
                self._session_pool.putconn(connection)
```

- [ ] **Step 5: Add `retry_transaction` to `PostgresConnection`**

Inside `PostgresConnection`:

```python
def retry_transaction(self, fn):
    import psycopg
    last_exc = None
    for attempt in range(self.retry_limit):
        try:
            with self.transaction() as tx:
                return fn(tx)
        except psycopg.Error as exc:
            sqlstate = getattr(exc, "sqlstate", None)
            if sqlstate not in self.retry_error_codes:
                raise
            last_exc = exc
            logger.warning(
                "retry_transaction_retry",
                extra={"attempt": attempt + 1, "max": self.retry_limit,
                       "sqlstate": sqlstate, "error": str(exc)},
            )
    raise last_exc
```

- [ ] **Step 6: Write tests**

Mirror the async tests for the sync path. Create `pg_test_conn_sync` fixture if needed. Append:

```python
class TestSyncAutocommitKwarg:
    def test_default_is_true(self):
        conn = PostgresConnection()
        assert conn._autocommit is True

    def test_pool_plus_no_autocommit_rejected(self):
        with pytest.raises(ValueError, match="use_pool=True with autocommit=False"):
            PostgresConnection(use_pool=True, autocommit=False)


class TestSyncTransactionCommit:
    def test_commit_on_normal_exit(self, pg_test_conn_sync):
        pg_test_conn_sync.execute("DELETE FROM tx_test")

        with pg_test_conn_sync.transaction() as tx:
            tx.execute("INSERT INTO tx_test VALUES (100, 'sync-a')")

        rows = pg_test_conn_sync.fetch_data("SELECT id FROM tx_test")
        assert rows == [(100,)]

    def test_rollback_on_exception(self, pg_test_conn_sync):
        pg_test_conn_sync.execute("DELETE FROM tx_test")

        with pytest.raises(RuntimeError):
            with pg_test_conn_sync.transaction() as tx:
                tx.execute("INSERT INTO tx_test VALUES (101, 'sync-b')")
                raise RuntimeError("sync fail")

        rows = pg_test_conn_sync.fetch_data("SELECT id FROM tx_test")
        assert rows == []
```

Run: `pytest tests/sql/test_postgres.py -v -k "TestSync"`
Expected: 4 pass.

- [ ] **Step 7: Run full suite**

Run: `pytest tests/sql -q`
Expected: baseline + 4 (sync autocommit + sync transaction).

- [ ] **Step 8: Commit**

```bash
git add wcp_library/sql/postgres.py tests/sql/test_postgres.py tests/sql/conftest.py
git commit -m "$(cat <<'EOF'
feat: sync parity — PostgresConnection autocommit + transaction +
retry_transaction

Mirrors the async additions: autocommit kwarg, commit/rollback
methods, transaction() context manager yielding a Transaction handle
inheriting SyncExecutor, retry_transaction(fn) helper.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Docs + CHANGELOG

**Files:**
- Modify: library's `CHANGELOG.md` or equivalent.
- Modify: library's `README.md` or documentation for usage examples.

- [ ] **Step 1: Add CHANGELOG entry**

Under the library's next-release heading:

```markdown
### Added
- `AsyncPostgresConnection` and `PostgresConnection` now accept
  `autocommit: bool = True` kwarg.  When `False`, primitives no
  longer commit per call; caller drives via new `commit()` /
  `rollback()` methods.  `use_pool=True` combined with
  `autocommit=False` is rejected at construction time.
- `AsyncPostgresConnection.transaction()` / `PostgresConnection.transaction()`
  — async / sync context managers yielding a `Transaction` handle
  that uses a single held connection with psycopg3's native
  transaction semantics.  Commits on normal exit, rolls back on
  exception.  Manual `tx.commit()` / `tx.rollback()` available.
- `retry_transaction(fn)` helper on both connection classes —
  applies the library's retry policy at the granularity of a whole
  transaction.
- `AsyncExecutor` / `SyncExecutor` abstract base classes that
  connection and transaction classes both implement.  Warehouse
  composite methods (`export_df_to_warehouse`, `upsert_df_to_warehouse`,
  `truncate_table`, `empty_table`) live on the base so they
  compose transparently against either context.

### Changed
- Warehouse composite methods moved from the connection classes to
  the new `AsyncExecutor` / `SyncExecutor` bases.  Accessible
  unchanged via inheritance (`conn.upsert_df_to_warehouse(...)` works
  as before).

### Non-breaking
- All existing call sites continue to work unchanged.  The new
  features are opt-in.
```

- [ ] **Step 2: Add a usage example to the README**

In the library's README, add a section documenting the new patterns:

```markdown
## Transactional Postgres work

For multi-statement work that should commit atomically — DDL blocks,
truncate+insert replacements, bulk reconciliations — use the
`transaction()` context manager:

\`\`\`python
async with conn.transaction() as tx:
    await tx.execute("SET LOCAL ROLE some_role")  # session-scoped
    await tx.execute("CREATE TABLE ...")
    await tx.upsert_df_to_warehouse(df, "target", cols, match_cols)
# Commit on normal exit; rollback on exception.
\`\`\`

The transaction handle exposes the same executor surface as a
connection — primitives and warehouse composites both work.

For atomic retry:

\`\`\`python
await conn.retry_transaction(lambda tx: do_block(tx))
\`\`\`
```

- [ ] **Step 3: Run full suite**

Run: `pytest tests/sql -q`
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md README.md
git commit -m "$(cat <<'EOF'
docs: CHANGELOG + README examples for transaction() API

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Final verification

- [ ] **Run the full suite**

Run: `pytest tests/sql -q`
Expected: green. New tests account for roughly:
- Task 1: 2 (inheritance)
- Task 2: 5 (async autocommit)
- Task 3: 8 (async transaction commit + composite)
- Task 4: 3 (async retry_transaction)
- Task 5: 2 (sync inheritance)
- Task 6: 4 (sync autocommit + transaction)

Baseline + ~24 new tests.

- [ ] **Publish** — follow the library's release process (version bump, publish to internal index, tag).

---

## Handoff notes

- Every task after Task 1 depends on the previous tasks' structure. Do not parallelise.
- Integration tests require a local Postgres. If CI doesn't have one, mark the DB-dependent tests with the library's usual `@pytest.mark.integration` or equivalent; they can still run locally.
- The `@async_retry` / `@retry` decorators on connection-class primitives stay in place. Their job is per-call retry on pool checkout. Transaction-class primitives intentionally have NO retry decorator; retry is at transaction boundary via `retry_transaction`.
- `retry_error_codes` and `retry_limit` are library conventions — follow whatever the current codebase uses. The async class may need the same state surfaced as the sync class.
- Keep all classes in `postgres.py` — don't split files unless the library maintainer has a strong preference. Cohesion here is high; separating `Transaction` into a separate file would add indirection for no benefit.
