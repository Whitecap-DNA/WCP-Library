# `wcp_library.sql.postgres` — transactional context + executor factoring — design

**Status:** Approved — ready for implementation plan
**Date:** 2026-04-21
**Author:** Mitch Petersen (with Claude)
**Scope:** `wcp_library/sql/postgres.py` (standalone — portable to the library repo)

## Motivation

Both `PostgresConnection` and `AsyncPostgresConnection` currently commit after every
single statement and, when `use_pool=True`, check out and return a connection to the
pool on each call.  This makes several real-world patterns impossible:

- Multi-statement transactions under caller control (BEGIN; stuff; COMMIT with
  conditional logic between statements).
- Session-scoped settings like ``SET ROLE``, ``SET LOCAL`` variables, or prepared
  statements that need to persist across calls.
- Atomic DDL blocks (CREATE + trigger + ALTER as one transaction).
- Atomic truncate-and-insert for full-load replacements.
- Atomic batch deletes where the whole reconciliation must succeed or nothing
  gets deleted.

This spec adds non-autocommit mode and an explicit transaction context manager to
both sync and async connection classes, without breaking any existing consumer.

## Goals

- Add a transaction context manager (`conn.transaction()`) that yields an executor
  bound to one held physical connection.  All statements within the block run on
  that connection; commit on normal exit, rollback on exception.
- Add a `autocommit: bool = True` keyword argument to both connection classes'
  constructors.  Preserves today's behavior as the default.
- Factor the existing data-manipulation methods (`execute_many`,
  `upsert_df_to_warehouse`, `export_df_to_warehouse`, `truncate_table`,
  `empty_table`) onto a shared abstract base (`Executor`) so they compose against
  either a connection or a transaction handle.
- Add a `retry_transaction(fn)` helper that applies the existing retry policy at
  the granularity of whole transactions instead of individual statements.
- Parallel implementation for sync (`PostgresConnection`) and async
  (`AsyncPostgresConnection`) — both get the same surface.
- Zero breaking changes.  Every existing call site continues to work unchanged.

## Non-goals

- Rewriting the pool implementation.  Continue to use
  ``psycopg_pool.AsyncConnectionPool`` / ``psycopg_pool.ConnectionPool``
  unchanged.
- Changing the retry policy itself (which errors are retriable, backoff
  strategy, retry counts).  Same policy, just applied at a different
  granularity when the caller uses ``retry_transaction``.
- Credential management or the ``set_user`` flow.  Those stay as-is.
- Oracle support.  The library has ``wcp_library/sql/oracle.py``; this spec
  touches only Postgres.

## Architecture

### Three-layer factoring

```
Executor (abstract base class)
    ├── execute(query)                  # primitive — abstract
    ├── safe_execute(query, values)     # primitive — abstract
    ├── execute_many(query, records)    # primitive — abstract
    ├── execute_multiple(queries)       # primitive — abstract
    ├── fetch_data(query, packed_data)  # primitive — abstract
    │
    ├── export_df_to_warehouse(...)     # composite — calls self.execute_many
    ├── upsert_df_to_warehouse(...)     # composite — calls self.execute_many
    ├── truncate_table(name)            # composite — calls self.execute
    └── empty_table(name)               # composite — calls self.execute

AsyncPostgresConnection(Executor)
    # Primitives do: checkout → work → commit (if autocommit) → putconn
    # set_user, close_connection, transaction(), retry_transaction()

AsyncTransaction(Executor)
    # Primitives use held connection — NO commit, NO putconn per call
    # commit() / rollback() available for manual early termination

PostgresConnection(Executor)    # sync mirror of AsyncPostgresConnection
Transaction(Executor)           # sync mirror of AsyncTransaction
```

Composite methods — `export_df_to_warehouse`, `upsert_df_to_warehouse`,
`truncate_table`, `empty_table` — live on the `Executor` base as concrete
implementations.  Subclasses only implement the five primitives; composites
inherit for free.

Because composites call `self.execute_many(...)` or `self.execute(...)`, they
dispatch to the right primitive at runtime — a connection's primitives use
pool checkout per call; a transaction's primitives use the held connection.
Same composite code, correct behavior in both contexts.

### `conn.transaction()` lifecycle

Async (sync mirror omitted for brevity):

```python
@asynccontextmanager
async def transaction(self):
    connection = await self._get_connection()
    prior_autocommit = connection.autocommit
    try:
        await connection.set_autocommit(False)
        async with connection.transaction():   # psycopg3 native transaction
            tx = AsyncTransaction(self, connection)
            yield tx
            # psycopg3's context commits here on normal exit, rolls back on
            # exception.  If tx.commit() / tx.rollback() were called manually,
            # tx._completed_manually is True and the commit/rollback below
            # becomes a no-op (psycopg3 detects no active transaction).
    finally:
        try:
            await connection.set_autocommit(prior_autocommit)
        finally:
            if self.use_pool:
                await self._session_pool.putconn(connection)
```

Key properties:
- One physical connection held for the whole block.
- Autocommit toggled for the block, restored on exit.
- psycopg3's native `connection.transaction()` handles BEGIN/COMMIT/ROLLBACK
  and supports nested transactions as SAVEPOINTs.
- Connection returned to pool in its original autocommit state.

### Non-autocommit mode on the connection class

When `AsyncPostgresConnection(use_pool=False, autocommit=False)`:

- Primitives (`execute`, `execute_many`, etc.) do NOT commit after each call.
- `connection.commit()` and `connection.rollback()` are exposed for caller
  control.
- `set_user` still opens the connection.  `close_connection` still closes.
- The `transaction()` context manager still works — it sets autocommit=False
  for the block (already False in this mode, so a no-op) and manages BEGIN
  / COMMIT explicitly.

`use_pool=True, autocommit=False` is rejected at construction with a clear
error.  A pool that hands out connections per call plus caller-driven
commits is a foot-gun — use `transaction()` instead.

### Retry semantics

Existing decorators:
- `@async_retry` on async primitives and composites.
- `@retry` on sync primitives and composites.

Both continue to apply to the connection-class methods (per-call retry with
pool checkout).

`AsyncTransaction` and `Transaction` primitives have NO retry decorator.
Retriable errors propagate out of the `async with conn.transaction():` block;
psycopg3's context manager rolls back; caller sees the original exception.

`conn.retry_transaction(fn)` applies the standard retry policy to the whole
transaction:

```python
async def retry_transaction(self, fn):
    """Run ``fn(tx)`` inside a transaction with retry applied at the
    transaction boundary.  On retriable errors, rolls back, waits per
    the standard backoff, and re-enters a fresh transaction.  Non-
    retriable errors propagate without retry."""
```

The retry policy (which errors count as retriable, wait strategy, attempt
limits) is the same as `@async_retry` — this helper reuses the existing
implementation at a different granularity.

## Components / API surface

### `AsyncPostgresConnection` (async)

New kwarg and methods.  Existing surface unchanged.

```python
class AsyncPostgresConnection(Executor):
    def __init__(
        self,
        use_pool: bool = False,
        min_connections: int = 2,
        max_connections: int = 5,
        autocommit: bool = True,  # NEW
    ) -> None: ...

    # Unchanged
    async def set_user(self, credentials_dict: dict) -> None: ...
    async def close_connection(self) -> None: ...

    # Primitives — retry decorator preserved, commit conditional on autocommit
    async def execute(self, query) -> None: ...
    async def safe_execute(self, query, packed_values) -> None: ...
    async def execute_many(self, query, records) -> None: ...
    async def execute_multiple(self, queries) -> None: ...
    async def fetch_data(self, query, packed_data=None) -> list[tuple]: ...

    # NEW — manual transaction control, only meaningful when autocommit=False
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...

    # NEW — transaction context
    def transaction(self) -> AsyncContextManager[AsyncTransaction]: ...

    # NEW — retry-at-transaction-boundary helper
    async def retry_transaction(
        self, fn: Callable[[AsyncTransaction], Awaitable[T]],
    ) -> T: ...
```

### `AsyncTransaction` (async)

```python
class AsyncTransaction(Executor):
    """Handle yielded by ``AsyncPostgresConnection.transaction()``.

    All primitives use the held connection and do NOT commit per call.
    None of the primitives are retry-decorated — retry belongs at the
    transaction boundary via ``conn.retry_transaction(fn)``.
    """

    @property
    def connection(self):
        """Underlying psycopg3 ``AsyncConnection`` for rare escape-hatch use."""

    async def commit(self) -> None:
        """Commit the transaction early.  Context manager exit becomes a no-op."""

    async def rollback(self) -> None:
        """Rollback the transaction early.  Context manager exit becomes a no-op."""

    # Inherits all Executor primitives + composites
```

### `Executor` (abstract base, shared)

```python
class Executor(ABC):
    """Abstract executor — implemented by connection classes and transaction
    handles.  Composite methods live here; primitives are abstract."""

    @abstractmethod
    async def execute(self, query) -> None: ...

    @abstractmethod
    async def safe_execute(self, query, packed_values) -> None: ...

    @abstractmethod
    async def execute_many(self, query, records) -> None: ...

    @abstractmethod
    async def execute_multiple(self, queries) -> None: ...

    @abstractmethod
    async def fetch_data(self, query, packed_data=None) -> list[tuple]: ...

    # Composite — concrete, call abstract primitives
    async def export_df_to_warehouse(
        self, df, table, columns, remove_nan=False,
    ) -> None:
        # Exactly the existing implementation — just relocated
        col = ", ".join(columns)
        params = ", ".join(f"%({c})s" for c in columns)
        if remove_nan:
            df = df.replace({np.nan: None})
        records = df.to_dict("records")
        for record in records:
            for key in record:
                if record[key] == "":
                    record[key] = None
        query = f"INSERT INTO {table} ({col}) VALUES ({params})"
        await self.execute_many(query, records)

    async def upsert_df_to_warehouse(
        self, df, table, columns, match_cols, remove_nan=False,
    ) -> int:
        # Exactly the existing implementation — just relocated
        ...
        await self.execute_many(query, records)
        return len(records)

    async def truncate_table(self, table_name: str) -> None:
        await self.execute(f"TRUNCATE TABLE {table_name}")

    async def empty_table(self, table_name: str) -> None:
        await self.execute(f"DELETE FROM {table_name}")
```

(Composites' bodies are unchanged relative to today.  Moving them to
`Executor` is pure reorganization.)

### Sync mirrors

`Executor` is the same abstract base for sync (primitives are non-async).
`PostgresConnection` and `Transaction` (sync) mirror their async twins
exactly, with synchronous method signatures.  Psycopg's sync
`connection.transaction()` context manager works identically.

## Usage examples

```python
# Unchanged — today's behavior
conn = AsyncPostgresConnection(use_pool=True)
await conn.set_user(cred)
await conn.execute("INSERT INTO foo VALUES (...)")    # autocommit per call
await conn.upsert_df_to_warehouse(df, "t", cols, match_cols)  # autocommit per call

# Transactional block
async with conn.transaction() as tx:
    await tx.execute("SET LOCAL ROLE some_role")
    await tx.execute("CREATE TABLE t (...)")
    rows = await tx.fetch_data("SELECT ...")
    for alter in build_alters(rows):
        await tx.execute(alter)
# Commit on normal exit.  Role auto-reverts (SET LOCAL scope).
# Connection returned to pool.

# Transactional with retry
await conn.retry_transaction(lambda tx: do_ddl_block(tx))
# Rolls back and re-enters fresh transaction on retriable error.

# Non-autocommit, single-connection, caller-driven commit
conn = AsyncPostgresConnection(use_pool=False, autocommit=False)
await conn.set_user(cred)
try:
    await conn.execute("INSERT INTO foo VALUES (...)")
    await conn.execute("INSERT INTO bar VALUES (...)")
    await conn.commit()
except Exception:
    await conn.rollback()
    raise
```

## Error handling

1. **Retriable errors inside `transaction()`** — propagate out, psycopg3
   rolls back, our `finally` restores autocommit and returns the connection
   to the pool.  If the connection itself is broken, psycopg_pool discards it
   rather than returning it.

2. **Non-retriable errors inside `transaction()`** — same rollback + cleanup
   + propagation.

3. **Manual `tx.commit()` or `tx.rollback()`** — sets the `_completed_manually`
   flag.  psycopg3's `connection.transaction()` context manager sees no
   active transaction at exit and the final commit/rollback is a no-op.

4. **Connection autocommit state** — always restored in `finally`, so even
   if `set_autocommit(prior)` raises, the connection is still released (the
   inner try/finally ensures `putconn` runs).

5. **Pool receives a broken connection** — psycopg_pool handles this: its
   `check` function discards unhealthy connections rather than returning
   them to callers.  No app-level reconnect is needed.  (Existing consumers
   that have their own reconnect logic can keep it, but the pool's health
   checking is the primary mechanism.)

6. **Nested transactions** — psycopg3 implements nested
   `connection.transaction()` as SAVEPOINT.  Our context manager inherits
   this behaviour — `async with conn.transaction() as outer:
   async with conn.transaction() as inner:` works and uses a savepoint for
   the inner block.  Rarely needed, documented as available.

7. **`use_pool=True` combined with `autocommit=False`** — rejected at
   construction time with a clear error.  A pool-per-call + caller-driven
   commit combo leaves held connections dangling across statement
   boundaries; use `transaction()` instead.

## Backward compatibility

- Constructor signatures are expanded with keyword-only defaults.  Positional
  callers unaffected.
- Composite methods move from the connection class to `Executor`, but remain
  accessible as `conn.upsert_df_to_warehouse(...)` via inheritance.  No
  signature or import changes for consumers.
- `autocommit=True` default preserves per-call commit behaviour for every
  existing caller.
- The `@async_retry` / `@retry` wrappers on connection-class primitives are
  preserved.  Callers that depend on retry semantics continue to get them.
- New methods (`commit`, `rollback`, `transaction`, `retry_transaction`) are
  opt-in.

## Testing

Library-side test suite additions (follow the library's existing test
infrastructure and conventions):

### Primitive behaviour

- `autocommit=True` default: `execute` commits implicitly (observable via
  a separate read after simulated reconnect).
- `autocommit=False`: `execute` does NOT commit; `conn.commit()` does.
- Pool behaviour: `use_pool=True, autocommit=True` checks out on each call
  and returns on each call.
- Construction guard: `use_pool=True, autocommit=False` raises a clear
  error.

### Transaction context

- Normal exit commits all statements.
- Exception rolls back all statements.
- Manual `tx.commit()` prevents double-commit.
- Manual `tx.rollback()` prevents double-rollback.
- Autocommit state restored after exit (both normal and exceptional).
- `use_pool=True`: one slot held for block duration, returned on exit.
- Nested `transaction()` produces a SAVEPOINT and behaves correctly on
  nested commit / rollback.

### Executor surface

- A composite method (e.g., `upsert_df_to_warehouse`) works identically on
  `AsyncPostgresConnection` (autocommits per call internally) and on
  `AsyncTransaction` (single commit at block exit).
- Create a table, upsert through a transaction, rollback the transaction,
  re-read — table is unchanged (no partial upsert persisted).

### Retry helper

- `retry_transaction` re-enters a fresh transaction on retriable error.
- Non-retriable errors propagate without retry.
- Retry count respects the library's existing retry_limit.

### Sync mirror

- Mirror each of the above for `PostgresConnection` / `Transaction` (sync).

### Integration fixtures

Follow whatever local-Postgres fixture the library already uses for its
existing integration tests.  If none exist, add a minimal psycopg3-based
fixture (the library already depends on psycopg3, so no new deps).

## Implementation sequence

Suggested order for the plan:

1. **Executor ABC + composite relocation (no behaviour change).**
   Define `Executor` with abstract primitives and concrete composites.
   Move `export_df_to_warehouse`, `upsert_df_to_warehouse`,
   `truncate_table`, `empty_table` from `AsyncPostgresConnection` /
   `PostgresConnection` onto `Executor`.  Connection classes inherit.
   Existing tests all pass — it's a pure relocation.

2. **`autocommit` kwarg on connection classes.**
   Default-True.  Primitives skip the implicit commit when False.  Add
   `connection.commit()` / `connection.rollback()` methods.  Construction
   guard for the `use_pool=True, autocommit=False` combination.

3. **`transaction()` context manager + `Transaction` / `AsyncTransaction`
   classes.**
   Implement as specified above.  Bind to the held connection; inherit
   from `Executor`.

4. **`retry_transaction` helper.**
   Reuse existing retry policy at transaction granularity.

5. **Sync parity.**
   Mirror every step onto the sync `PostgresConnection` / `Transaction`.

6. **Docs.**
   Library CHANGELOG entry, docstrings for the new methods, one
   end-to-end example in the README (or wherever the library
   documents usage).

Each step should keep the existing test suite green.  New tests land in
the corresponding step.

## Open questions

None at spec-writing time.  Any ambiguity that surfaces during plan
authoring or implementation should come back to this spec as an amendment
rather than being resolved ad-hoc.
