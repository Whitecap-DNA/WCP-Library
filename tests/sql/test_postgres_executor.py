"""Mock tests for Executor ABC inheritance.

These tests avoid any live Postgres interaction -- they only verify
the class hierarchy and the shape of the composite methods as
inherited from AsyncExecutor.
"""
import inspect

import pytest

from wcp_library.sql.postgres import AsyncExecutor, AsyncPostgresConnection
from wcp_library.sql.postgres import PostgresConnection, SyncExecutor


class TestAsyncExecutorInheritance:
    def test_connection_is_executor(self):
        assert issubclass(AsyncPostgresConnection, AsyncExecutor)

    def test_connection_inherits_composites(self):
        for name in (
            "upsert_df_to_warehouse",
            "export_df_to_warehouse",
            "truncate_table",
            "empty_table",
        ):
            assert hasattr(AsyncPostgresConnection, name), name

    def test_composites_are_coroutines(self):
        for name in (
            "upsert_df_to_warehouse",
            "export_df_to_warehouse",
            "truncate_table",
            "empty_table",
        ):
            attr = getattr(AsyncPostgresConnection, name)
            assert inspect.iscoroutinefunction(attr), name

    def test_abstract_primitives_declared(self):
        assert AsyncExecutor.__abstractmethods__ == frozenset({
            "execute",
            "safe_execute",
            "execute_many",
            "execute_multiple",
            "fetch_data",
        })

    def test_cannot_instantiate_abc_directly(self):
        with pytest.raises(TypeError, match="abstract"):
            AsyncExecutor()


class TestSyncExecutorInheritance:
    def test_sync_connection_is_executor(self):
        assert issubclass(PostgresConnection, SyncExecutor)

    def test_sync_connection_inherits_composites(self):
        for name in (
            "upsert_df_to_warehouse",
            "export_df_to_warehouse",
            "truncate_table",
            "empty_table",
        ):
            assert hasattr(PostgresConnection, name), name

    def test_sync_composites_are_sync(self):
        for name in (
            "upsert_df_to_warehouse",
            "export_df_to_warehouse",
            "truncate_table",
            "empty_table",
        ):
            attr = getattr(PostgresConnection, name)
            assert not inspect.iscoroutinefunction(attr), name

    def test_sync_abstract_primitives_declared(self):
        assert SyncExecutor.__abstractmethods__ == frozenset({
            "execute",
            "safe_execute",
            "execute_many",
            "execute_multiple",
            "fetch_data",
        })

    def test_cannot_instantiate_sync_abc_directly(self):
        with pytest.raises(TypeError, match="abstract"):
            SyncExecutor()
