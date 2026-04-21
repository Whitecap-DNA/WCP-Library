"""Mock tests for Executor ABC inheritance.

These tests avoid any live Postgres interaction -- they only verify
the class hierarchy and the shape of the composite methods as
inherited from AsyncExecutor.
"""
import inspect

import pytest

from wcp_library.sql.postgres import AsyncExecutor, AsyncPostgresConnection


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
