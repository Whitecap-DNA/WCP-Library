"""Mock tests for autocommit kwarg on AsyncPostgresConnection.

No live DB — tests construction behavior and attribute state only.
"""
import pytest

from wcp_library.sql.postgres import AsyncPostgresConnection, PostgresConnection


class TestAsyncAutocommitKwarg:
    def test_default_is_true(self):
        conn = AsyncPostgresConnection()
        assert conn._autocommit is True

    def test_can_be_false_without_pool(self):
        conn = AsyncPostgresConnection(use_pool=False, autocommit=False)
        assert conn._autocommit is False

    def test_pool_plus_no_autocommit_rejected(self):
        with pytest.raises(ValueError, match="use_pool=True with autocommit=False"):
            AsyncPostgresConnection(use_pool=True, autocommit=False)

    def test_pool_plus_default_autocommit_accepted(self):
        conn = AsyncPostgresConnection(use_pool=True)
        assert conn._autocommit is True
        assert conn.use_pool is True

    def test_nopool_plus_default_autocommit_accepted(self):
        conn = AsyncPostgresConnection()
        assert conn._autocommit is True
        assert conn.use_pool is False


class TestAsyncCommitRollbackNoOp:
    """commit() / rollback() should be no-ops before a connection is opened."""

    async def test_commit_noop_when_no_connection(self):
        conn = AsyncPostgresConnection(autocommit=False)
        # _connection hasn't been set (set_user not called) — commit must not raise
        await conn.commit()

    async def test_rollback_noop_when_no_connection(self):
        conn = AsyncPostgresConnection(autocommit=False)
        await conn.rollback()


class TestSyncAutocommitKwarg:
    def test_default_is_true(self):
        conn = PostgresConnection()
        assert conn._autocommit is True

    def test_can_be_false_without_pool(self):
        conn = PostgresConnection(use_pool=False, autocommit=False)
        assert conn._autocommit is False

    def test_pool_plus_no_autocommit_rejected(self):
        with pytest.raises(ValueError, match="use_pool=True with autocommit=False"):
            PostgresConnection(use_pool=True, autocommit=False)

    def test_pool_plus_default_accepted(self):
        conn = PostgresConnection(use_pool=True)
        assert conn._autocommit is True


class TestSyncCommitRollbackNoOp:
    def test_commit_noop_when_no_connection(self):
        conn = PostgresConnection(autocommit=False)
        conn.commit()  # must not raise

    def test_rollback_noop_when_no_connection(self):
        conn = PostgresConnection(autocommit=False)
        conn.rollback()  # must not raise
