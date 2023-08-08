"""
Table classes integration test - PlainCassandraTable
"""

import pytest

from cassio.table.tables import (
    PlainCassandraTable,
)


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestPlainCassandraTable:
    def test_crud(self, db_session, db_keyspace):
        table_name = "ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = PlainCassandraTable(
            db_session, db_keyspace, table_name, primary_key_type="TEXT"
        )
        t.put(row_id="empty_row")
        gotten1 = t.get(row_id="empty_row")
        assert gotten1 == {"row_id": "empty_row", "body_blob": None}
        t.put(row_id="full_row", body_blob="body_blob")
        gotten2 = t.get(row_id="full_row")
        assert gotten2 == {"row_id": "full_row", "body_blob": "body_blob"}
        t.delete(row_id="full_row")
        gotten2n = t.get(row_id="full_row")
        assert gotten2n is None
        t.clear()
        gotten1n = t.get(row_id="empty_row")
        assert gotten1n is None


if __name__ == "__main__":
    # TEST_DB_MODE=LOCAL_CASSANDRA python -m pdb -m  \
    #   tests.integration.test_tableclasses_plaincassandratable
    from ..conftest import createDBSessionSingleton, getDBKeyspace

    s = createDBSessionSingleton()
    k = getDBKeyspace()
    TestPlainCassandraTable().test_crud(s, k)
