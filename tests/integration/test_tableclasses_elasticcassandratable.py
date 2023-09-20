"""
Table classes integration test - ElasticCassandraTable
"""

import pytest

from cassio.table.tables import (
    ElasticCassandraTable,
)


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestElasticCassandraTable:
    def test_crud(self, db_session, db_keyspace):
        table_name = "e_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = ElasticCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            keys=["k1", "k2"],
            primary_key_type=["INT", "TEXT"],
        )
        t.put(k1=1, k2="one", body_blob="bb_1")
        gotten1 = t.get(k1=1, k2="one")
        assert gotten1 == {"k1": 1, "k2": "one", "body_blob": "bb_1"}
        t.clear()


if __name__ == "__main__":
    # TEST_DB_MODE=LOCAL_CASSANDRA python -m pdb -m  \
    #   tests.integration.test_tableclasses_elasticcassandratable
    from ..conftest import createDBSessionSingleton, getDBKeyspace

    s = createDBSessionSingleton()
    k = getDBKeyspace()
    TestElasticCassandraTable().test_crud(s, k)
