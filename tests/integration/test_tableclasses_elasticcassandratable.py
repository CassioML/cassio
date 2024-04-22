"""
Table classes integration test - ElasticCassandraTable
"""

import pytest
from cassandra.cluster import Session

from cassio.table.tables import ElasticCassandraTable


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestElasticCassandraTable:
    def test_crud(self, db_session: Session, db_keyspace: str) -> None:
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
