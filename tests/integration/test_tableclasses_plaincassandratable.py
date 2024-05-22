"""
Table classes integration test - PlainCassandraTable
"""
import os

import pytest
from cassandra.cluster import Session

from cassio.table.cql import STANDARD_ANALYZER
from cassio.table.tables import (
    PlainCassandraTable,
)


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestPlainCassandraTable:
    def test_crud(self, db_session: Session, db_keyspace: str) -> None:
        table_name = "ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = PlainCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            primary_key_type="TEXT",
        )
        t.put(row_id="empty_row")
        gotten1 = t.get(row_id="empty_row")
        assert gotten1 == {"row_id": "empty_row", "body_blob": None}
        t.put(row_id="full_row", body_blob="body blob")
        gotten2 = t.get(row_id="full_row")
        assert gotten2 == {"row_id": "full_row", "body_blob": "body blob"}
        t.delete(row_id="full_row")
        gotten2n = t.get(row_id="full_row")
        assert gotten2n is None
        t.clear()
        gotten1n = t.get(row_id="empty_row")
        assert gotten1n is None

        with pytest.raises(ValueError):
            t.get(body_search="body")

    @pytest.mark.skipif(
        os.getenv("TEST_DB_MODE", "LOCAL_CASSANDRA") != "ASTRA_DB",
        reason="requires a test Astra DB instance",
    )
    def test_index_analyzers(self, db_session: Session, db_keyspace: str) -> None:
        table_name = "ct_analyzers"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = PlainCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            primary_key_type="TEXT",
            body_index_options=[STANDARD_ANALYZER],
        )
        t.put(row_id="full_row", body_blob="body blob foo")
        gotten = t.get(body_search="blob")
        assert gotten == {"row_id": "full_row", "body_blob": "body blob foo"}
        gotten2 = t.get(body_search=["blob", "foo"])
        assert gotten2 == {"row_id": "full_row", "body_blob": "body blob foo"}
        gotten3 = t.get(body_search=["blob", "bar"])
        assert gotten3 is None

    def test_body_type(self, db_session: Session, db_keyspace: str) -> None:
        table_name = "ct_body_type"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        t = PlainCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            primary_key_type="TEXT",
            body_type="INT"
        )
        t.put(row_id="row", body_blob=42)
        gotten = t.get(row_id="row")
        assert gotten == {'row_id': 'row', 'body_blob': 42}
