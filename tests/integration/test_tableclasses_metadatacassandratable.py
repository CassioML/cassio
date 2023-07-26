"""
Table classes integration test - MetadataCassandraTable
"""

import pytest

from cassio.table.tables import (
    MetadataCassandraTable,
)


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestMetadataCassandraTable:
    def test_crud(self, db_session, db_keyspace):
        table_name = "m_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = MetadataCassandraTable(
            db_session, db_keyspace, table_name, primary_key_type="TEXT"
        )
        t.put(row_id="row1", body_blob="bb1")
        gotten1 = t.get(row_id="row1")
        assert gotten1 == {"row_id": "row1", "body_blob": "bb1", "metadata": {}}
        gotten1_s = list(t.search(row_id="row1", n=1))[0]
        assert gotten1_s == {"row_id": "row1", "body_blob": "bb1", "metadata": {}}
        t.put(row_id="row2", metadata={})
        gotten2 = t.get(row_id="row2")
        assert gotten2 == {"row_id": "row2", "body_blob": None, "metadata": {}}
        md3 = {"a": 1, "b": "Bee", "c": True}
        t.put(row_id="row3", metadata=md3)
        gotten3 = t.get(row_id="row3")
        assert gotten3 == {"row_id": "row3", "body_blob": None, "metadata": md3}
        md4 = {"c1": True, "c2": True, "c3": True}
        t.put(row_id="row4", metadata=md4)
        gotten4 = t.get(row_id="row4")
        assert gotten4 == {"row_id": "row4", "body_blob": None, "metadata": md4}
        # metadata searches:
        md_gotten3a = t.get(metadata={"a": 1})
        assert md_gotten3a == gotten3
        md_gotten3b = t.get(metadata={"b": "Bee", "c": True})
        assert md_gotten3b == gotten3
        md_gotten4a = t.get(metadata={"c1": True, "c3": True})
        assert md_gotten4a == gotten4
        md_gotten4b = t.get(row_id="row4", metadata={"c1": True, "c3": True})
        assert md_gotten4b == gotten4
        # 'search' proper
        t.put(row_id="twin_a", metadata={"twin": True, "index": 0})
        t.put(row_id="twin_b", metadata={"twin": True, "index": 1})
        md_twins_gotten = sorted(
            t.search(metadata={"twin": True}, n=3),
            key=lambda res: res["metadata"]["index"],
        )
        expected = [
            {
                "metadata": {"twin": True, "index": 0.0},
                "row_id": "twin_a",
                "body_blob": None,
            },
            {
                "metadata": {"twin": True, "index": 1.0},
                "row_id": "twin_b",
                "body_blob": None,
            },
        ]
        assert md_twins_gotten == expected
        assert list(t.search(row_id="fake", n=10)) == []
        #
        t.clear()


if __name__ == "__main__":
    # TEST_DB_MODE=LOCAL_CASSANDRA python -m pdb -m  tests.integration.test_tableclasses_MetadataCassandraTable
    from ..conftest import createDBSessionSingleton, getDBKeyspace

    s = createDBSessionSingleton()
    k = getDBKeyspace()
    TestMetadataCassandraTable().test_crud(s, k)
