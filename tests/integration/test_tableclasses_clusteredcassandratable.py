"""
Table classes integration test - ClusteredCassandraTable
"""

import pytest

from cassio.table.tables import (
    ClusteredCassandraTable,
)


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestClusteredCassandraTable:
    def test_crud(self, db_session, db_keyspace):
        table_name = "c_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = ClusteredCassandraTable(
            db_session, db_keyspace, table_name, partition_id="my_part"
        )
        t.put(row_id="reg_row", body_blob="reg_blob")
        gotten1 = t.get(row_id="reg_row")
        assert gotten1 == {
            "row_id": "reg_row",
            "partition_id": "my_part",
            "body_blob": "reg_blob",
        }
        t.put(row_id="irr_row", partition_id="other_p", body_blob="irr_blob")
        gotten2n = t.get(row_id="irr_row")
        assert gotten2n is None
        gotten2 = t.get(row_id="irr_row", partition_id="other_p")
        assert gotten2 == {
            "row_id": "irr_row",
            "partition_id": "other_p",
            "body_blob": "irr_blob",
        }
        #
        t.delete(row_id="reg_row")
        assert t.get(row_id="reg_row") is None
        t.delete(row_id="irr_row")
        assert t.get(row_id="irr_row", partition_id="other_p") is not None
        t.delete(row_id="irr_row", partition_id="other_p")
        assert t.get(row_id="irr_row", partition_id="other_p") is None
        #
        t.put(row_id="nr1")
        t.put(row_id="nr2", partition_id="another_p")
        assert t.get(row_id="nr1") is not None
        assert t.get(row_id="nr2", partition_id="another_p") is not None
        t.delete_partition()
        assert t.get(row_id="nr1") is None
        assert t.get(row_id="nr2", partition_id="another_p") is not None
        t.clear()


if __name__ == "__main__":
    from ..conftest import createDBSessionSingleton, getDBKeyspace

    s = createDBSessionSingleton()
    k = getDBKeyspace()
    TestClusteredCassandraTable().test_crud(s, k)
