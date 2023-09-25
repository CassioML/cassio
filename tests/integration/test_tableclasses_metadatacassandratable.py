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
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            primary_key_type="TEXT",
        )
        t.put(row_id="row1", body_blob="bb1")
        gotten1 = t.get(row_id="row1")
        assert gotten1 == {"row_id": "row1", "body_blob": "bb1", "metadata": {}}
        gotten1_s = list(t.find_entries(row_id="row1", n=1))[0]
        assert gotten1_s == {"row_id": "row1", "body_blob": "bb1", "metadata": {}}
        t.put(row_id="row2", metadata={})
        gotten2 = t.get(row_id="row2")
        assert gotten2 == {"row_id": "row2", "body_blob": None, "metadata": {}}
        md3 = {"a": 1, "b": "Bee", "c": True}
        md3_string = {"a": "1.0", "b": "Bee", "c": "true"}
        t.put(row_id="row3", metadata=md3)
        gotten3 = t.get(row_id="row3")
        assert gotten3 == {"row_id": "row3", "body_blob": None, "metadata": md3_string}
        md4 = {"c1": True, "c2": True, "c3": True}
        md4_string = {"c1": "true", "c2": "true", "c3": "true"}
        t.put(row_id="row4", metadata=md4)
        gotten4 = t.get(row_id="row4")
        assert gotten4 == {"row_id": "row4", "body_blob": None, "metadata": md4_string}
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
            t.find_entries(metadata={"twin": True}, n=3),
            key=lambda res: int(float(res["metadata"]["index"])),
        )
        expected = [
            {
                "metadata": {"twin": "true", "index": "0.0"},
                "row_id": "twin_a",
                "body_blob": None,
            },
            {
                "metadata": {"twin": "true", "index": "1.0"},
                "row_id": "twin_b",
                "body_blob": None,
            },
        ]
        assert md_twins_gotten == expected
        assert list(t.find_entries(row_id="fake", n=10)) == []
        #
        t.clear()

    def test_md_routing(self, db_session, db_keyspace):
        test_md = {"mds": "string", "mdn": 255, "mdb": True}
        test_md_string = {"mds": "string", "mdn": "255.0", "mdb": "true"}
        #
        table_name_all = "m_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_all};")
        t_all = MetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_all,
            primary_key_type="TEXT",
            metadata_indexing="all",
        )
        t_all.put(row_id="row1", body_blob="bb1", metadata=test_md)
        gotten_all = list(t_all.find_entries(metadata={"mds": "string"}, n=1))[0]
        assert gotten_all["metadata"] == test_md_string
        t_all.clear()
        #
        table_name_none = "m_ct_rtnone"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_none};")
        t_none = MetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_none,
            primary_key_type="TEXT",
            metadata_indexing="none",
        )
        t_none.put(row_id="row1", body_blob="bb1", metadata=test_md)
        with pytest.raises(ValueError):
            # querying on non-indexed metadata fields:
            t_none.find_entries(metadata={"mds": "string"}, n=1)
        gotten_none = t_none.get(row_id="row1")
        assert gotten_none["metadata"] == test_md_string
        t_none.clear()
        #
        test_md_allowdeny = {
            "mdas": "MDAS",
            "mdds": "MDDS",
            "mdan": 255,
            "mddn": 127,
            "mdab": True,
            "mddb": True,
        }
        test_md_allowdeny_string = {
            "mdas": "MDAS",
            "mdds": "MDDS",
            "mdan": "255.0",
            "mddn": "127.0",
            "mdab": "true",
            "mddb": "true",
        }
        #
        table_name_allow = "m_ct_rtallow"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_allow};")
        t_allow = MetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_allow,
            primary_key_type="TEXT",
            metadata_indexing=("allow", {"mdas", "mdan", "mdab"}),
        )
        t_allow.put(row_id="row1", body_blob="bb1", metadata=test_md_allowdeny)
        with pytest.raises(ValueError):
            t_allow.find_entries(metadata={"mdds": "MDDS"}, n=1)
        gotten_allow = list(t_allow.find_entries(metadata={"mdas": "MDAS"}, n=1))[0]
        assert gotten_allow["metadata"] == test_md_allowdeny_string
        t_allow.clear()
        #
        table_name_deny = "m_ct_rtdeny"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_deny};")
        t_deny = MetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_deny,
            primary_key_type="TEXT",
            metadata_indexing=("deny", {"mdds", "mddn", "mddb"}),
        )
        t_deny.put(row_id="row1", body_blob="bb1", metadata=test_md_allowdeny)
        with pytest.raises(ValueError):
            t_deny.find_entries(metadata={"mdds": "MDDS"}, n=1)
        gotten_deny = list(t_deny.find_entries(metadata={"mdas": "MDAS"}, n=1))[0]
        assert gotten_deny["metadata"] == test_md_allowdeny_string
        t_deny.clear()

    def test_find_and_delete_entries(self, db_session, db_keyspace):
        table_name_fad = "m_ct"
        N_ROWS = 640
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_fad};")
        t_fad = MetadataCassandraTable(
            db_session,
            db_keyspace,
            table_name_fad,
            primary_key_type="TEXT",
            metadata_indexing="all",
        )
        futures = [
            t_fad.put_async(
                row_id=f"r_{row_i}_md_{mdf}",
                body_blob=f"r_{row_i}_md_{mdf}",
                metadata={"field": mdf},
            )
            for row_i in range(N_ROWS)
            for mdf in ['alpha', 'omega']
        ]
        for f in futures:
            _ = f.result()
        #
        q_md = {"field": "alpha"}
        #
        num_found_items = len(list(t_fad.find_entries(n=N_ROWS +1, metadata=q_md)))
        assert num_found_items == N_ROWS
        #
        num_deleted = t_fad.find_and_delete_entries(metadata=q_md, batch_size=120)
        num_found_items = len(list(t_fad.find_entries(n=N_ROWS +1, metadata=q_md)))
        assert num_deleted == N_ROWS
        assert num_found_items == 0
        


if __name__ == "__main__":
    # TEST_DB_MODE=LOCAL_CASSANDRA python -m pdb -m  \
    #   tests.integration.test_tableclasses_MetadataCassandraTable
    from ..conftest import createDBSessionSingleton, getDBKeyspace

    s = createDBSessionSingleton()
    k = getDBKeyspace()
    TestMetadataCassandraTable().test_crud(s, k)
    TestMetadataCassandraTable().test_md_routing(s, k)
