"""
Table classes integration test - MetadataCassandraTable
"""
import asyncio
import os

import pytest
from cassandra.cluster import Session

from cassio.table.cql import STANDARD_ANALYZER
from cassio.table.tables import MetadataCassandraTable
from cassio.table.utils import execute_cql


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestMetadataCassandraTable:
    def test_crud(self, db_session: Session, db_keyspace: str) -> None:
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

    def test_md_routing(self, db_session: Session, db_keyspace: str) -> None:
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
        table_name_none = "m_ct"
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
        assert gotten_none is not None
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
        table_name_allow = "m_ct"
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
        table_name_deny = "m_ct"
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

    def test_metadata_update_sync(self, db_session: Session, db_keyspace: str) -> None:
        """Consistent behaviour when writing new metadata to an existing row."""
        table_name_fad = "m_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_fad};")
        t_fad = MetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_fad,
            primary_key_type="TEXT",
            metadata_indexing=("allow", {"idx", "idx2"}),
        )
        row_id_to_put_args = {
            f"{'I' if has_idx else '_'}{'U' if has_uid else '_'}": {
                "row_id": f"{'I' if has_idx else '_'}{'U' if has_uid else '_'}",
                "body_blob": f"has_idx={has_idx}, has_uid={has_uid}",
                "metadata": {
                    k: v
                    for k, v in {
                        "idx": "I" if has_idx else None,
                        "uid": "U" if has_uid else None,
                    }.items()
                    if v is not None
                },
            }
            for has_idx in [True, False]
            for has_uid in [True, False]
        }

        # check that updates without 'metadata' at all leave the rest unchanged
        t_fad.clear()
        futures = [t_fad.put_async(**pa) for pa in row_id_to_put_args.values()]
        for f in futures:
            _ = f.result()
        for row_id, orig_row in row_id_to_put_args.items():
            retrieved = t_fad.get(row_id=row_id)
            assert retrieved is not None
            assert retrieved["metadata"] == orig_row["metadata"]
        for row_id, orig_row in row_id_to_put_args.items():
            new_bb = f"Updated: {orig_row['body_blob']}"
            t_fad.put(row_id=row_id, body_blob=new_bb)
        for row_id, orig_row in row_id_to_put_args.items():
            retrieved = t_fad.get(row_id=row_id)
            assert retrieved is not None
            assert retrieved["metadata"] == orig_row["metadata"]

        # check before-and-after various types of metadata overwrite
        new_metadatas = [
            {"idx": "newI", "idx2": "newI2", "uid": "newU", "uid2": "newU2"},
            {"idx": "newI", "idx2": "newI2"},
            {"uid": "newU", "uid2": "newU2"},
            {},
        ]
        for new_md in new_metadatas:
            # reset:
            t_fad.clear()
            futures = [t_fad.put_async(**pa) for pa in row_id_to_put_args.values()]
            for f in futures:
                _ = f.result()
            # check 0:
            for row_id, orig_row in row_id_to_put_args.items():
                retrieved = t_fad.get(row_id=row_id)
                assert retrieved is not None
                assert retrieved["metadata"] == orig_row["metadata"]
            # alter metadata:
            for row_id in row_id_to_put_args.keys():
                t_fad.put(row_id=row_id, metadata=new_md)
            # check after the fact
            for row_id, orig_row in row_id_to_put_args.items():
                retrieved = t_fad.get(row_id=row_id)
                assert retrieved is not None
                assert retrieved["metadata"] == new_md

    @pytest.mark.asyncio
    async def test_metadata_update_asyncio(
        self, db_session: Session, db_keyspace: str
    ) -> None:
        """Consistent behaviour when writing new metadata to an existing row."""
        table_name_fad = "m_ct"
        await execute_cql(
            db_session,
            f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_fad};",
        )
        t_fad = MetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_fad,
            primary_key_type="TEXT",
            metadata_indexing=("allow", {"idx", "idx2"}),
            async_setup=True,
        )
        row_id_to_put_args = {
            f"{'I' if has_idx else '_'}{'U' if has_uid else '_'}": {
                "row_id": f"{'I' if has_idx else '_'}{'U' if has_uid else '_'}",
                "body_blob": f"has_idx={has_idx}, has_uid={has_uid}",
                "metadata": {
                    k: v
                    for k, v in {
                        "idx": "I" if has_idx else None,
                        "uid": "U" if has_uid else None,
                    }.items()
                    if v is not None
                },
            }
            for has_idx in [True, False]
            for has_uid in [True, False]
        }

        # check that updates without 'metadata' at all leave the rest unchanged
        await t_fad.aclear()
        tasks = [t_fad.aput(**pa) for pa in row_id_to_put_args.values()]
        await asyncio.gather(*tasks)
        for row_id, orig_row in row_id_to_put_args.items():
            retrieved = await t_fad.aget(row_id=row_id)
            assert retrieved is not None
            assert retrieved["metadata"] == orig_row["metadata"]
        for row_id, orig_row in row_id_to_put_args.items():
            new_bb = f"Updated: {orig_row['body_blob']}"
            await t_fad.aput(row_id=row_id, body_blob=new_bb)
        for row_id, orig_row in row_id_to_put_args.items():
            retrieved = await t_fad.aget(row_id=row_id)
            assert retrieved is not None
            assert retrieved["metadata"] == orig_row["metadata"]

        # check before-and-after various types of metadata overwrite
        new_metadatas = [
            {"idx": "newI", "idx2": "newI2", "uid": "newU", "uid2": "newU2"},
            {"idx": "newI", "idx2": "newI2"},
            {"uid": "newU", "uid2": "newU2"},
            {},
        ]
        for new_md in new_metadatas:
            # reset:
            await t_fad.aclear()
            coros = [t_fad.aput(**pa) for pa in row_id_to_put_args.values()]
            await asyncio.gather(*coros)
            # check 0:
            for row_id, orig_row in row_id_to_put_args.items():
                retrieved = await t_fad.aget(row_id=row_id)
                assert retrieved is not None
                assert retrieved["metadata"] == orig_row["metadata"]
            # alter metadata:
            for row_id in row_id_to_put_args.keys():
                await t_fad.aput(row_id=row_id, metadata=new_md)
            # check after the fact
            for row_id, orig_row in row_id_to_put_args.items():
                retrieved = await t_fad.aget(row_id=row_id)
                assert retrieved is not None
                assert retrieved["metadata"] == new_md

    def test_find_and_delete_entries_sync(
        self, db_session: Session, db_keyspace: str
    ) -> None:
        table_name_fad = "m_ct"
        HALF_N_ROWS = 128
        FAD_MAX_COUNT = 30  # must be < HALF_N_ROWS for full testing
        FAD_BATCH_SIZE = 25  # must be < FAD_MAX_COUNT-1 for full testing
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_fad};")
        t_fad = MetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_fad,
            primary_key_type="TEXT",
            metadata_indexing="all",
        )
        futures = [
            t_fad.put_async(
                row_id=f"r_{row_i}_md_{mdf}",
                body_blob=f"r_{row_i}_md_{mdf}",
                metadata={"field": mdf},
            )
            for row_i in range(HALF_N_ROWS)
            for mdf in ["alpha", "omega"]
        ]
        for f in futures:
            _ = f.result()
        #
        q_md = {"field": "alpha"}

        num_found_items_0 = len(
            list(t_fad.find_entries(n=HALF_N_ROWS + 1, metadata=q_md))
        )
        assert num_found_items_0 == HALF_N_ROWS

        # find_and_delete entries with a primary key specified, matching/nonmatching
        num_deleted1 = t_fad.find_and_delete_entries(
            metadata=q_md, row_id="r_0_md_alpha"
        )
        assert num_deleted1 == 1
        num_deleted0 = t_fad.find_and_delete_entries(
            metadata=q_md, row_id="r_0_md_omega"
        )
        assert num_deleted0 == 0
        num_found_items_a = len(
            list(t_fad.find_entries(n=HALF_N_ROWS + 1, metadata=q_md))
        )
        assert num_found_items_a == HALF_N_ROWS - 1

        # find_and_delete entries with a max count
        num_deleted_m = t_fad.find_and_delete_entries(
            metadata=q_md, batch_size=FAD_BATCH_SIZE, n=FAD_MAX_COUNT
        )
        assert num_deleted_m == FAD_MAX_COUNT
        num_found_items_b = len(
            list(t_fad.find_entries(n=HALF_N_ROWS + 1, metadata=q_md))
        )
        assert num_found_items_b == HALF_N_ROWS - 1 - FAD_MAX_COUNT

        # find_and_delete entries, all remaining items
        num_deleted_c = t_fad.find_and_delete_entries(
            metadata=q_md, batch_size=FAD_BATCH_SIZE
        )
        assert num_deleted_c == HALF_N_ROWS - 1 - FAD_MAX_COUNT
        num_found_items_c = len(
            list(t_fad.find_entries(n=HALF_N_ROWS + 1, metadata=q_md))
        )
        assert num_found_items_c == 0

    @pytest.mark.asyncio
    async def test_find_and_delete_entries_asyncio(
        self, db_session: Session, db_keyspace: str
    ) -> None:
        table_name_fad = "m_ct"
        HALF_N_ROWS = 128
        FAD_MAX_COUNT = 30  # must be < HALF_N_ROWS for full testing
        FAD_BATCH_SIZE = 25  # must be < FAD_MAX_COUNT-1 for full testing
        await execute_cql(
            db_session,
            f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_fad};",
        )
        t_fad = MetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_fad,
            primary_key_type="TEXT",
            metadata_indexing="all",
            async_setup=True,
        )
        coros = [
            t_fad.aput(
                row_id=f"r_{row_i}_md_{mdf}",
                body_blob=f"r_{row_i}_md_{mdf}",
                metadata={"field": mdf},
            )
            for row_i in range(HALF_N_ROWS)
            for mdf in ["alpha", "omega"]
        ]
        await asyncio.gather(*coros)
        #
        q_md = {"field": "alpha"}
        #
        num_found_items_0 = len(
            list(await t_fad.afind_entries(n=HALF_N_ROWS + 1, metadata=q_md))
        )
        assert num_found_items_0 == HALF_N_ROWS

        # find_and_delete entries with a primary key specified, matching/nonmatching
        num_deleted1 = await t_fad.afind_and_delete_entries(
            metadata=q_md, row_id="r_0_md_alpha"
        )
        assert num_deleted1 == 1
        num_deleted0 = await t_fad.afind_and_delete_entries(
            metadata=q_md, row_id="r_0_md_omega"
        )
        assert num_deleted0 == 0
        num_found_items_a = len(
            list(await t_fad.afind_entries(n=HALF_N_ROWS + 1, metadata=q_md))
        )
        assert num_found_items_a == HALF_N_ROWS - 1

        # find_and_delete entries with a max count
        num_deleted_m = await t_fad.afind_and_delete_entries(
            metadata=q_md, batch_size=FAD_BATCH_SIZE, n=FAD_MAX_COUNT
        )
        assert num_deleted_m == FAD_MAX_COUNT
        num_found_items_b = len(
            list(await t_fad.afind_entries(n=HALF_N_ROWS + 1, metadata=q_md))
        )
        assert num_found_items_b == HALF_N_ROWS - 1 - FAD_MAX_COUNT

        # find_and_delete entries, all remaining items
        num_deleted_c = await t_fad.afind_and_delete_entries(
            metadata=q_md, batch_size=FAD_BATCH_SIZE
        )
        assert num_deleted_c == HALF_N_ROWS - 1 - FAD_MAX_COUNT
        num_found_items_c = len(
            list(await t_fad.afind_entries(n=HALF_N_ROWS + 1, metadata=q_md))
        )
        assert num_found_items_c == 0

    @pytest.mark.skipif(
        os.getenv("TEST_DB_MODE", "LOCAL_CASSANDRA") != "ASTRA_DB",
        reason="requires a test Astra DB instance",
    )
    def test_index_analyzers(self, db_session: Session, db_keyspace: str) -> None:
        table_name = "m_ct_analyzers"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = MetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            primary_key_type="TEXT",
            body_index_options=[STANDARD_ANALYZER],
        )
        md = {"a": 1, "b": "Bee", "c": True}
        md_string = {"a": "1.0", "b": "Bee", "c": "true"}
        md_string2 = {"a": "2.0"}
        t.put(row_id="full_row", metadata=md, body_blob="body blob")
        gotten = t.get(body_search="blob", metadata=md_string)
        assert gotten == {
            "row_id": "full_row",
            "body_blob": "body blob",
            "metadata": md_string,
        }
        gotten2 = t.get(body_search="bar", metadata=md_string)
        assert gotten2 is None
        gotten3 = t.get(body_search="blob", metadata=md_string2)
        assert gotten3 is None
