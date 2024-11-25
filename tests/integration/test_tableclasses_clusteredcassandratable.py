"""
Table classes integration test - ClusteredCassandraTable
"""

import pytest
from cassandra.cluster import Session

from cassio.table.tables import ClusteredCassandraTable
from cassio.table.utils import execute_cql


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestClusteredCassandraTable:
    def test_crud(self, db_session: Session, db_keyspace: str) -> None:
        table_name = "c_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = ClusteredCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            partition_id="my_part",
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

    def test_partition_ordering(self, db_session: Session, db_keyspace: str) -> None:
        table_name_asc = "c_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_asc};")
        t_asc = ClusteredCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_asc,
            partition_id="my_part",
        )
        t_asc.put(row_id="row1", body_blob="blob1")
        t_asc.put(row_id="row2", body_blob="blob1")
        t_asc.put(row_id="row3", body_blob="blob1")
        part_rows = t_asc.get_partition(n=2)
        assert [gotten["row_id"] for gotten in part_rows] == ["row1", "row2"]
        assert len(list(t_asc.get_partition())) == 3
        assert len(list(t_asc.get_partition(n=10))) == 3
        t_asc.clear()
        #
        table_name_desc = "c_ct_desc"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_desc};")
        t_desc = ClusteredCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_desc,
            partition_id="my_part",
            ordering_in_partition="desc",
        )
        t_desc.put(row_id="row1", body_blob="blob1")
        t_desc.put(row_id="row2", body_blob="blob1")
        t_desc.put(row_id="row3", body_blob="blob1")
        part_rows = t_desc.get_partition(n=2)
        assert [gotten["row_id"] for gotten in part_rows] == ["row3", "row2"]
        assert len(list(t_desc.get_partition())) == 3
        assert len(list(t_desc.get_partition(n=10))) == 3
        #
        t_desc.clear()

    def test_crud_async(self, db_session: Session, db_keyspace: str) -> None:
        table_name = "c_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = ClusteredCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            partition_id="my_part",
        )
        rf1 = t.put_async(row_id="reg_row", body_blob="reg_blob")
        _ = rf1.result()
        gotten1 = t.get(row_id="reg_row")
        assert gotten1 == {
            "row_id": "reg_row",
            "partition_id": "my_part",
            "body_blob": "reg_blob",
        }
        rf2 = t.put_async(
            row_id="irr_row", partition_id="other_p", body_blob="irr_blob"
        )
        _ = rf2.result()
        gotten2n = t.get(row_id="irr_row")
        assert gotten2n is None
        gotten2 = t.get(row_id="irr_row", partition_id="other_p")
        assert gotten2 == {
            "row_id": "irr_row",
            "partition_id": "other_p",
            "body_blob": "irr_blob",
        }
        #
        rf3 = t.delete_async(row_id="reg_row")
        _ = rf3.result()
        assert t.get(row_id="reg_row") is None
        rf4 = t.delete_async(row_id="irr_row")
        _ = rf4.result()
        assert t.get(row_id="irr_row", partition_id="other_p") is not None
        rf5 = t.delete_async(row_id="irr_row", partition_id="other_p")
        _ = rf5.result()
        assert t.get(row_id="irr_row", partition_id="other_p") is None
        #
        rf6 = t.put_async(row_id="nr1")
        _ = rf6.result()
        rf7 = t.put_async(row_id="nr2", partition_id="another_p")
        _ = rf7.result()
        assert t.get(row_id="nr1") is not None
        assert t.get(row_id="nr2", partition_id="another_p") is not None
        rf8 = t.delete_partition_async()
        _ = rf8.result()
        assert t.get(row_id="nr1") is None
        assert t.get(row_id="nr2", partition_id="another_p") is not None
        rf9 = t.clear_async()
        _ = rf9.result()
        assert t.get(row_id="nr2", partition_id="another_p") is None

    @pytest.mark.asyncio
    async def test_crud_asyncio(self, db_session: Session, db_keyspace: str) -> None:
        table_name = "c_ct"
        await execute_cql(
            db_session, f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};"
        )
        #
        t = ClusteredCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            partition_id="my_part",
            async_setup=True,
        )
        await t.aput(row_id="reg_row", body_blob="reg_blob")
        gotten1 = await t.aget(row_id="reg_row")
        assert gotten1 == {
            "row_id": "reg_row",
            "partition_id": "my_part",
            "body_blob": "reg_blob",
        }
        await t.aput(row_id="irr_row", partition_id="other_p", body_blob="irr_blob")
        gotten2n = await t.aget(row_id="irr_row")
        assert gotten2n is None
        gotten2 = await t.aget(row_id="irr_row", partition_id="other_p")
        assert gotten2 == {
            "row_id": "irr_row",
            "partition_id": "other_p",
            "body_blob": "irr_blob",
        }
        #
        await t.adelete(row_id="reg_row")
        assert await t.aget(row_id="reg_row") is None
        await t.adelete(row_id="irr_row")
        assert await t.aget(row_id="irr_row", partition_id="other_p") is not None
        await t.adelete(row_id="irr_row", partition_id="other_p")
        assert await t.aget(row_id="irr_row", partition_id="other_p") is None
        #
        await t.aput(row_id="nr1")
        await t.aput(row_id="nr2", partition_id="another_p")
        assert await t.aget(row_id="nr1") is not None
        assert await t.aget(row_id="nr2", partition_id="another_p") is not None
        await t.adelete_partition()
        assert await t.aget(row_id="nr1") is None
        assert await t.aget(row_id="nr2", partition_id="another_p") is not None
        await t.aclear()
        assert await t.aget(row_id="nr2", partition_id="another_p") is None

    @pytest.mark.asyncio
    async def test_partition_ordering_asyncio(
        self, db_session: Session, db_keyspace: str
    ) -> None:
        table_name_asc = "c_ct"
        await execute_cql(
            db_session,
            f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_asc};",
        )
        t_asc = ClusteredCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_asc,
            partition_id="my_part",
            async_setup=True,
        )
        await t_asc.aput(row_id="row1", body_blob="blob1")
        await t_asc.aput(row_id="row2", body_blob="blob1")
        await t_asc.aput(row_id="row3", body_blob="blob1")
        part_rows = await t_asc.aget_partition(n=2)
        assert [gotten["row_id"] for gotten in part_rows] == ["row1", "row2"]
        assert len(list(await t_asc.aget_partition())) == 3
        assert len(list(await t_asc.aget_partition(n=10))) == 3
        await t_asc.aclear()
        #
        table_name_desc = "c_ct_desc"
        await execute_cql(
            db_session,
            f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_desc};",
        )
        t_desc = ClusteredCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_desc,
            partition_id="my_part",
            ordering_in_partition="desc",
            async_setup=True,
        )
        await t_desc.aput(row_id="row1", body_blob="blob1")
        await t_desc.aput(row_id="row2", body_blob="blob1")
        await t_desc.aput(row_id="row3", body_blob="blob1")
        part_rows = await t_desc.aget_partition(n=2)
        assert [gotten["row_id"] for gotten in part_rows] == ["row3", "row2"]
        assert len(list(await t_desc.aget_partition())) == 3
        assert len(list(await t_desc.aget_partition(n=10))) == 3
        #
        await t_desc.aclear()
