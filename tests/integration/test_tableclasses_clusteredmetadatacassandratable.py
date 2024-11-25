"""
Table classes integration test - ClusteredMetadataCassandraTable
"""

import asyncio

import pytest
from cassandra.cluster import Session

from cassio.table.tables import ClusteredMetadataCassandraTable
from cassio.table.utils import execute_cql


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestClusteredMetadataCassandraTable:
    def test_find_and_delete_entries_sync(
        self, db_session: Session, db_keyspace: str
    ) -> None:
        """
        Plan for the rows in this table:

        primary key         partition key        metadata
        -----------------------------------------------------------
        ("C", "up")         ("dele", 0)          {"field": "alpha"}
        ("C", "up")         ("dele", 1)          {"field": "alpha"}
                                    ...
        ("C", "up")         ("good", 0)          {"field": "omega"}
        ("C", "up")         ("good", 1)          {"field": "omega"}
                                    ...
        ("C", "dn")         ("dele", 0)          {"field": "alpha"}
        ("C", "dn")         ("dele", 1)          {"field": "alpha"}
                                    ...
        ("C", "dn")         ("good", 0)          {"field": "omega"}
        ("C", "dn")         ("good", 1)          {"field": "omega"}
                                    ...

        for a total of 2 x 2 x ONEFOURTH_N_ROWS:
            a 2 due to up/dn in part key
            a 2 due to dele/good in clustering
        The total rows to delete, i.e. those with alpha, are 2 x ONEFOURTH_N_ROWS.
        """
        table_name_fad = "cm_ct"
        ONEFOURTH_N_ROWS = 128
        FAD_MAX_COUNT = 30  # must be < ONEFOURTH_N_ROWS for full testing
        FAD_BATCH_SIZE = 25  # must be < FAD_MAX_COUNT-1 for full testing
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_fad};")
        t_fad = ClusteredMetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_fad,
            primary_key_type=["TEXT", "TEXT", "TEXT", "INT"],
            num_partition_keys=2,
        )
        futures = [
            t_fad.put_async(
                partition_id=("C", part_k),
                row_id=(["good", "dele"][dele_status], row_i),
                body_blob=(
                    f"PART_{part_k} / ROWID_{['good', 'dele'][dele_status]} "
                    f"/ md_{['omega', 'alpha'][dele_status]}"
                ),
                metadata={"field": ["omega", "alpha"][dele_status]},
            )
            for row_i in range(ONEFOURTH_N_ROWS)
            for dele_status in [1, 0]  # 1 means "alpha", i.e. delete-me
            for part_k in ["up", "dn"]
        ]
        for f in futures:
            _ = f.result()
        #
        q_md = {"field": "alpha"}

        total_matching_rows = 2 * ONEFOURTH_N_ROWS

        num_found_items_0a = len(
            list(t_fad.find_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_0a == total_matching_rows

        # find_and_delete calls without match:
        num_deleted0a = t_fad.find_and_delete_entries(
            metadata=q_md,
            partition_id=("X", "up"),
        )
        assert num_deleted0a == 0
        num_deleted0b = t_fad.find_and_delete_entries(
            metadata=q_md,
            partition_id=("C", "up"),
            row_id=("no", -1),
        )
        assert num_deleted0b == 0
        num_deleted0c = t_fad.find_and_delete_entries(
            metadata=q_md,
            partition_id=("C", "up"),
            row_id=("good", 0),
        )
        assert num_deleted0c == 0

        num_found_items_0b = len(
            list(t_fad.find_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_0b == total_matching_rows

        # one-item deletion
        num_deleted1 = t_fad.find_and_delete_entries(
            metadata=q_md,
            partition_id=("C", "up"),
            row_id=("dele", 0),
        )
        assert num_deleted1 == 1
        num_found_items_1 = len(
            list(t_fad.find_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_1 == total_matching_rows - 1

        # deletion of part of a partition
        num_deleted_p = t_fad.find_and_delete_entries(
            metadata=q_md,
            partition_id=("C", "up"),
            n=FAD_MAX_COUNT,
            batch_size=FAD_BATCH_SIZE,
        )
        assert num_deleted_p == FAD_MAX_COUNT
        num_found_items_p = len(
            list(t_fad.find_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_p == total_matching_rows - FAD_MAX_COUNT - 1

        # deletion of the rest of the partition
        num_deleted_p2 = t_fad.find_and_delete_entries(
            metadata=q_md,
            partition_id=("C", "up"),
            batch_size=FAD_BATCH_SIZE,
        )
        assert num_deleted_p2 == ONEFOURTH_N_ROWS - FAD_MAX_COUNT - 1
        num_found_items_p2 = len(
            list(t_fad.find_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_p2 == total_matching_rows - ONEFOURTH_N_ROWS

        # deletion of everything that remains
        num_deleted_a = t_fad.find_and_delete_entries(
            metadata=q_md,
            batch_size=FAD_BATCH_SIZE,
        )
        assert num_deleted_a == ONEFOURTH_N_ROWS
        num_found_items_a = len(
            list(t_fad.find_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_a == 0

    @pytest.mark.asyncio
    async def test_find_and_delete_entries_async(
        self, db_session: Session, db_keyspace: str
    ) -> None:
        """Same logic as for the sync counterpart."""
        table_name_fad = "cm_ct"
        ONEFOURTH_N_ROWS = 128
        FAD_MAX_COUNT = 30  # must be < ONEFOURTH_N_ROWS for full testing
        FAD_BATCH_SIZE = 25  # must be < FAD_MAX_COUNT-1 for full testing
        await execute_cql(
            db_session, f"DROP TABLE IF EXISTS {db_keyspace}.{table_name_fad};"
        )
        t_fad = ClusteredMetadataCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name_fad,
            primary_key_type=["TEXT", "TEXT", "TEXT", "INT"],
            num_partition_keys=2,
            async_setup=True,
        )

        coros = [
            t_fad.aput(
                partition_id=("C", part_k),
                row_id=(["good", "dele"][dele_status], row_i),
                body_blob=(
                    f"PART_{part_k} / ROWID_{['good', 'dele'][dele_status]} "
                    f"/ md_{['omega', 'alpha'][dele_status]}"
                ),
                metadata={"field": ["omega", "alpha"][dele_status]},
            )
            for row_i in range(ONEFOURTH_N_ROWS)
            for dele_status in [1, 0]  # 1 means "alpha", i.e. delete-me
            for part_k in ["up", "dn"]
        ]
        await asyncio.gather(*coros)

        #
        q_md = {"field": "alpha"}

        total_matching_rows = 2 * ONEFOURTH_N_ROWS

        num_found_items_0a = len(
            list(await t_fad.afind_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_0a == total_matching_rows

        # find_and_delete calls without match:
        num_deleted0a = await t_fad.afind_and_delete_entries(
            metadata=q_md,
            partition_id=("X", "up"),
        )
        assert num_deleted0a == 0
        num_deleted0b = await t_fad.afind_and_delete_entries(
            metadata=q_md,
            partition_id=("C", "up"),
            row_id=("no", -1),
        )
        assert num_deleted0b == 0
        num_deleted0c = await t_fad.afind_and_delete_entries(
            metadata=q_md,
            partition_id=("C", "up"),
            row_id=("good", 0),
        )
        assert num_deleted0c == 0

        num_found_items_0b = len(
            list(await t_fad.afind_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_0b == total_matching_rows

        # one-item deletion
        num_deleted1 = await t_fad.afind_and_delete_entries(
            metadata=q_md,
            partition_id=("C", "up"),
            row_id=("dele", 0),
        )
        assert num_deleted1 == 1
        num_found_items_1 = len(
            list(await t_fad.afind_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_1 == total_matching_rows - 1

        # deletion of part of a partition
        num_deleted_p = await t_fad.afind_and_delete_entries(
            metadata=q_md,
            partition_id=("C", "up"),
            n=FAD_MAX_COUNT,
            batch_size=FAD_BATCH_SIZE,
        )
        assert num_deleted_p == FAD_MAX_COUNT
        num_found_items_p = len(
            list(await t_fad.afind_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_p == total_matching_rows - FAD_MAX_COUNT - 1

        # deletion of the rest of the partition
        num_deleted_p2 = await t_fad.afind_and_delete_entries(
            metadata=q_md,
            partition_id=("C", "up"),
            batch_size=FAD_BATCH_SIZE,
        )
        assert num_deleted_p2 == ONEFOURTH_N_ROWS - FAD_MAX_COUNT - 1
        num_found_items_p2 = len(
            list(await t_fad.afind_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_p2 == total_matching_rows - ONEFOURTH_N_ROWS

        # deletion of everything that remains
        num_deleted_a = await t_fad.afind_and_delete_entries(
            metadata=q_md,
            batch_size=FAD_BATCH_SIZE,
        )
        assert num_deleted_a == ONEFOURTH_N_ROWS
        num_found_items_a = len(
            list(await t_fad.afind_entries(n=total_matching_rows + 1, metadata=q_md))
        )
        assert num_found_items_a == 0
