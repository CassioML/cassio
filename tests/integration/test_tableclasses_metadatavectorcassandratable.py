"""
Table classes integration test - MetadataVectorCassandraTable
"""
import math
import pytest
from cassandra.cluster import Session  # type: ignore

from cassio.table.tables import (
    MetadataVectorCassandraTable,
)
from cassio.table.utils import call_wrapped_async

N = 16


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestMetadataVectorCassandraTable:
    def test_crud(self, db_session, db_keyspace):
        table_name = "m_v_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = MetadataVectorCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            vector_dimension=2,
            primary_key_type="TEXT",
        )

        for n_theta in range(N):
            theta = n_theta * math.pi * 2 / N
            group = "odd" if n_theta % 2 == 1 else "even"
            t.put(
                row_id=f"theta_{n_theta}",
                body_blob=f"theta = {theta:.4f}",
                vector=[math.cos(theta), math.sin(theta)],
                metadata={
                    group: True,
                    "n_theta_mod_2": n_theta % 2,
                    "group": group,
                },
            )

        # retrieval
        theta_1 = t.get(row_id="theta_1")
        assert theta_1 is not None
        assert abs(theta_1["vector"][0] - math.cos(math.pi * 2 / N)) < 3.0e-8
        assert abs(theta_1["vector"][1] - math.sin(math.pi * 2 / N)) < 3.0e-8

        # retrieval with metadata filtering
        theta_1b = t.get(row_id="theta_1", metadata={"odd": True})
        assert theta_1b == theta_1
        theta_1n = t.get(row_id="theta_1", metadata={"even": True})
        assert theta_1n is None

        # ANN
        # a vector halfway between 0 and 1 inserted above
        query_theta = 1 * math.pi * 2 / (2 * N)
        ref_vector = [math.cos(query_theta), math.sin(query_theta)]
        ann_results1 = list(t.ann_search(ref_vector, n=4))
        assert {r["row_id"] for r in ann_results1[:2]} == {"theta_1", "theta_0"}
        assert {r["row_id"] for r in ann_results1[2:4]} == {"theta_2", "theta_15"}
        # ANN with metadata filtering
        ann_results_md1 = list(t.ann_search(ref_vector, n=4, metadata={"odd": True}))
        assert {r["row_id"] for r in ann_results_md1[:2]} == {"theta_1", "theta_15"}
        assert {r["row_id"] for r in ann_results_md1[2:4]} == {"theta_3", "theta_13"}
        # and in another way...
        ann_results_md2 = list(t.ann_search(ref_vector, n=4, metadata={"group": "odd"}))
        assert {r["row_id"] for r in ann_results_md2[:2]} == {"theta_1", "theta_15"}
        assert {r["row_id"] for r in ann_results_md2[2:4]} == {"theta_3", "theta_13"}
        # with two conditions ...
        ann_results_md3 = list(
            t.ann_search(ref_vector, n=4, metadata={"group": "odd", "odd": True})
        )
        assert {r["row_id"] for r in ann_results_md3[:2]} == {"theta_1", "theta_15"}
        assert {r["row_id"] for r in ann_results_md3[2:4]} == {"theta_3", "theta_13"}

        # metric search testing
        # a vector between 15 and 1 inserted above, but closer to the 1
        query_theta_mt = 1 * math.pi * 2 / (2 * N + 1)
        ref_vector_mt = [math.cos(query_theta_mt), math.sin(query_theta_mt)]
        ann_results_mt = list(
            t.metric_ann_search(
                ref_vector_mt,
                n=2,
                metric="cos",
                metric_threshold=0.8,
                metadata={"group": "odd"},
            )
        )
        assert [r["row_id"] for r in ann_results_mt] == ["theta_1", "theta_15"]
        assert ann_results_mt[0]["distance"] > ann_results_mt[1]["distance"]
        # a max distance makes this call return just two results despite the n=3:
        ann_results_mt_l2 = list(
            t.metric_ann_search(
                ref_vector_mt,
                n=3,
                metric="l2",
                metric_threshold=0.6,
                metadata={"group": "odd"},
            )
        )
        assert [r["row_id"] for r in ann_results_mt_l2] == ["theta_1", "theta_15"]
        assert ann_results_mt_l2[0]["distance"] < ann_results_mt_l2[1]["distance"]
        t.clear()

    @pytest.mark.asyncio
    async def test_crud_asyncio(self, db_session: Session, db_keyspace: str) -> None:
        table_name = "m_v_ct"
        await call_wrapped_async(
            db_session.execute_async,
            f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};",
        )
        #
        t = MetadataVectorCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            vector_dimension=2,
            primary_key_type="TEXT",
        )

        for n_theta in range(N):
            theta = n_theta * math.pi * 2 / N
            group = "odd" if n_theta % 2 == 1 else "even"
            await t.aput(
                row_id=f"theta_{n_theta}",
                body_blob=f"theta = {theta:.4f}",
                vector=[math.cos(theta), math.sin(theta)],
                metadata={
                    group: True,
                    "n_theta_mod_2": n_theta % 2,
                    "group": group,
                },
            )

        # retrieval
        theta_1 = await t.aget(row_id="theta_1")
        assert theta_1 is not None
        assert abs(theta_1["vector"][0] - math.cos(math.pi * 2 / N)) < 3.0e-8
        assert abs(theta_1["vector"][1] - math.sin(math.pi * 2 / N)) < 3.0e-8

        # retrieval with metadata filtering
        theta_1b = await t.aget(row_id="theta_1", metadata={"odd": True})
        assert theta_1b == theta_1
        theta_1n = await t.aget(row_id="theta_1", metadata={"even": True})
        assert theta_1n is None

        # ANN
        # a vector halfway between 0 and 1 inserted above
        query_theta = 1 * math.pi * 2 / (2 * N)
        ref_vector = [math.cos(query_theta), math.sin(query_theta)]
        ann_results1 = list(await t.aann_search(ref_vector, n=4))
        assert {r["row_id"] for r in ann_results1[:2]} == {"theta_1", "theta_0"}
        assert {r["row_id"] for r in ann_results1[2:4]} == {"theta_2", "theta_15"}
        # ANN with metadata filtering
        ann_results_md1 = list(
            await t.aann_search(ref_vector, n=4, metadata={"odd": True})
        )
        assert {r["row_id"] for r in ann_results_md1[:2]} == {"theta_1", "theta_15"}
        assert {r["row_id"] for r in ann_results_md1[2:4]} == {"theta_3", "theta_13"}
        # and in another way...
        ann_results_md2 = list(
            await t.aann_search(ref_vector, n=4, metadata={"group": "odd"})
        )
        assert {r["row_id"] for r in ann_results_md2[:2]} == {"theta_1", "theta_15"}
        assert {r["row_id"] for r in ann_results_md2[2:4]} == {"theta_3", "theta_13"}
        # with two conditions ...
        ann_results_md3 = list(
            await t.aann_search(ref_vector, n=4, metadata={"group": "odd", "odd": True})
        )
        assert {r["row_id"] for r in ann_results_md3[:2]} == {"theta_1", "theta_15"}
        assert {r["row_id"] for r in ann_results_md3[2:4]} == {"theta_3", "theta_13"}

        # metric search testing
        # a vector between 15 and 1 inserted above, but closer to the 1
        query_theta_mt = 1 * math.pi * 2 / (2 * N + 1)
        ref_vector_mt = [math.cos(query_theta_mt), math.sin(query_theta_mt)]
        ann_results_mt = list(
            await t.ametric_ann_search(
                ref_vector_mt,
                n=2,
                metric="cos",
                metric_threshold=0.8,
                metadata={"group": "odd"},
            )
        )
        assert [r["row_id"] for r in ann_results_mt] == ["theta_1", "theta_15"]
        assert ann_results_mt[0]["distance"] > ann_results_mt[1]["distance"]
        # a max distance makes this call return just two results despite the n=3:
        ann_results_mt_l2 = list(
            await t.ametric_ann_search(
                ref_vector_mt,
                n=3,
                metric="l2",
                metric_threshold=0.6,
                metadata={"group": "odd"},
            )
        )
        assert [r["row_id"] for r in ann_results_mt_l2] == ["theta_1", "theta_15"]
        assert ann_results_mt_l2[0]["distance"] < ann_results_mt_l2[1]["distance"]
        await t.aclear()


if __name__ == "__main__":
    # TEST_DB_MODE=LOCAL_CASSANDRA python -m pdb -m  \
    #   tests.integration.test_tableclasses_metadatavectorcassandratable
    from ..conftest import createDBSessionSingleton, getDBKeyspace

    s = createDBSessionSingleton()
    k = getDBKeyspace()
    TestMetadataVectorCassandraTable().test_crud(s, k)
