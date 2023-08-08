"""
Table classes integration test - ClusteredMetadataVectorCassandraTable
"""
import math
import pytest

from cassandra import InvalidRequest  # type: ignore

from cassio.table.tables import (
    ClusteredMetadataVectorCassandraTable,
)


N = 16


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestClusteredMetadataVectorCassandraTable:
    def test_crud(self, db_session, db_keyspace):
        table_name = "c_m_v_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        # "INT" here means: partition_id is a number (for fun)
        t = ClusteredMetadataVectorCassandraTable(
            db_session,
            db_keyspace,
            table_name,
            vector_dimension=2,
            primary_key_type=["INT", "TEXT"],
            partition_id=0,
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
        # fill another partition (999 = "the other one")
        for n_theta in range(N):
            theta = n_theta * math.pi * 2 / N
            group = "odd" if n_theta % 2 == 1 else "even"
            t.put(
                row_id=f"Z_theta_{n_theta}",
                body_blob=f"Z_theta = {theta:.4f}",
                vector=[math.cos(theta), math.sin(theta)],
                partition_id=999,
                metadata={
                    group: True,
                    "n_theta_mod_2": n_theta % 2,
                    "group": group,
                },
            )

        # retrieval
        theta_1 = t.get(row_id="theta_1")
        assert abs(theta_1["vector"][0] - math.cos(math.pi * 2 / N)) < 3.0e-8
        assert abs(theta_1["vector"][1] - math.sin(math.pi * 2 / N)) < 3.0e-8
        assert theta_1["partition_id"] == 0

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

        # retrieval on 999
        ztheta_1 = t.get(row_id="Z_theta_1", partition_id=999)
        assert abs(ztheta_1["vector"][0] - math.cos(math.pi * 2 / N)) < 3.0e-8
        assert abs(ztheta_1["vector"][1] - math.sin(math.pi * 2 / N)) < 3.0e-8
        assert ztheta_1["partition_id"] == 999

        # retrieval with metadata filtering on 999
        ztheta_1b = t.get(row_id="Z_theta_1", metadata={"odd": True}, partition_id=999)
        assert ztheta_1b == ztheta_1
        ztheta_1n = t.get(row_id="Z_theta_1", metadata={"even": True}, partition_id=999)
        assert ztheta_1n is None
        # "theta_1" is not an ID on 999:
        ztheta_1n2 = t.get(row_id="theta_1", metadata={"odd": True}, partition_id=999)
        assert ztheta_1n2 is None

        # ANN on 999
        # a vector halfway between 0 and 1 inserted above
        zquery_theta = 1 * math.pi * 2 / (2 * N)
        zref_vector = [math.cos(zquery_theta), math.sin(zquery_theta)]
        zann_results1 = list(t.ann_search(zref_vector, n=4, partition_id=999))
        assert {r["row_id"] for r in zann_results1[:2]} == {"Z_theta_1", "Z_theta_0"}
        assert {r["row_id"] for r in zann_results1[2:4]} == {"Z_theta_2", "Z_theta_15"}
        # ANN with metadata filtering
        zann_results_md1 = list(
            t.ann_search(zref_vector, n=4, metadata={"odd": True}, partition_id=999)
        )
        assert {r["row_id"] for r in zann_results_md1[:2]} == {
            "Z_theta_1",
            "Z_theta_15",
        }
        assert {r["row_id"] for r in zann_results_md1[2:4]} == {
            "Z_theta_3",
            "Z_theta_13",
        }
        # and in another way...
        zann_results_md2 = list(
            t.ann_search(zref_vector, n=4, metadata={"group": "odd"}, partition_id=999)
        )
        assert {r["row_id"] for r in zann_results_md2[:2]} == {
            "Z_theta_1",
            "Z_theta_15",
        }
        assert {r["row_id"] for r in zann_results_md2[2:4]} == {
            "Z_theta_3",
            "Z_theta_13",
        }
        # with two conditions ...
        zann_results_md3 = list(
            t.ann_search(
                zref_vector,
                n=4,
                metadata={"group": "odd", "odd": True},
                partition_id=999,
            )
        )
        assert {r["row_id"] for r in zann_results_md3[:2]} == {
            "Z_theta_1",
            "Z_theta_15",
        }
        assert {r["row_id"] for r in zann_results_md3[2:4]} == {
            "Z_theta_3",
            "Z_theta_13",
        }

        # cross-partition ANN search test
        t_xpart = ClusteredMetadataVectorCassandraTable(
            db_session,
            db_keyspace,
            table_name,
            vector_dimension=2,
            primary_key_type=["INT", "TEXT"],
            skip_provisioning=True,
        )
        # a vector at 1/4 step from the "_0" for both partitions
        xp_query_theta = 1 * math.pi * 2 / (4 * N)
        xp_vector = [math.cos(xp_query_theta), math.sin(xp_query_theta)]
        xpart_results = list(
            t_xpart.ann_search(
                xp_vector,
                n=2,
            )
        )
        assert {r["row_id"] for r in xpart_results} == {"theta_0", "Z_theta_0"}
        # "cross partition GET" (i.e. partition_id not specified).
        # Outside of ANN this should throw an error
        with pytest.raises(InvalidRequest):
            _ = t.get(row_id="not_enough_info", partition_id=None)
        with pytest.raises(InvalidRequest):
            _ = t_xpart.get(row_id="not_enough_info")

        t.clear()


if __name__ == "__main__":
    # TEST_DB_MODE=LOCAL_CASSANDRA python -m pdb -m \
    #   tests.integration.test_tableclasses_clusteredmetadatavectorcassandratable
    from ..conftest import createDBSessionSingleton, getDBKeyspace

    s = createDBSessionSingleton()
    k = getDBKeyspace()
    TestClusteredMetadataVectorCassandraTable().test_crud(s, k)
