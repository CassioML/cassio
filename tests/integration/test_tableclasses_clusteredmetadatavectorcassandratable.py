"""
Table classes integration test - ClusteredMetadataVectorCassandraTable
"""
import math
import os

import pytest
from cassandra import InvalidRequest
from cassandra.cluster import Session

from cassio.table.query import Predicate, PredicateOperator
from cassio.table.tables import ClusteredMetadataVectorCassandraTable

N = 16


TEST_DB_MODE = os.getenv("TEST_DB_MODE")


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestClusteredMetadataVectorCassandraTable:
    def test_crud(self, db_session: Session, db_keyspace: str) -> None:
        table_name = "c_m_v_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        # "INT" here means: partition_id is a number (for fun)
        t = ClusteredMetadataVectorCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            vector_dimension=2,
            vector_similarity_function="DOT_PRODUCT",
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
        assert theta_1 is not None
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
        assert ztheta_1 is not None
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

        # cross-partition ANN search test
        t_xpart = ClusteredMetadataVectorCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
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

    @pytest.mark.skipif(
        os.getenv("TEST_DB_MODE", "LOCAL_CASSANDRA") != "ASTRA_DB",
        reason="requires a test Astra DB instance",
    )
    def test_vector_source_model_parameter(
        self, db_session: Session, db_keyspace: str
    ) -> None:
        table_name = "c_m_v_ct_sm"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        ClusteredMetadataVectorCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            vector_dimension=2,
            vector_similarity_function="DOT_PRODUCT",
            vector_source_model="bert",
        )

    @pytest.mark.skipif(
        TEST_DB_MODE in {"LOCAL_CASSANDRA", "TESTCONTAINERS_CASSANDRA"},
        reason="fails in Cassandra 5-beta1. To be reactivated once Cassandra is fixed.",
    )
    def test_crud_partitioned_ann(self, db_session: Session, db_keyspace: str) -> None:
        table_name = "c_m_v_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        # "INT" here means: partition_id is a number (for fun)
        t = ClusteredMetadataVectorCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
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

        # ANN on partition 999
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

        t.clear()

    def test_colbertflow_multicolumn(
        self, db_session: Session, db_keyspace: str
    ) -> None:
        """
        A colbert-style full set of write and read patterns.
        Except here the partition key is also multicolumn.

        Partitions ("documents") are here == ("moot", "document_A")
        In a partition, page -> single vector, so that the row_id is:
            ("page0", -1)
            ("page0", 0)
            ...
            ("page1", i)
            ...
        with the -1 being "special" for colBERT purposes.

        The five test entries in the table will have (euclidean!) distances
        between them that enables clear testing: they are arranged vertically
        like this:
                ^       * A.0.1  , positioned at (3, 3)
                |       * A.0.0
                |       * A.1.10
            ----0-------* A.0.-1 ----(x axis)--->
                |       * B.2.100
        and we'll do ANN queries with a query vector "just above A.0.-1"
        """
        table_name = "c_m_v_colbert"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")

        # table creation
        t = ClusteredMetadataVectorCassandraTable(
            session=db_session,
            keyspace=db_keyspace,
            table=table_name,
            vector_dimension=2,
            vector_similarity_function="EUCLIDEAN",
            primary_key_type=["TEXT", "TEXT", "TEXT", "INT"],
            num_partition_keys=2,
            partition_id=("moot", "document_A"),
        )

        # implied-partition inserts
        t.put(
            row_id=("page0", -1),
            vector=[3, 0],
            body_blob="A.0.-1",
            metadata={"bb": "A.0.-1"},
        )
        t.put(
            row_id=("page1", 10),
            vector=[3, 1],
            body_blob="A.1.10",
            metadata={"bb": "A.1.10"},
        )
        t.put(
            row_id=("page0", 0),
            vector=[3, 2],
            body_blob="A.0.0",
            metadata={"bb": "A.0.0"},
        )

        # explicit-partition inserts
        t.put(
            partition_id=("moot", "document_A"),
            row_id=("page0", 1),
            vector=[3, 3],
            body_blob="A.0.1",
            metadata={"bb": "A.0.1"},
        )
        t.put(
            partition_id=("moot", "document_B"),
            row_id=("page2", 100),
            vector=[3, -1],
            body_blob="B.2.100",
            metadata={"bb": "B.2.100"},
        )

        # get with implied partition
        imp_row = t.get(row_id=("page1", 10))
        assert imp_row is not None
        assert imp_row["body_blob"] == "A.1.10"

        # get with explicit partition
        exp_row = t.get(partition_id=("moot", "document_B"), row_id=("page2", 100))
        assert exp_row is not None
        assert exp_row["body_blob"] == "B.2.100"
        assert exp_row["partition_id"] == ("moot", "document_B")
        assert exp_row["row_id"] == ("page2", 100)

        # get_partition, fully unspecified row_id
        full_get_part_bodies = [row["body_blob"] for row in t.get_partition()]
        assert full_get_part_bodies == ["A.0.-1", "A.0.0", "A.0.1", "A.1.10"]

        # get_partition, partial row_id
        partial_get_part_bodies = [
            row["body_blob"] for row in t.get_partition(row_id=("page0",))
        ]
        assert partial_get_part_bodies == ["A.0.-1", "A.0.0", "A.0.1"]

        # get_partition, partial row_id with range
        range_get_part_bodies = [
            row["body_blob"]
            for row in t.get_partition(
                row_id=("page0", Predicate(PredicateOperator.GT, -1))
            )
        ]
        assert range_get_part_bodies == ["A.0.0", "A.0.1"]

        # get_partition, full specification (effectively one-row get)
        fullspec_get_part_bodies = [
            row["body_blob"] for row in t.get_partition(row_id=("page0", 0))
        ]
        assert fullspec_get_part_bodies == ["A.0.0"]

        qvector = [1, 0.02]
        # ANN within a partition (implied)
        ann_rows = list(t.ann_search(vector=qvector, n=3))
        assert [row["body_blob"] for row in ann_rows] == ["A.0.-1", "A.1.10", "A.0.0"]
        assert [row["partition_id"] for row in ann_rows] == [("moot", "document_A")] * 3
        assert [row["row_id"] for row in ann_rows] == [
            ("page0", -1),
            ("page1", 10),
            ("page0", 0),
        ]

        # ANN within a partition (explicit)
        exp_ann_bodies = [
            row["body_blob"]
            for row in t.ann_search(
                vector=qvector, n=3, partition_id=("moot", "document_B")
            )
        ]
        assert exp_ann_bodies == ["B.2.100"]

        # ANN within a partition with partial row_id specification
        # [row['body_blob'] for row in t.ann_search(vector=qvector,n=3,row_id=("page0",))]
        # "ANN ordering by vector requires each restricted column to be indexed except for fully-specified partition keys"

        # ANN within a partition with range on row_id
        # [row['body_blob'] for row in t.ann_search(vector=qvector,n=3,row_id=("page0",Predicate(PredicateOperator.GT,-1)))]
        # same error as above

        # ANN across partitions - explicitly setting partition_id to None:
        full_ann_bodies = [
            row["body_blob"]
            for row in t.ann_search(vector=qvector, partition_id=None, n=3)
        ]
        assert full_ann_bodies == ["A.0.-1", "A.1.10", "B.2.100"]

        # partition deletion
        len_B_pre = len(list(t.get_partition(partition_id=("moot", "document_B"))))
        assert len_B_pre > 0
        t.delete_partition(partition_id=("moot", "document_B"))
        len_B_post = len(list(t.get_partition(partition_id=("moot", "document_B"))))
        assert len_B_post == 0

        # row deletion
        len_A_pre = len(list(t.get_partition(partition_id=("moot", "document_A"))))
        t.delete(partition_id=("moot", "document_A"), row_id=("page1", 10))
        len_A_post = len(list(t.get_partition(partition_id=("moot", "document_A"))))
        assert len_A_post == len_A_pre - 1
