"""
Correct renormalization of multicolumn primary key into schema
(by which we mean multiple partition- and/or multiple clustering-key)
"""

from cassio.table.cql import MockDBSession
from cassio.table import ClusteredCassandraTable


class TestMulticolumnPrimaryKey:
    def test_11_pit(self) -> None:
        """
        The numbers in the test function denote the number of
        partition key columns and the number of clustering columns in sequence.

        "pit" means: we use the `partition_id_type` parameter,
        i.e. bypass the `rearrange_pk_type` step.

        So this is a test that everything is like pre-multicolumns
        """

        clu11 = ClusteredCassandraTable(
            "table",
            partition_id_type="PIT",
            session="s",
            keyspace="k",
            skip_provisioning=True,
        )
        sch = clu11._schema()
        assert sch["pk"] == [("partition_id", "PIT")]
        assert sch["cc"] == [("row_id", "TEXT")]

    def test_21_pit(self) -> None:
        """
        A 2-column partition key, bypass `rearrange_pk_type` step
        """

        clu21 = ClusteredCassandraTable(
            "table",
            partition_id_type=["PIT1", "PIT2"],
            session="s",
            keyspace="k",
            skip_provisioning=True,
        )
        sch = clu21._schema()
        assert sch["pk"] == [("partition_id_0", "PIT1"), ("partition_id_1", "PIT2")]
        assert sch["cc"] == [("row_id", "TEXT")]

    def test_12_pkt(self) -> None:
        """
        "pkt" means: use `primary_key_type`, i.e. go through
        the `rearrange_pk_type` step to construct the schema.
        """

        clu12 = ClusteredCassandraTable(
            "table",
            primary_key_type=["COL1", "COL2", "COL3"],
            session="s",
            keyspace="k",
            skip_provisioning=True,
        )
        sch = clu12._schema()
        assert sch["pk"] == [("partition_id", "COL1")]
        assert sch["cc"] == [("row_id_0", "COL2"), ("row_id_1", "COL3")]

    def test_21_pkt(self) -> None:
        """
        The only difference w.r.t test_12_pkt is the `num_partition_keys`
        """
        clu21 = ClusteredCassandraTable(
            "table",
            primary_key_type=["COL1", "COL2", "COL3"],
            num_partition_keys=2,
            session="s",
            keyspace="k",
            skip_provisioning=True,
        )
        sch = clu21._schema()
        assert sch["pk"] == [("partition_id_0", "COL1"), ("partition_id_1", "COL2")]
        assert sch["cc"] == [("row_id", "COL3")]

    def test_22_pkt(self) -> None:
        """
        The only difference w.r.t test_12_pkt is the `num_partition_keys`
        """
        clu21 = ClusteredCassandraTable(
            "table",
            primary_key_type=["COL1", "COL2", "COL3", "COL4"],
            num_partition_keys=2,
            session="s",
            keyspace="k",
            skip_provisioning=True,
        )
        sch = clu21._schema()
        assert sch["pk"] == [("partition_id_0", "COL1"), ("partition_id_1", "COL2")]
        assert sch["cc"] == [("row_id_0", "COL3"), ("row_id_1", "COL4")]

    def test_22_pkt_create_table_cql(self, mock_db_session: MockDBSession) -> None:
        ClusteredCassandraTable(
            "table",
            primary_key_type=["COL1", "COL2", "COL3", "COL4"],
            num_partition_keys=2,
            session=mock_db_session,
            keyspace="k",
        )
        mock_db_session.assert_last_equal(
            [
                (
                    (
                        "CREATE TABLE IF NOT EXISTS k.table ("
                        "partition_id_0 COL1, "
                        "partition_id_1 COL2, row_id_0 COL3, row_id_1 COL4, "
                        "body_blob TEXT, PRIMARY KEY "
                        "((partition_id_0, partition_id_1), row_id_0, row_id_1)"
                        ") WITH CLUSTERING ORDER BY (row_id_0 ASC, row_id_1 ASC);"
                    ),
                    tuple(),
                ),
            ]
        )
