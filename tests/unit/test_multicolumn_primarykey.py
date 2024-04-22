"""
Correct renormalization of multicolumn primary key into schema
(by which we mean multiple partition- and/or multiple clustering-key)
"""

from cassio.table import ClusteredCassandraTable, PlainCassandraTable
from cassio.table.cql import MockDBSession
from cassio.table.query import Predicate, PredicateOperator


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

    def test_22_pkt(self, mock_db_session: MockDBSession) -> None:
        """
        Everything is multicolumn here
        """
        clu21 = ClusteredCassandraTable(
            "table",
            primary_key_type=["COL1", "COL2", "COL3", "COL4"],
            num_partition_keys=2,
            session=mock_db_session,
            keyspace="k",
            ordering_in_partition=["ORD3", "ORD4"],
            skip_provisioning=False,
        )
        sch = clu21._schema()
        assert sch["pk"] == [("partition_id_0", "COL1"), ("partition_id_1", "COL2")]
        assert sch["cc"] == [("row_id_0", "COL3"), ("row_id_1", "COL4")]

        mock_db_session.assert_last_equal(
            [
                (
                    (
                        "CREATE TABLE IF NOT EXISTS k.table ( partition_id_0 COL1, partition_id_1 COL2, row_id_0 COL3, row_id_1 COL4, body_blob TEXT, PRIMARY KEY ( ( partition_id_0, partition_id_1 ) , row_id_0, row_id_1 ) ) WITH CLUSTERING ORDER BY ( row_id_0 ORD3, row_id_1 ORD4 )"
                    ),
                    (),
                ),
            ]
        )

        clu21.put(partition_id=("pk0", "pk1"), row_id=("ri0", "ri1"), body_blob="bb")
        mock_db_session.assert_last_equal(
            [
                (
                    (
                        "INSERT INTO k.table (body_blob, row_id_0, row_id_1, partition_id_0, partition_id_1) VALUES (?, ?, ?, ?, ?)  ;"
                    ),
                    ("bb", "ri0", "ri1", "pk0", "pk1"),
                ),
            ]
        )
        clu21.get(partition_id=("pk0", "pk1"), row_id=("ri0", "ri1"), body_blob="bb")
        mock_db_session.assert_last_equal(
            [
                (
                    (
                        "SELECT * FROM k.table WHERE body_blob = ? AND partition_id_0 = ? AND partition_id_1 = ? AND row_id_0 = ? AND row_id_1 = ? ;"
                    ),
                    ("bb", "pk0", "pk1", "ri0", "ri1"),
                ),
            ]
        )

        clu21.get_partition(partition_id=("pk0", "pk1"), row_id=("ri0", "ri1"))
        mock_db_session.assert_last_equal(
            [
                (
                    (
                        "SELECT * FROM k.table WHERE partition_id_0 = ? AND partition_id_1 = ? AND row_id_0 = ? AND row_id_1 = ? ;"
                    ),
                    ("pk0", "pk1", "ri0", "ri1"),
                ),
            ]
        )

        clu21.get_partition(partition_id=("pk0", "pk1"), row_id=tuple())
        clu21.get_partition(partition_id=("pk0", "pk1"))
        mock_db_session.assert_last_equal(
            [
                (
                    (
                        "SELECT * FROM k.table WHERE partition_id_0 = ? AND partition_id_1 = ? ;"
                    ),
                    ("pk0", "pk1"),
                ),
                (
                    (
                        "SELECT * FROM k.table WHERE partition_id_0 = ? AND partition_id_1 = ? ;"
                    ),
                    ("pk0", "pk1"),
                ),
            ]
        )

        # partial clustering supplied
        clu21.get_partition(partition_id=("pk0", "pk1"), row_id=("ri0",))
        mock_db_session.assert_last_equal(
            [
                (
                    (
                        "SELECT * FROM k.table WHERE partition_id_0 = ? AND partition_id_1 = ? AND row_id_0 = ? ;"
                    ),
                    ("pk0", "pk1", "ri0"),
                ),
            ]
        )

        # non-equalities on clustering
        TEST_GTE = Predicate(PredicateOperator.GTE, "x")

        clu21.get_partition(partition_id=("pk0", "pk1"), row_id=(TEST_GTE,))
        clu21.get_partition(partition_id=("pk0", "pk1"), row_id=(TEST_GTE, TEST_GTE))
        clu21.get_partition(partition_id=("pk0", "pk1"), row_id=("ri0", TEST_GTE))
        mock_db_session.assert_last_equal(
            [
                (
                    (
                        "SELECT * FROM k.table WHERE partition_id_0 = ? AND partition_id_1 = ? AND row_id_0 >= ? ;"
                    ),
                    ("pk0", "pk1", "x"),
                ),
                (
                    (
                        "SELECT * FROM k.table WHERE partition_id_0 = ? AND partition_id_1 = ? AND row_id_0 >= ? AND row_id_1 >= ? ;"
                    ),
                    ("pk0", "pk1", "x", "x"),
                ),
                (
                    (
                        "SELECT * FROM k.table WHERE partition_id_0 = ? AND partition_id_1 = ? AND row_id_0 = ? AND row_id_1 >= ? ;"
                    ),
                    ("pk0", "pk1", "ri0", "x"),
                ),
            ]
        )

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

    def test_multirowid_basetable_pit(self, mock_db_session: MockDBSession) -> None:
        """
        Support for multiple row_id in the BaseTable,
        i.e. independent of clustering logic.
        The table is created through `row_id_type` i.e.
        """
        pla21 = PlainCassandraTable(
            session=mock_db_session,
            keyspace="k",
            table="table",
            row_id_type=["R0", "R1"],
        )
        mock_db_session.assert_last_equal(
            [
                (
                    (
                        "CREATE TABLE IF NOT EXISTS k.table (  row_id_0 R0,   row_id_1 R1,   body_blob TEXT, PRIMARY KEY ( ( row_id_0, row_id_1 )   )) ;"
                    ),
                    tuple(),
                ),
            ]
        )

        pla21.put(row_id=("a", "b"), body_blob="x")
        mock_db_session.assert_last_equal(
            [
                (
                    (
                        "INSERT INTO k.table (body_blob, row_id_0, row_id_1) VALUES (?, ?, ?)  ;"
                    ),
                    ("x", "a", "b"),
                ),
            ]
        )

        pla21.delete(row_id=("a", "b"))
        mock_db_session.assert_last_equal(
            [
                (
                    ("DELETE FROM k.table WHERE row_id_0 = ? AND row_id_1 = ?;"),
                    ("a", "b"),
                ),
            ]
        )

        pla21.get(row_id=("a", "b"))
        mock_db_session.assert_last_equal(
            [
                (
                    ("SELECT * FROM k.table WHERE row_id_0 = ? AND row_id_1 = ? ;"),
                    ("a", "b"),
                ),
            ]
        )
