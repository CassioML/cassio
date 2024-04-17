"""
CQL for mixin-based table classes tests
"""
from cassio.table.cql import MockDBSession
from cassio.table.tables import (
    ClusteredElasticMetadataVectorCassandraTable,
    ClusteredMetadataVectorCassandraTable,
    VectorCassandraTable,
)
from cassio.table.range_operator import RangeOperator

# from cassio.table.table_types import Not, SelectColumn


class TestTableClassesCQLGeneration:
    def test_vector_cassandra_table(self, mock_db_session: MockDBSession) -> None:
        vt = VectorCassandraTable(
            session=mock_db_session,
            keyspace="k",
            table="tn",
            vector_dimension=1536,
            vector_source_model="openai-v3-small",
            primary_key_type="UUID",
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "CREATE TABLE IF NOT EXISTS k.tn (  row_id UUID,   body_blob TEXT,   vector VECTOR<FLOAT,1536>, PRIMARY KEY ( ( row_id )   )) ;",  # noqa: E501
                    tuple(),
                ),
                (
                    "CREATE CUSTOM INDEX IF NOT EXISTS idx_vector_tn ON k.tn (vector) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' WITH OPTIONS = { 'source_model': 'openai-v3-small' };",  # noqa: E501
                    tuple(),
                ),
            ]
        )

        vt.delete(row_id="ROWID")
        mock_db_session.assert_last_equal(
            [
                (
                    "DELETE FROM k.tn WHERE row_id = ?;",
                    ("ROWID",),
                ),
            ]
        )

        vt.get(row_id="ROWID")
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE row_id = ? ;",
                    ("ROWID",),
                ),
            ]
        )

        vt.put(row_id="ROWID", body_blob="BODYBLOB", vector="VECTOR")
        mock_db_session.assert_last_equal(
            [
                (
                    "INSERT INTO k.tn (body_blob, vector, row_id) VALUES (?, ?, ?)  ;",
                    (
                        "BODYBLOB",
                        "VECTOR",
                        "ROWID",
                    ),
                ),
            ]
        )

        vt.ann_search([10, 11], 2)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn ORDER BY vector ANN OF ?  LIMIT ?;",
                    (
                        [10, 11],
                        2,
                    ),
                ),
            ]
        )

        vt.clear()
        mock_db_session.assert_last_equal(
            [
                (
                    "TRUNCATE TABLE k.tn;",
                    tuple(),
                ),
            ]
        )

    def test_multi_clustering_column_metadata_vector_cassandra_table(
        self, mock_db_session: MockDBSession
    ) -> None:
        vt_multi_cc = ClusteredMetadataVectorCassandraTable(
            session=mock_db_session,
            keyspace="k",
            table="tn",
            row_id_type=["INT", "INT"],
            vector_dimension=128,
            vector_similarity_function="DOT_PRODUCT",
            vector_source_model="bert",
            comment="Descriptive comment here",
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "CREATE TABLE IF NOT EXISTS k.tn ( partition_id TEXT, row_id_0 INT, row_id_1 INT, body_blob TEXT, vector VECTOR<FLOAT,128>, attributes_blob TEXT, metadata_s MAP<TEXT,TEXT>, PRIMARY KEY ( ( partition_id ) , row_id_0, row_id_1 )) WITH CLUSTERING ORDER BY (row_id_0 ASC, row_id_1 ASC) AND COMMENT = 'Descriptive comment here';",  # noqa: E501
                    tuple(),
                ),
                (
                    "CREATE CUSTOM INDEX IF NOT EXISTS idx_vector_tn ON k.tn (vector) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' WITH OPTIONS = { 'similarity_function': 'dot_product', 'source_model': 'bert' };",  # noqa: E501
                    tuple(),
                ),
                (
                    "CREATE CUSTOM INDEX IF NOT EXISTS eidx_metadata_s_tn ON k.tn (ENTRIES(metadata_s)) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';",  # noqa: E501
                    tuple(),
                ),
            ]
        )

        vt_multi_cc.delete(partition_id="PARTITIONID", row_id=(1, 2))
        mock_db_session.assert_last_equal(
            [
                (
                    "DELETE FROM k.tn WHERE partition_id = ? AND row_id_0 = ? AND row_id_1 = ?;",  # noqa: E501
                    ("PARTITIONID", 1, 2),
                ),
            ]
        )

        vt_multi_cc.get(partition_id="PARTITIONID", row_id=(1, 2))
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE partition_id = ? AND row_id_0 = ? AND row_id_1 = ?;",  # noqa: E501
                    ("PARTITIONID", 1, 2),
                ),
            ]
        )

        # colbert retrieval step 2
        vt_multi_cc.get(partition_id="PARTITIONID", row_id=(1,RangeOperator(RangeOperator.Operator.GT, -1)))
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE partition_id = ? AND row_id_0 = ? AND row_id_1 > ?;",  # noqa: E501
                     ("PARTITIONID", 1, -1),
                ),
            ]
        )

        # # colbert retrieval step 2
        # vt_multi_cc.get(partition_id="PARTITIONID", row_id=(1,Not(-1)), columns=[SelectColumn.PARTITION_ID, SelectColumn.ROW_ID, SelectColumn.VECTOR])
        # mock_db_session.assert_last_equal(
        #     [
        #         (
        #             "SELECT partition_id, row_id_0, row_id_1, vector FROM k.tn WHERE partition_id = ? AND row_id_0 = ? AND row_id_1 = ?;",  # noqa: E501
        #              ("PARTITIONID", 1, -1),
        #         ),
        #     ]
        # )

        # # colbert retrieval step 3
        # vt_multi_cc.get(partition_id="PARTITIONID", row_id=(1,-1), columns=[SelectColumn.BODY, SelectColumn.METADATA])
        # mock_db_session.assert_last_equal(
        #     [
        #         (
        #             "SELECT body_blob, attributes_blob, metadata_s FROM k.tn WHERE partition_id = ? AND row_id_0 = ? AND row_id_1 = ?;",  # noqa: E501
        #              ("PARTITIONID", 1, -1),
        #         ),
        #     ]
        # )

        # colbert delete
        vt_multi_cc.delete_partition(partition_id="PARTITIONID")
        mock_db_session.assert_last_equal(
            [
                (
                    "DELETE FROM k.tn WHERE partition_id = ?;",
                    ("PARTITIONID",),
                ),
            ]
        )

        # insert colbert document without embedding
        vt_multi_cc.put(
            partition_id="PARTITIONID",
            row_id=(1, -1),
            body_blob="BODYBLOB",
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "INSERT INTO k.tn (body_blob, row_id_0, row_id_1, partition_id) VALUES (?, ?, ?, ?);",  # noqa: E501
                    (
                        "BODYBLOB",
                        1,
                        -1,
                        "PARTITIONID",
                    ),
                ),
            ]
        )

        # insert colbert embedding without document
        vt_multi_cc.put(
            partition_id="PARTITIONID",
            row_id=(1, 2),
            vector="VECTOR",
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "INSERT INTO k.tn (vector, row_id_0, row_id_1, partition_id) VALUES (?, ?, ?, ?);",  # noqa: E501
                    (
                        "VECTOR",
                        1,
                        2,
                        "PARTITIONID",
                    ),
                ),
            ]
        )

        md1 = {"num1": 123, "num2": 456, "str1": "STR1", "tru1": True}
        md2 = {"tru1": True, "tru2": True}
        vt_multi_cc.put(
            partition_id="PARTITIONID",
            row_id=(1, 2),
            body_blob="BODYBLOB",
            vector="VECTOR",
            metadata=md1,
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "INSERT INTO k.tn (body_blob, vector, metadata_s, row_id_0, row_id_1, partition_id) VALUES (?, ?, ?, ?, ?, ?);",  # noqa: E501
                    (
                        "BODYBLOB",
                        "VECTOR",
                        {
                            "str1": "STR1",
                            "num1": "123.0",
                            "num2": "456.0",
                            "tru1": "true",
                        },
                        1,
                        2,
                        "PARTITIONID",
                    ),
                ),
            ]
        )

        vt_multi_cc.put(
            partition_id="PARTITIONID",
            row_id=(1, 2),
            body_blob="BODYBLOB",
            vector="VECTOR",
            metadata=md2,
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "INSERT INTO k.tn (body_blob, vector, metadata_s, row_id_0, row_id_1, partition_id) VALUES (?, ?, ?, ?, ?, ?);",  # noqa: E501
                    (
                        "BODYBLOB",
                        "VECTOR",
                        {"tru2": "true", "tru1": "true"},
                        1,
                        2,
                        "PARTITIONID",
                    ),
                ),
            ]
        )

        vt_multi_cc.put(partition_id="PARTITIONID", row_id=(1, 2), metadata=md2)
        mock_db_session.assert_last_equal(
            [
                (
                    "INSERT INTO k.tn (metadata_s, row_id_0, row_id_1, partition_id) VALUES (?, ?, ?, ?);",  # noqa: E501
                    (
                        {"tru2": "true", "tru1": "true"},
                        1,
                        2,
                        "PARTITIONID",
                    ),
                ),
            ]
        )

        vt_multi_cc.get_partition(partition_id="PARTITIONID", n=10)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE partition_id = ? LIMIT ?;",
                    ("PARTITIONID", 10),
                ),
            ]
        )

        vt_multi_cc.get_partition(partition_id="PARTITIONID")
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE partition_id = ? ;",
                    ("PARTITIONID",),
                ),
            ]
        )

        vt_multi_cc.ann_search([10, 11], 2)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
                    ([10, 11], 2),
                ),
            ]
        )

        # # colbert retrieval step 1
        # vt_multi_cc.ann_search([10, 11], 2, columns=[SelectColumn.PARTITION_ID, SelectColumn.ROW_ID])
        # mock_db_session.assert_last_equal(
        #     [
        #         (
        #             "SELECT partition_id, row_id_0, row_id_1 FROM k.tn ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
        #             ([10, 11], 2),
        #         ),
        #     ]
        # )

        vt_multi_cc.ann_search([10, 11], 2, row_id=(1, 2), partition_id="PARTITIONID")
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE partition_id = ? AND row_id_0 = ? AND row_id_1 = ? ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
                    ("PARTITIONID", 1, 2, [10, 11], 2),
                ),
            ]
        )

        search_md = {"mdks": "mdv", "mdkn": 123, "mdke": True}
        vt_multi_cc.get(partition_id="MDPART", row_id=(1, 2), metadata=search_md)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdkn'] = ? AND metadata_s['mdks'] = ? AND partition_id = ? AND row_id_0 = ? AND row_id_1 = ?;",  # noqa: E501
                    ("true", "123.0", "mdv", "MDPART", 1, 2),
                ),
            ]
        )

        vt_multi_cc.ann_search(
            [100, 101], 9, row_id=(1, 2), partition_id="MDPART", metadata=search_md
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdkn'] = ? AND metadata_s['mdks'] = ? AND partition_id = ? AND row_id_0 = ? AND row_id_1 = ? ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
                    (
                        "true",
                        "123.0",
                        "mdv",
                        "MDPART",
                        1,
                        2,
                        [100, 101],
                        9,
                    ),
                ),
            ]
        )

        vt_multi_cc.get_partition(partition_id="MDPART", metadata=search_md)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdkn'] = ? AND metadata_s['mdks'] = ? AND partition_id = ? ;",  # noqa: E501
                    ("true", "123.0", "mdv", "MDPART"),
                ),
            ]
        )

        search_md_part = {"mdke": True, "mdke2": True}
        vt_multi_cc.get(partition_id="MDPART", row_id=(1, 2), metadata=search_md_part)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdke2'] = ? AND partition_id = ? AND row_id_0 = ? AND row_id_1 = ? ;",  # noqa: E501
                    ("true", "true", "MDPART", 1, 2),
                ),
            ]
        )

        vt_multi_cc.ann_search(
            [100, 101],
            9,
            row_id=(1, 2),
            partition_id="MDPART",
            metadata=search_md_part,
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdke2'] = ? AND partition_id = ? AND row_id_0 = ? AND row_id_1 = ? ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
                    (
                        "true",
                        "true",
                        "MDPART",
                        1,
                        2,
                        [100, 101],
                        9,
                    ),
                ),
            ]
        )

        vt_multi_cc.get_partition(partition_id="MDPART", metadata=search_md_part)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdke2'] = ? AND partition_id = ? ;",  # noqa: E501
                    ("true", "true", "MDPART"),
                ),
            ]
        )

        vt_multi_cc.clear()
        mock_db_session.assert_last_equal(
            [
                (
                    "TRUNCATE TABLE k.tn;",
                    tuple(),
                ),
            ]
        )

    def test_clustered_elastic_metadata_vector_cassandra_table(
        self, mock_db_session: MockDBSession
    ) -> None:
        cemvt = ClusteredElasticMetadataVectorCassandraTable(
            session=mock_db_session,
            keyspace="k",
            table="tn",
            keys=["a", "b"],
            vector_dimension=765,
            primary_key_type=["PUUID", "AT", "BT"],
            ttl_seconds=123,
            partition_id="PRE-PART-ID",
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "CREATE TABLE IF NOT EXISTS k.tn (  partition_id PUUID,   key_desc TEXT,   key_vals TEXT,   body_blob TEXT,   vector VECTOR<FLOAT,765>, attributes_blob TEXT,  metadata_s MAP<TEXT,TEXT>, PRIMARY KEY ( ( partition_id ) , key_desc, key_vals )) WITH CLUSTERING ORDER BY (key_desc ASC, key_vals ASC);",  # noqa: E501
                    tuple(),
                ),
                (
                    "CREATE CUSTOM INDEX IF NOT EXISTS idx_vector_tn ON k.tn (vector) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';",  # noqa: E501
                    tuple(),
                ),
                (
                    "CREATE CUSTOM INDEX IF NOT EXISTS eidx_metadata_s_tn ON k.tn (ENTRIES(metadata_s)) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';",  # noqa: E501
                    tuple(),
                ),
            ]
        )

        cemvt.delete(partition_id="PARTITIONID", a="A", b="B")
        mock_db_session.assert_last_equal(
            [
                (
                    "DELETE FROM k.tn WHERE key_desc = ? AND key_vals = ? AND partition_id = ?;",  # noqa: E501
                    ('["a","b"]', '["A","B"]', "PARTITIONID"),
                ),
            ]
        )

        cemvt.get(partition_id="PARTITIONID", a="A", b="B")
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE key_desc = ? AND key_vals = ? AND partition_id = ? ;",  # noqa: E501
                    ('["a","b"]', '["A","B"]', "PARTITIONID"),
                ),
            ]
        )

        cemvt.delete_partition(partition_id="PARTITIONID")
        mock_db_session.assert_last_equal(
            [
                (
                    "DELETE FROM k.tn WHERE partition_id = ?;",
                    ("PARTITIONID",),
                ),
            ]
        )

        cemvt.put(
            partition_id="PARTITIONID",
            a="A",
            b="B",
            body_blob="BODYBLOB",
            vector="VECTOR",
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "INSERT INTO k.tn (body_blob, vector, key_desc, key_vals, partition_id) VALUES (?, ?, ?, ?, ?) USING TTL ? ;",  # noqa: E501
                    (
                        "BODYBLOB",
                        "VECTOR",
                        '["a","b"]',
                        '["A","B"]',
                        "PARTITIONID",
                        123,
                    ),
                ),
            ]
        )

        md1 = {"num1": 123, "num2": 456, "str1": "STR1", "tru1": True}
        md2 = {"tru1": True, "tru2": True}
        cemvt.put(
            partition_id="PARTITIONID",
            a="A",
            b="B",
            body_blob="BODYBLOB",
            vector="VECTOR",
            metadata=md1,
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "INSERT INTO k.tn (body_blob, vector, metadata_s, key_desc, key_vals, partition_id) VALUES (?, ?, ?, ?, ?, ?) USING TTL ? ;",  # noqa: E501
                    (
                        "BODYBLOB",
                        "VECTOR",
                        {
                            "str1": "STR1",
                            "num1": "123.0",
                            "num2": "456.0",
                            "tru1": "true",
                        },
                        '["a","b"]',
                        '["A","B"]',
                        "PARTITIONID",
                        123,
                    ),
                ),
            ]
        )

        cemvt.put(
            partition_id="PARTITIONID",
            a="A",
            b="B",
            body_blob="BODYBLOB",
            vector="VECTOR",
            metadata=md2,
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "INSERT INTO k.tn (body_blob, vector, metadata_s, key_desc, key_vals, partition_id) VALUES (?, ?, ?, ?, ?, ?) USING TTL ? ;",  # noqa: E501
                    (
                        "BODYBLOB",
                        "VECTOR",
                        {"tru2": "true", "tru1": "true"},
                        '["a","b"]',
                        '["A","B"]',
                        "PARTITIONID",
                        123,
                    ),
                ),
            ]
        )

        cemvt.put(partition_id="PARTITIONID", a="A", b="B", metadata=md2)
        mock_db_session.assert_last_equal(
            [
                (
                    "INSERT INTO k.tn (metadata_s, key_desc, key_vals, partition_id) VALUES (?, ?, ?, ?) USING TTL ? ;",  # noqa: E501
                    (
                        {"tru2": "true", "tru1": "true"},
                        '["a","b"]',
                        '["A","B"]',
                        "PARTITIONID",
                        123,
                    ),
                ),
            ]
        )

        cemvt.get_partition(partition_id="PARTITIONID", n=10)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE partition_id = ? LIMIT ?;",
                    ("PARTITIONID", 10),
                ),
            ]
        )

        cemvt.get_partition(partition_id="PARTITIONID")
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE partition_id = ? ;",
                    ("PARTITIONID",),
                ),
            ]
        )

        cemvt.get_partition()
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE partition_id = ? ;",
                    ("PRE-PART-ID",),
                ),
            ]
        )

        cemvt.ann_search([10, 11], 2, a="A", b="B", partition_id="PARTITIONID")
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
                    ('["a","b"]', '["A","B"]', "PARTITIONID", [10, 11], 2),
                ),
            ]
        )

        cemvt.ann_search([10, 11], 2, a="A", b="B")
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
                    ('["a","b"]', '["A","B"]', "PRE-PART-ID", [10, 11], 2),
                ),
            ]
        )

        search_md = {"mdks": "mdv", "mdkn": 123, "mdke": True}
        cemvt.get(partition_id="MDPART", a="MDA", b="MDB", metadata=search_md)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdkn'] = ? AND metadata_s['mdks'] = ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ;",  # noqa: E501
                    ("true", "123.0", "mdv", '["a","b"]', '["MDA","MDB"]', "MDPART"),
                ),
            ]
        )

        cemvt.ann_search(
            [100, 101], 9, a="MDA", b="MDB", partition_id="MDPART", metadata=search_md
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdkn'] = ? AND metadata_s['mdks'] = ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
                    (
                        "true",
                        "123.0",
                        "mdv",
                        '["a","b"]',
                        '["MDA","MDB"]',
                        "MDPART",
                        [100, 101],
                        9,
                    ),
                ),
            ]
        )

        cemvt.ann_search([100, 101], 9, a="MDA", b="MDB", metadata=search_md)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdkn'] = ? AND metadata_s['mdks'] = ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
                    (
                        "true",
                        "123.0",
                        "mdv",
                        '["a","b"]',
                        '["MDA","MDB"]',
                        "PRE-PART-ID",
                        [100, 101],
                        9,
                    ),
                ),
            ]
        )

        cemvt.get_partition(partition_id="MDPART", metadata=search_md)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdkn'] = ? AND metadata_s['mdks'] = ? AND partition_id = ? ;",  # noqa: E501
                    ("true", "123.0", "mdv", "MDPART"),
                ),
            ]
        )

        cemvt.get_partition(metadata=search_md)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdkn'] = ? AND metadata_s['mdks'] = ? AND partition_id = ? ;",  # noqa: E501
                    ("true", "123.0", "mdv", "PRE-PART-ID"),
                ),
            ]
        )

        search_md_part = {"mdke": True, "mdke2": True}
        cemvt.get(partition_id="MDPART", a="MDA", b="MDB", metadata=search_md_part)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdke2'] = ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ;",  # noqa: E501
                    ("true", "true", '["a","b"]', '["MDA","MDB"]', "MDPART"),
                ),
            ]
        )

        cemvt.ann_search(
            [100, 101],
            9,
            a="MDA",
            b="MDB",
            partition_id="MDPART",
            metadata=search_md_part,
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdke2'] = ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
                    (
                        "true",
                        "true",
                        '["a","b"]',
                        '["MDA","MDB"]',
                        "MDPART",
                        [100, 101],
                        9,
                    ),
                ),
            ]
        )

        cemvt.ann_search([100, 101], 9, a="MDA", b="MDB", metadata=search_md_part)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdke2'] = ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",  # noqa: E501
                    (
                        "true",
                        "true",
                        '["a","b"]',
                        '["MDA","MDB"]',
                        "PRE-PART-ID",
                        [100, 101],
                        9,
                    ),
                ),
            ]
        )

        cemvt.get_partition(partition_id="MDPART", metadata=search_md_part)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdke2'] = ? AND partition_id = ? ;",  # noqa: E501
                    ("true", "true", "MDPART"),
                ),
            ]
        )

        cemvt.get_partition(metadata=search_md_part)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_s['mdke'] = ? AND metadata_s['mdke2'] = ? AND partition_id = ? ;",  # noqa: E501
                    ("true", "true", "PRE-PART-ID"),
                ),
            ]
        )

        cemvt.clear()
        mock_db_session.assert_last_equal(
            [
                (
                    "TRUNCATE TABLE k.tn;",
                    tuple(),
                ),
            ]
        )
