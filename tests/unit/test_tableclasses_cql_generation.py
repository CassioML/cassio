"""
CQL for mixin-based table classes tests
"""

import pytest

from cassio.table.tables import (
    VectorCassandraTable,
    ClusteredElasticMetadataVectorCassandraTable,
)


class TestTableClassesCQLGeneration:
    def test_vector_cassandra_table(self, mock_db_session):
        vt = VectorCassandraTable(
            mock_db_session, "k", "tn", vector_dimension=765, primary_key_type="UUID"
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "CREATE TABLE IF NOT EXISTS k.tn (  row_id UUID,   body_blob TEXT,   vector VECTOR<FLOAT,765>, PRIMARY KEY ( ( row_id )   )) ;",
                    tuple(),
                ),
                (
                    "CREATE CUSTOM INDEX IF NOT EXISTS idx_vector_tn ON k.tn (vector) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';",
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

    def test_clustered_elastic_metadata_vector_cassandra_table(self, mock_db_session):
        cemvt = ClusteredElasticMetadataVectorCassandraTable(
            mock_db_session,
            "k",
            "tn",
            keys=["a", "b"],
            vector_dimension=765,
            primary_key_type=["PUUID", "AT", "BT"],
            ttl_seconds=123,
            partition_id="PRE-PART-ID",
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "CREATE TABLE IF NOT EXISTS k.tn (  partition_id PUUID,   key_desc TEXT,   key_vals TEXT,   body_blob TEXT,   vector VECTOR<FLOAT,765>, attributes_blob TEXT,  metadata_s MAP<TEXT,TEXT>,   metadata_n MAP<TEXT,FLOAT>,   metadata_tags SET<TEXT>, PRIMARY KEY ( ( partition_id ) , key_desc, key_vals )) WITH CLUSTERING ORDER BY (key_desc ASC, key_vals ASC);",
                    tuple(),
                ),
                (
                    "CREATE CUSTOM INDEX IF NOT EXISTS idx_vector_tn ON k.tn (vector) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';",
                    tuple(),
                ),
                (
                    "CREATE CUSTOM INDEX IF NOT EXISTS idx_metadata_tags_tn ON k.tn (metadata_tags) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';",
                    tuple(),
                ),
                (
                    "CREATE CUSTOM INDEX IF NOT EXISTS eidx_metadata_s_tn ON k.tn (ENTRIES(metadata_s)) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';",
                    tuple(),
                ),
                (
                    "CREATE CUSTOM INDEX IF NOT EXISTS eidx_metadata_n_tn ON k.tn (ENTRIES(metadata_n)) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';",
                    tuple(),
                ),
            ]
        )

        cemvt.delete(partition_id="PARTITIONID", a="A", b="B")
        mock_db_session.assert_last_equal(
            [
                (
                    "DELETE FROM k.tn WHERE key_desc = ? AND key_vals = ? AND partition_id = ?;",
                    ('["a","b"]', '["A","B"]', "PARTITIONID"),
                ),
            ]
        )

        cemvt.get(partition_id="PARTITIONID", a="A", b="B")
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE key_desc = ? AND key_vals = ? AND partition_id = ? ;",
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
                    "INSERT INTO k.tn (body_blob, vector, key_desc, key_vals, partition_id) VALUES (?, ?, ?, ?, ?) USING TTL ? ;",
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
                    "INSERT INTO k.tn (body_blob, vector, metadata_s, metadata_n, metadata_tags, key_desc, key_vals, partition_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? ;",
                    (
                        "BODYBLOB",
                        "VECTOR",
                        {"str1": "STR1"},
                        {"num1": 123.0, "num2": 456.0},
                        {"tru1"},
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
                    "INSERT INTO k.tn (body_blob, vector, metadata_tags, key_desc, key_vals, partition_id) VALUES (?, ?, ?, ?, ?, ?) USING TTL ? ;",
                    (
                        "BODYBLOB",
                        "VECTOR",
                        {"tru2", "tru1"},
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
                    "INSERT INTO k.tn (metadata_tags, key_desc, key_vals, partition_id) VALUES (?, ?, ?, ?) USING TTL ? ;",
                    ({"tru2", "tru1"}, '["a","b"]', '["A","B"]', "PARTITIONID", 123),
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
                    "SELECT * FROM k.tn WHERE key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",
                    ('["a","b"]', '["A","B"]', "PARTITIONID", [10, 11], 2),
                ),
            ]
        )

        cemvt.ann_search([10, 11], 2, a="A", b="B")
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",
                    ('["a","b"]', '["A","B"]', "PRE-PART-ID", [10, 11], 2),
                ),
            ]
        )

        search_md = {"mdks": "mdv", "mdkn": 123, "mdke": True}
        cemvt.get(partition_id="MDPART", a="MDA", b="MDB", metadata=search_md)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_tags CONTAINS ? AND metadata_s['mdks'] = ? AND metadata_n['mdkn'] = ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ;",
                    ("mdke", "mdv", 123.0, '["a","b"]', '["MDA","MDB"]', "MDPART"),
                ),
            ]
        )

        cemvt.ann_search(
            [100, 101], 9, a="MDA", b="MDB", partition_id="MDPART", metadata=search_md
        )
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_tags CONTAINS ? AND metadata_s['mdks'] = ? AND metadata_n['mdkn'] = ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",
                    (
                        "mdke",
                        "mdv",
                        123.0,
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
                    "SELECT * FROM k.tn WHERE metadata_tags CONTAINS ? AND metadata_s['mdks'] = ? AND metadata_n['mdkn'] = ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",
                    (
                        "mdke",
                        "mdv",
                        123.0,
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
                    "SELECT * FROM k.tn WHERE metadata_tags CONTAINS ? AND metadata_s['mdks'] = ? AND metadata_n['mdkn'] = ? AND partition_id = ? ;",
                    ("mdke", "mdv", 123.0, "MDPART"),
                ),
            ]
        )

        cemvt.get_partition(metadata=search_md)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_tags CONTAINS ? AND metadata_s['mdks'] = ? AND metadata_n['mdkn'] = ? AND partition_id = ? ;",
                    ("mdke", "mdv", 123.0, "PRE-PART-ID"),
                ),
            ]
        )

        search_md_part = {"mdke": True, "mdke2": True}
        cemvt.get(partition_id="MDPART", a="MDA", b="MDB", metadata=search_md_part)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_tags CONTAINS ? AND metadata_tags CONTAINS ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ;",
                    ("mdke", "mdke2", '["a","b"]', '["MDA","MDB"]', "MDPART"),
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
                    "SELECT * FROM k.tn WHERE metadata_tags CONTAINS ? AND metadata_tags CONTAINS ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",
                    (
                        "mdke",
                        "mdke2",
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
                    "SELECT * FROM k.tn WHERE metadata_tags CONTAINS ? AND metadata_tags CONTAINS ? AND key_desc = ? AND key_vals = ? AND partition_id = ? ORDER BY vector ANN OF ? LIMIT ?;",
                    (
                        "mdke",
                        "mdke2",
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
                    "SELECT * FROM k.tn WHERE metadata_tags CONTAINS ? AND metadata_tags CONTAINS ? AND partition_id = ? ;",
                    ("mdke", "mdke2", "MDPART"),
                ),
            ]
        )

        cemvt.get_partition(metadata=search_md_part)
        mock_db_session.assert_last_equal(
            [
                (
                    "SELECT * FROM k.tn WHERE metadata_tags CONTAINS ? AND metadata_tags CONTAINS ? AND partition_id = ? ;",
                    ("mdke", "mdke2", "PRE-PART-ID"),
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
