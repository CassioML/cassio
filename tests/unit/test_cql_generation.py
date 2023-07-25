"""
CQL generation tests
"""

import pytest

from cassio.vector import VectorTable


class TestGenerateCQL:
    """
    Tests for the creation of CQL statements
    """

    def test_create_vector_table(self, mock_db_session):
        vector_table = VectorTable(
            session=mock_db_session,
            keyspace="keyspace",
            table="table",
            embedding_dimension=123,
            primary_key_type="PK_TYPE",
        )
        statements = mock_db_session.last(2)
        expected_statements = [
            (mock_db_session.normalizeCQLStatement(st[0]), st[1])
            for st in [
                (
                    "create table if not exists keyspace.table ( document_id pk_type primary key, embedding_vector vector<float, 123>, document text, metadata_blob text )",
                    (),
                ),
                (
                    "create custom index if not exists table_embedding_idx on keyspace.table ( embedding_vector ) using 'org.apache.cassandra.index.sai.storageattachedindex'",
                    (),
                ),
            ]
        ]
        assert statements == expected_statements
