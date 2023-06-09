"""
Vector search tests
"""

import pytest

from cassio.vector import VectorDBTable

@pytest.mark.usefixtures('db_session', 'db_keyspace')
class TestVectorDBTable():
    """
    DB-backed tests for VectorDBTable
    """

    def test_put_and_get_autoidFalse(self, db_session, db_keyspace):
        vTableName1 = 'vector_table_1'
        vEmbDim1=3
        db_session.execute(f'DROP TABLE IF EXISTS {db_keyspace}.{vTableName1};')
        vTable = VectorDBTable(
            db_session,
            db_keyspace,
            tableName=vTableName1,
            embeddingDimension=vEmbDim1,
            autoID=False,
        )
        vTable.put(
            'document',
            [1,2,3],
            'doc_id',
            {'a':1},
            None,
        )
        assert(vTable.get('doc_id') == {
            'document_id': 'doc_id',
            'metadata': {'a': 1},
            'document': 'document',
            'embedding_vector': [1,2,3],
        })

    def test_put_and_search_autoidFalse(self, db_session, db_keyspace):
        vTableName2 = 'vector_table_2'
        vEmbDim2=3
        db_session.execute(f'DROP TABLE IF EXISTS {db_keyspace}.{vTableName2};')
        vTable = VectorDBTable(
            db_session,
            db_keyspace,
            tableName=vTableName2,
            embeddingDimension=vEmbDim2,
            autoID=False,
        )
        vTable.put(
            'document',
            [5,5,10],
            'doc_id1',
            {'a':1},
            None,
        )
        vTable.put(
            'document',
            [10,5,5,],
            'doc_id2',
            {'a':2},
            None,
        )
        vTable.put(
            'document',
            [5,10,5],
            'doc_id3',
            {'a':3},
            None,
        )
        matches = vTable.search(
            [6,10,6],
            1,
            5,
            'cos',
            0.5,
        )
        assert(len(matches)==1)
        assert(matches[0]['document_id'] == 'doc_id3')
