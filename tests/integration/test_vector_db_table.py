"""
Vector search tests
"""

import pytest

from cassio.vector import VectorTable

@pytest.mark.usefixtures('db_session', 'db_keyspace')
class TestVectorTable():
    """
    DB-backed tests for VectorTable
    """

    def test_put_and_get(self, db_session, db_keyspace):
        vtable_name1 = 'vector_table_1'
        v_emb_dim_1 = 3
        db_session.execute(f'DROP TABLE IF EXISTS {db_keyspace}.{vtable_name1};')
        v_table = VectorTable(
            db_session,
            db_keyspace,
            table=vtable_name1,
            embedding_dimension=v_emb_dim_1,
            primary_key_type='TEXT',
        )
        v_table.put(
            'document',
            [1,2,3],
            'doc_id',
            {'a':1},
            None,
        )
        assert(v_table.get('doc_id') == {
            'document_id': 'doc_id',
            'metadata': {'a': 1},
            'document': 'document',
            'embedding_vector': [1,2,3],
        })

    def test_put_and_search(self, db_session, db_keyspace):
        vtable_name_2 = 'vector_table_2'
        v_emb_dim_2 = 3
        db_session.execute(f'DROP TABLE IF EXISTS {db_keyspace}.{vtable_name_2};')
        v_table = VectorTable(
            db_session,
            db_keyspace,
            table=vtable_name_2,
            embedding_dimension=v_emb_dim_2,
            primary_key_type='TEXT',
        )
        v_table.put(
            'document',
            [5,5,10],
            'doc_id1',
            {'a':1},
            None,
        )
        v_table.put(
            'document',
            [10,5,5,],
            'doc_id2',
            {'a':2},
            None,
        )
        v_table.put(
            'document',
            [5,10,5],
            'doc_id3',
            {'a':3},
            None,
        )
        matches = v_table.search(
            [6,10,6],
            1,
            'cos',
            0.5,
        )
        assert(len(matches) == 1)
        assert(matches[0]['document_id'] == 'doc_id3')

    def test_put_intpk_and_get(self, db_session, db_keyspace):
        vtable_name_3 = 'vector_table_3'
        v_emb_dim_3 = 6
        db_session.execute(f'DROP TABLE IF EXISTS {db_keyspace}.{vtable_name_3};')
        v_table = VectorTable(
            db_session,
            db_keyspace,
            table=vtable_name_3,
            embedding_dimension=v_emb_dim_3,
            primary_key_type='INT',
        )
        v_table.put(
            'document_int',
            [0.1] * v_emb_dim_3,
            9999,
            {'a':1},
            None,
        )
        match = v_table.get(9999)
        assert(match['document'] == 'document_int')
        assert(match['metadata'] == {'a':1})

        match_no = v_table.get(123)
        assert(match_no is None)
