"""
Vector search tests
"""

import time
import pytest

from cassio.vector import VectorTable


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestVectorTable:
    """
    DB-backed tests for VectorTable
    """

    def test_put_and_get(self, db_session, db_keyspace):
        vtable_name1 = "vector_table_1"
        v_emb_dim_1 = 3
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{vtable_name1};")
        v_table = VectorTable(
            db_session,
            db_keyspace,
            table=vtable_name1,
            embedding_dimension=v_emb_dim_1,
            primary_key_type="TEXT",
        )
        v_table.put(
            "document",
            [1, 2, 3],
            "doc_id",
            {"a": "value_1"},
            None,
        )
        assert v_table.get("doc_id") == {
            "document_id": "doc_id",
            "metadata": {"a": "value_1"},
            "document": "document",
            "embedding_vector": [1, 2, 3],
        }

    def test_put_and_search(self, db_session, db_keyspace):
        vtable_name_2 = "vector_table_2"
        v_emb_dim_2 = 3
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{vtable_name_2};")
        v_table = VectorTable(
            db_session,
            db_keyspace,
            table=vtable_name_2,
            embedding_dimension=v_emb_dim_2,
            primary_key_type="TEXT",
        )
        v_table.put(
            "document",
            [5, 5, 10],
            "doc_id1",
            {"a": 1},
            None,
        )
        v_table.put(
            "document",
            [
                10,
                5,
                5,
            ],
            "doc_id2",
            {"a": 2},
            None,
        )
        v_table.put(
            "document",
            [5, 10, 5],
            "doc_id3",
            {"a": 3},
            None,
        )
        matches = v_table.search(
            [6, 10, 6],
            1,
            "cos",
            0.5,
        )
        assert len(matches) == 1
        assert matches[0]["document_id"] == "doc_id3"

    def test_put_and_search_async(self, db_session, db_keyspace):
        vtable_name_2a = "vector_table_2async"
        v_emb_dim_2a = 3
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{vtable_name_2a};")
        v_table = VectorTable(
            db_session,
            db_keyspace,
            table=vtable_name_2a,
            embedding_dimension=v_emb_dim_2a,
            primary_key_type="TEXT",
        )
        futures = [
            v_table.put_async(
                "document",
                [5, 5, 10],
                "doc_id1",
                {"a": 1},
                None,
            ),
            v_table.put_async(
                "document",
                [
                    10,
                    5,
                    5,
                ],
                "doc_id2",
                {"a": 2},
                None,
            ),
            v_table.put_async(
                "document",
                [5, 10, 5],
                "doc_id3",
                {"a": 3},
                None,
            ),
        ]
        for f in futures:
            _ = f.result()
        matches = v_table.search(
            [6, 10, 6],
            1,
            "cos",
            0.5,
        )
        assert len(matches) == 1
        assert matches[0]["document_id"] == "doc_id3"

    def test_put_intpk_and_get(self, db_session, db_keyspace):
        vtable_name_3 = "vector_table_3"
        v_emb_dim_3 = 6
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{vtable_name_3};")
        v_table = VectorTable(
            db_session,
            db_keyspace,
            table=vtable_name_3,
            embedding_dimension=v_emb_dim_3,
            primary_key_type="INT",
        )
        v_table.put(
            "document_int",
            [0.1] * v_emb_dim_3,
            9999,
            {"a": "value_1"},
            None,
        )
        match = v_table.get(9999)
        assert match["document"] == "document_int"
        assert match["metadata"] == {"a": "value_1"}

        match_no = v_table.get(123)
        assert match_no is None

    def test_null_json(self, db_session, db_keyspace):
        vtable_name4 = "vector_table_4"
        v_emb_dim_4 = 3
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{vtable_name4};")
        v_table = VectorTable(
            db_session,
            db_keyspace,
            table=vtable_name4,
            embedding_dimension=v_emb_dim_4,
            primary_key_type="TEXT",
        )
        v_table.put(
            "document",
            [1, 2, 3],
            "doc_id",
            None,
            None,
        )
        assert v_table.get("doc_id") == {
            "document_id": "doc_id",
            "metadata": {},
            "document": "document",
            "embedding_vector": [1, 2, 3],
        }

    def test_nullsearch_results(self, db_session, db_keyspace):
        vtable_name5 = "vector_table_5"
        v_emb_dim_5 = 5
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{vtable_name5};")
        v_table = VectorTable(
            db_session,
            db_keyspace,
            table=vtable_name5,
            embedding_dimension=v_emb_dim_5,
            primary_key_type="INT",
        )
        v_table.put("boasting", [2, 2, 2, 2, 2], 123)
        assert v_table.search([1, 0, 0, 0, 0], 10, "cos", 1.01) == []
        # cannot use zero-vectors with cosine similarity:
        with pytest.raises(ValueError):
            _ = v_table.search([0, 0, 0, 0, 0], 10, "cos", 1.01)
        v_table.clear()

    def test_ttl(self, db_session, db_keyspace):
        vtable_name6 = "vector_table_6"
        v_emb_dim_6 = 2
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{vtable_name6};")
        v_table = VectorTable(
            db_session,
            db_keyspace,
            table=vtable_name6,
            embedding_dimension=v_emb_dim_6,
            primary_key_type="TEXT",
        )
        #
        v_table.put("this is short lived", [1, 0], "short_lived", ttl_seconds=2)
        v_table.put("this is long lived", [0, 1], "long_lived", ttl_seconds=5)
        time.sleep(0.2)
        assert len(v_table.search([0.5, 0.5], 3, "cos", 0.01)) == 2
        time.sleep(2.5)
        assert len(v_table.search([0.5, 0.5], 3, "cos", 0.01)) == 1
        time.sleep(3.0)
        assert len(v_table.search([0.5, 0.5], 3, "cos", 0.01)) == 0


if __name__ == "__main__":
    from ..conftest import createDBSessionSingleton, getDBKeyspace

    s = createDBSessionSingleton()
    k = getDBKeyspace()

    TestVectorTable().test_null_json(s, k)
