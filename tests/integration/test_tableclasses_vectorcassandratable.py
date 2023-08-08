"""
Table classes integration test - VectorCassandraTable
"""
import math
import pytest

from cassio.table.tables import (
    VectorCassandraTable,
)


N = 8


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestVectorCassandraTable:
    def test_crud(self, db_session, db_keyspace):
        table_name = "v_ct"
        db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
        #
        t = VectorCassandraTable(
            db_session,
            db_keyspace,
            table_name,
            vector_dimension=2,
            primary_key_type="TEXT",
        )

        for n_theta in range(N):
            theta = n_theta * math.pi * 2 / N
            t.put(
                row_id=f"theta_{n_theta}",
                body_blob=f"theta = {theta:.4f}",
                vector=[math.cos(theta), math.sin(theta)],
            )

        # retrieval
        theta_1 = t.get(row_id="theta_1")
        assert abs(theta_1["vector"][0] - math.cos(math.pi * 2 / N)) < 3.0e-8
        assert abs(theta_1["vector"][1] - math.sin(math.pi * 2 / N)) < 3.0e-8

        # ANN
        # a vector halfway between 0 and 1 inserted above
        query_theta = 1 * math.pi * 2 / (2 * N)
        ref_vector = [math.cos(query_theta), math.sin(query_theta)]
        ann_results = list(t.ann_search(ref_vector, n=4))
        assert {r["row_id"] for r in ann_results[:2]} == {"theta_1", "theta_0"}
        assert {r["row_id"] for r in ann_results[2:4]} == {"theta_2", "theta_7"}

        t.clear()


if __name__ == "__main__":
    # TEST_DB_MODE=LOCAL_CASSANDRA python -m pdb -m  \
    #   tests.integration.test_tableclasses_vectorcassandratable
    from ..conftest import createDBSessionSingleton, getDBKeyspace

    s = createDBSessionSingleton()
    k = getDBKeyspace()
    TestVectorCassandraTable().test_crud(s, k)
