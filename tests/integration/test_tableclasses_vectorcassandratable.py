"""
Table classes integration test - VectorCassandraTable
"""
import math
import os

import pytest
from cassandra.cluster import Session

from cassio.table.cql import STANDARD_ANALYZER
from cassio.table.tables import (
    VectorCassandraTable,
)


N = 8


def crud(db_session: Session, db_keyspace: str) -> None:
    table_name = "v_ct"
    db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
    #
    t = VectorCassandraTable(
        session=db_session,
        keyspace=db_keyspace,
        table=table_name,
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
    assert theta_1 is not None
    assert abs(theta_1["vector"][0] - math.cos(math.pi * 2 / N)) < 3.0e-8
    assert abs(theta_1["vector"][1] - math.sin(math.pi * 2 / N)) < 3.0e-8

    # ANN
    # a vector halfway between 0 and 1 inserted above
    query_theta = 1 * math.pi * 2 / (2 * N)
    ref_vector = [math.cos(query_theta), math.sin(query_theta)]
    ann_results = list(t.ann_search(ref_vector, n=4))
    assert {r["row_id"] for r in ann_results[:2]} == {"theta_1", "theta_0"}
    assert {r["row_id"] for r in ann_results[2:4]} == {"theta_2", "theta_7"}

    with pytest.raises(ValueError):
        t.get(body_search="theta")

    t.clear()


@pytest.mark.skipif(
    os.getenv("TEST_DB_MODE", "LOCAL_CASSANDRA") != "ASTRA_DB",
    reason="requires a test Astra DB instance",
)
def index_analyzers(db_session: Session, db_keyspace: str) -> None:
    table_name = "v_ct_analyzers"
    db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")
    #
    t = VectorCassandraTable(
        session=db_session,
        keyspace=db_keyspace,
        table=table_name,
        vector_dimension=2,
        primary_key_type="TEXT",
        body_index_options=[STANDARD_ANALYZER],
    )

    for n_theta in range(N):
        theta = n_theta * math.pi * 2 / N
        t.put(
            row_id=f"theta_{n_theta}",
            body_blob=f"body blob theta_{n_theta} = {theta:.4f}",
            vector=[math.cos(theta), math.sin(theta)],
        )

    # a vector halfway between 0 and 1 inserted above
    query_theta = 1 * math.pi * 2 / (2 * N)
    ref_vector = [math.cos(query_theta), math.sin(query_theta)]

    ann_results = list(t.ann_search(ref_vector, n=4, body_search="theta_2"))
    assert {r["row_id"] for r in ann_results} == {"theta_2"}

    ann_results = list(t.ann_search(ref_vector, n=4, body_search=["theta_2", "blob"]))
    assert {r["row_id"] for r in ann_results} == {"theta_2"}

    ann_results = list(t.ann_search(ref_vector, n=4, body_search=["theta_2", "foo"]))
    assert ann_results == []

    t.clear()


@pytest.mark.asyncio
async def test_async_vector_dimension(db_session: Session, db_keyspace: str) -> None:
    table_name = "v_ct"
    db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{table_name};")

    async def get_vector_dimension() -> int:
        return 2

    t = VectorCassandraTable(
        session=db_session,
        keyspace=db_keyspace,
        table=table_name,
        vector_dimension=get_vector_dimension(),
        async_setup=True,
        primary_key_type="TEXT",
    )
    await t.aput(
        row_id="theta",
        body_blob="theta",
        vector=[0.1, 0.2],
    )
    await t.aclear()
