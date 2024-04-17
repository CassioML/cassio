"""
init method, Astra side
"""
import os

import pytest
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

import cassio
from cassio.config import resolve_keyspace, resolve_session
from tests.conftest import _freeze_envvars, _reset_cassio_globals, _unfreeze_envvars

CASSANDRA_USERNAME = os.environ.get("CASSANDRA_USERNAME")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD")
CASSANDRA_CONTACT_POINTS = os.environ.get("CASSANDRA_CONTACT_POINTS")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE")


@pytest.mark.skipif(
    os.getenv("TEST_DB_MODE", "LOCAL_CASSANDRA")
    not in ["LOCAL_CASSANDRA", "TESTCONTAINERS_CASSANDRA"],
    reason="requires a test Cassandra",
)
class TestInitCassandra:
    """
    Init method signatures, Cassandra side.
    Requires a running (local) Cassandra cluster.
    """

    def test_init_session(self, cassandra_port: int) -> None:
        #
        _reset_cassio_globals()
        assert resolve_session() is None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"
        #
        _cps = [
            cp.strip()
            for cp in (CASSANDRA_CONTACT_POINTS or "").split(",")
            if cp.strip()
        ]
        if CASSANDRA_USERNAME and CASSANDRA_PASSWORD:
            auth = PlainTextAuthProvider(CASSANDRA_USERNAME, CASSANDRA_PASSWORD)
        else:
            auth = None
        if _cps:
            cluster = Cluster(_cps, port=cassandra_port, auth_provider=auth)
        else:
            cluster = Cluster(port=cassandra_port, auth_provider=auth)
        session = cluster.connect()
        #
        cassio.init(session=session)
        assert resolve_session() is not None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"
        #
        _reset_cassio_globals()
        cassio.init(session=session, keyspace="l")
        assert resolve_session() is not None
        assert resolve_keyspace() == "l"
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"

    def test_init_empty_cps(self, cassandra_port: int) -> None:
        stolen = _freeze_envvars(["CASSANDRA_CONTACT_POINTS"])
        _reset_cassio_globals()
        assert resolve_session() is None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"
        cassio.init(
            contact_points=CASSANDRA_CONTACT_POINTS or "",
            username=CASSANDRA_USERNAME,
            password=CASSANDRA_PASSWORD,
            cluster_kwargs={"port": cassandra_port},
        )
        assert resolve_session() is not None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"
        _unfreeze_envvars(stolen)

    def test_init_cps(self, cassandra_port: int) -> None:
        _reset_cassio_globals()
        assert resolve_session() is None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"
        cassio.init(
            contact_points=CASSANDRA_CONTACT_POINTS or "127.0.0.1",
            username=CASSANDRA_USERNAME,
            password=CASSANDRA_PASSWORD,
            cluster_kwargs={"port": cassandra_port},
        )
        assert resolve_session() is not None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"

    def test_init_cleaning_cps(self, cassandra_port: int) -> None:
        _reset_cassio_globals()
        assert resolve_session() is None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"
        cassio.init(
            contact_points=(CASSANDRA_CONTACT_POINTS or "127.0.0.1") + ",,,,   , ",
            username=CASSANDRA_USERNAME,
            password=CASSANDRA_PASSWORD,
            cluster_kwargs={"port": cassandra_port},
        )
        assert resolve_session() is not None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"

    def test_init_with_keyspace(self, cassandra_port: int) -> None:
        _reset_cassio_globals()
        assert resolve_session() is None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"
        cassio.init(
            contact_points=CASSANDRA_CONTACT_POINTS or "127.0.0.1",
            username=CASSANDRA_USERNAME,
            password=CASSANDRA_PASSWORD,
            keyspace=CASSANDRA_KEYSPACE,
            cluster_kwargs={"port": cassandra_port},
        )
        assert resolve_session() is not None
        assert resolve_keyspace() == CASSANDRA_KEYSPACE
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"

    def test_init_auto(self, cassandra_port: int) -> None:
        _reset_cassio_globals()
        stolen = _freeze_envvars(
            [
                "ASTRA_DB_APPLICATION_TOKEN",
                "ASTRA_DB_INIT_STRING",
                "ASTRA_DB_SECURE_BUNDLE_PATH",
                "ASTRA_DB_KEYSPACE",
                "ASTRA_DB_DATABASE_ID",
            ]
        )
        cassio.init(auto=True, cluster_kwargs={"port": cassandra_port})
        assert resolve_session() is not None
        assert resolve_keyspace() == os.environ.get("CASSANDRA_KEYSPACE")
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"
        _unfreeze_envvars(stolen)
