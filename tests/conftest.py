"""
fixtures for testing
"""

import os
from typing import Dict, List, Iterator, Tuple

import pytest

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from cassio.table.cql import MockDBSession

import cassio


# Fixtures


@pytest.fixture(scope="session", autouse=True)
def cassandra_port(db_keyspace: str) -> Iterator[int]:
    if os.getenv("TEST_DB_MODE", "LOCAL_CASSANDRA") == "TESTCONTAINERS_CASSANDRA":
        cassandra = DockerContainer("cassandra:5")
        cassandra.with_exposed_ports(9042)
        cassandra.with_env(
            "JVM_OPTS",
            "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0",
        )
        cassandra.with_env("HEAP_NEWSIZE", "128M")
        cassandra.with_env("MAX_HEAP_SIZE", "1024M")
        cassandra.with_env("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
        cassandra.with_env("CASSANDRA_DC", "datacenter1")
        cassandra.start()
        wait_for_logs(cassandra, "Startup complete")
        cassandra.get_wrapped_container().exec_run(
            (
                f"""cqlsh -e "CREATE KEYSPACE {db_keyspace} WITH replication = """
                '''{'class': 'SimpleStrategy', 'replication_factor': '1'};"'''
            )
        )
        os.environ["CASSANDRA_CONTACT_POINTS"] = "127.0.0.1"
        yield cassandra.get_exposed_port(9042)
        cassandra.stop()
    else:
        yield 9042


@pytest.fixture(scope="session")
def db_session(cassandra_port: int) -> Session:
    mode = os.getenv("TEST_DB_MODE", "LOCAL_CASSANDRA")
    # the proper DB session is created as required
    if mode == "ASTRA_DB":
        ASTRA_DB_SECURE_BUNDLE_PATH = os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"]
        ASTRA_DB_CLIENT_ID = "token"
        ASTRA_DB_APPLICATION_TOKEN = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
        cluster = Cluster(
            cloud={
                "secure_connect_bundle": ASTRA_DB_SECURE_BUNDLE_PATH,
            },
            auth_provider=PlainTextAuthProvider(
                ASTRA_DB_CLIENT_ID,
                ASTRA_DB_APPLICATION_TOKEN,
            ),
        )
        return cluster.connect()
    elif mode in ["LOCAL_CASSANDRA", "TESTCONTAINERS_CASSANDRA"]:
        CASSANDRA_USERNAME = os.environ.get("CASSANDRA_USERNAME")
        CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD")
        if CASSANDRA_USERNAME and CASSANDRA_PASSWORD:
            auth_provider = PlainTextAuthProvider(
                CASSANDRA_USERNAME,
                CASSANDRA_PASSWORD,
            )
        else:
            auth_provider = None
        CASSANDRA_CONTACT_POINTS = os.environ.get("CASSANDRA_CONTACT_POINTS")
        if CASSANDRA_CONTACT_POINTS:
            contact_points = [cp.strip() for cp in CASSANDRA_CONTACT_POINTS.split(",")]
        else:
            contact_points = None
        #
        cluster = Cluster(
            contact_points,
            port=cassandra_port,
            auth_provider=auth_provider,
        )
        return cluster.connect()
    else:
        raise ValueError("invalid TEST_DB_MODE")


@pytest.fixture(scope="session")
def db_keyspace() -> str:
    mode = os.getenv("TEST_DB_MODE", "LOCAL_CASSANDRA")
    if mode == "ASTRA_DB":
        astra_db_keyspace = os.environ["ASTRA_DB_KEYSPACE"]
        return astra_db_keyspace
    elif mode in ["LOCAL_CASSANDRA", "TESTCONTAINERS_CASSANDRA"]:
        cassandra_keyspace = os.getenv("CASSANDRA_KEYSPACE", "default_keyspace")
        return cassandra_keyspace
    else:
        raise ValueError("invalid TEST_DB_MODE")


@pytest.fixture(scope="function")
def mock_db_session() -> MockDBSession:
    return MockDBSession()


# Utilities


def _reset_cassio_globals() -> None:
    cassio.config.default_session = None
    cassio.config.default_keyspace = None


def _freeze_envvars(var_names: List[str]) -> Dict[str, str]:
    frozen = {var: os.environ[var] for var in var_names if var in os.environ}
    for var in frozen.keys():
        del os.environ[var]
    return frozen


def _unfreeze_envvars(var_map: Dict[str, str]) -> None:
    for var, val in var_map.items():
        os.environ[var] = val


P_TABLE_NAME = "people_x"
C_TABLE_NAME = "nicknames_x"


@pytest.fixture(scope="class")
def extractor_tables(
    db_session: Session, db_keyspace: str
) -> Iterator[Tuple[str, str]]:
    db_session.execute(
        f"CREATE TABLE IF NOT EXISTS {db_keyspace}.{P_TABLE_NAME} (city text, name text, age int, PRIMARY KEY (city, name)) WITH CLUSTERING ORDER BY (name ASC);"  # noqa: E501
    )
    db_session.execute(
        f"INSERT INTO {db_keyspace}.{P_TABLE_NAME} (city, name, age) VALUES ('milan', 'alba', 11);"  # noqa: E501
    )
    db_session.execute(
        f"CREATE TABLE IF NOT EXISTS {db_keyspace}.{C_TABLE_NAME} (city text PRIMARY KEY, nickname text);"  # noqa: E501
    )
    db_session.execute(
        f"INSERT INTO {db_keyspace}.{C_TABLE_NAME} (city, nickname) VALUES ('milan', 'Taaac');"  # noqa: E501
    )

    yield (P_TABLE_NAME, C_TABLE_NAME)

    db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{C_TABLE_NAME};")
    db_session.execute(f"DROP TABLE IF EXISTS {db_keyspace}.{P_TABLE_NAME};")
