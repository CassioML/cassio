"""
fixtures for testing
"""

import os
from typing import Dict, List

import pytest

from cassandra.cluster import Cluster  # type: ignore
from cassandra.auth import PlainTextAuthProvider  # type: ignore

from cassio.table.cql import MockDBSession

import cassio


# DB session (as per settings detected in env vars)
dbSession = None


def createDBSessionSingleton():
    global dbSession
    if dbSession is None:
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
            dbSession = cluster.connect()
        elif mode == "LOCAL_CASSANDRA":
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
                contact_points = [
                    cp.strip() for cp in CASSANDRA_CONTACT_POINTS.split(",")
                ]
            else:
                contact_points = None
            #
            cluster = Cluster(
                contact_points,
                auth_provider=auth_provider,
            )
            localSession = cluster.connect()
            return localSession
        else:
            raise NotImplementedError
    return dbSession


def getDBKeyspace():
    mode = os.getenv("TEST_DB_MODE", "LOCAL_CASSANDRA")
    if mode == "ASTRA_DB":
        ASTRA_DB_KEYSPACE = os.environ["ASTRA_DB_KEYSPACE"]
        return ASTRA_DB_KEYSPACE
    elif mode == "LOCAL_CASSANDRA":
        CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "default_keyspace")
        return CASSANDRA_KEYSPACE


# Fixtures


@pytest.fixture(scope="session")
def db_session():
    return createDBSessionSingleton()


@pytest.fixture(scope="session")
def db_keyspace():
    return getDBKeyspace()


@pytest.fixture(scope="function")
def mock_db_session():
    return MockDBSession()


# Utilities


def _reset_cassio_globals():
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
def extractor_tables(db_session, db_keyspace):
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
