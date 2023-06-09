"""
fixtures for testing
"""

import os

import pytest

from cassandra.cluster import (
    Cluster,
)
from cassandra.auth import PlainTextAuthProvider

# Mock DB session

class MockDBSession():

    def __init__(self):
        self.statements = []

    def execute(self, statement, arguments=tuple()):
        self.statements.append((statement, arguments))

    def last(self, n):
        if n<=0:
            return []
        else:
            return self.statements[-n:]

# DB session (as per settings detected in env vars)
dbSession = None

def createDBSessionSingleton():
    global dbSession
    if dbSession is None:
        mode = os.environ['TEST_DB_MODE']
        # the proper DB session is created as required
        if mode == 'ASTRA_DB':
            ASTRA_DB_SECURE_BUNDLE_PATH = os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"]
            ASTRA_DB_CLIENT_ID = "token"
            ASTRA_DB_APPLICATION_TOKEN = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
            ASTRA_DB_KEYSPACE = os.environ["ASTRA_DB_KEYSPACE"]
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
        elif mode == 'LOCAL_CASSANDRA':
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
                contact_points = [cp.strip() for cp in CASSANDRA_CONTACT_POINTS.split(',')]
            else:
                contact_points = None
            CASSANDRA_KEYSPACE = os.environ["CASSANDRA_KEYSPACE"]
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
        mode = os.environ['TEST_DB_MODE']
        if mode == 'ASTRA_DB':
            ASTRA_DB_KEYSPACE = os.environ["ASTRA_DB_KEYSPACE"]
            return ASTRA_DB_KEYSPACE
        elif mode == 'LOCAL_CASSANDRA':
            CASSANDRA_KEYSPACE = os.environ["CASSANDRA_KEYSPACE"]
            return CASSANDRA_KEYSPACE


# Fixtures

@pytest.fixture(scope='session')
def db_session():
    return createDBSessionSingleton()


@pytest.fixture(scope='session')
def db_keyspace():
    return getDBKeyspace()
