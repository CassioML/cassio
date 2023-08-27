"""
Bundle conversion tests
"""
import pytest
import tempfile
import shutil
import os

from cassandra.cluster import Cluster  # type: ignore
from cassandra.auth import PlainTextAuthProvider  # type: ignore
from cassandra.cluster import NoHostAvailable  # type: ignore

import cassio  # type: ignore
from cassio.config import resolve_session, resolve_keyspace
from cassio.config.bundle_management import (
    bundle_path_to_init_string,
    init_string_to_bundle_path_and_options,
)


def _reset_cassio_globals():
    cassio.config.default_session = None
    cassio.config.default_keyspace = None


@pytest.mark.skipif(
    os.environ["TEST_DB_MODE"] != "ASTRA_DB", reason="requires a test on Astra DB"
)
class TestZeroBundleInit:
    """
    Init methods and facilities around bundle conversions.
    Save for `test_bundle_to_valid_init_string`, all other
    require a "INIT_STRING" environment variable with the init string to run.
    """

    def test_bundle_to_valid_init_string(self):
        """
        Make a "genuine" bundle into an init string,
        then back to a bundle in a temp dir,
        then use it to get a cloud connection.
        """
        ASTRA_DB_SECURE_BUNDLE_PATH = os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"]
        ASTRA_DB_CLIENT_ID = "token"
        ASTRA_DB_APPLICATION_TOKEN = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
        #
        temp_dir = tempfile.mkdtemp(dir=tempfile.gettempdir())
        try:
            original_bundle_path = ASTRA_DB_SECURE_BUNDLE_PATH
            init_string = bundle_path_to_init_string(original_bundle_path)
            built_b, options = init_string_to_bundle_path_and_options(
                init_string,
                target_dir=temp_dir,
            )
            #
            cluster = Cluster(
                cloud={"secure_connect_bundle": built_b},
                auth_provider=PlainTextAuthProvider(
                    ASTRA_DB_CLIENT_ID,
                    ASTRA_DB_APPLICATION_TOKEN,
                ),
            )
            _ = cluster.connect()
        finally:
            shutil.rmtree(temp_dir)

    def test_init_noop(self):
        _reset_cassio_globals()
        assert resolve_session() is None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"
        cassio.init()
        assert resolve_session() is None
        assert resolve_keyspace() is None
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"

    def test_init_session(self):
        _reset_cassio_globals()
        cassio.init(session="s")
        assert resolve_session() == "s"
        assert resolve_keyspace() is None
        assert resolve_session("t") == "t"
        assert resolve_session(None) == "s"
        assert resolve_keyspace("k") == "k"

    def test_init_scb(self):
        _reset_cassio_globals()
        scb = os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"]
        tok = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
        cassio.init(secure_connect_bundle=scb, token=tok)
        assert resolve_session() is not None
        assert resolve_keyspace() is None

    def test_init_scb_keyspace(self):
        _reset_cassio_globals()
        scb = os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"]
        tok = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
        kys = os.environ["ASTRA_DB_KEYSPACE"]
        cassio.init(secure_connect_bundle=scb, token=tok, keyspace=kys)
        assert resolve_session() is not None
        assert resolve_keyspace() is not None

    def test_init_init_string(self):
        _reset_cassio_globals()
        inst = os.environ["INIT_STRING"]
        cassio.init(init_string=inst)
        assert resolve_session() is not None
        assert resolve_keyspace() is not None

    def test_init_init_string_overwrite_ks(self):
        _reset_cassio_globals()
        inst = os.environ["INIT_STRING"]
        cassio.init(init_string=inst, keyspace="my_keyspace")
        assert resolve_session() is not None
        assert resolve_keyspace() == "my_keyspace"

    def test_init_init_string_overwrite_tk(self):
        _reset_cassio_globals()
        inst = os.environ["INIT_STRING"]
        with pytest.raises(NoHostAvailable):
            cassio.init(init_string=inst, token="AstraCS:wrong")
