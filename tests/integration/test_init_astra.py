"""
Bundle conversion tests
"""
import pytest
import tempfile
import shutil
import os

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable

import cassio
from cassio.config import resolve_session, resolve_keyspace
from cassio.config.bundle_management import (
    bundle_path_to_init_string,
    init_string_to_bundle_path_and_options,
)

from tests.conftest import (
    _freeze_envvars,
    _reset_cassio_globals,
    _unfreeze_envvars,
)


@pytest.mark.skipif(
    os.getenv("TEST_DB_MODE", "LOCAL_CASSANDRA") != "ASTRA_DB",
    reason="requires a test Astra DB instance",
)
class TestInitAstra:
    """
    Init method signatures, Astra side. Trying various possible ways to supply
    Astra-related initialization to CassIO.
    Also testing the init string packing/unpacking.
    Requires a "ASTRA_DB_INIT_STRING" environment variable with the init string to run.
    """

    def test_bundle_to_valid_init_string(self) -> None:
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

    def test_init_noop(self) -> None:
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

    def test_init_session(self) -> None:
        _reset_cassio_globals()
        cassio.init(session="s")
        assert resolve_session() == "s"
        assert resolve_keyspace() is None
        assert resolve_session("t") == "t"
        assert resolve_session(None) == "s"
        assert resolve_keyspace("k") == "k"

    def test_init_scb(self) -> None:
        _reset_cassio_globals()
        scb = os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"]
        tok = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
        cassio.init(secure_connect_bundle=scb, token=tok)
        assert resolve_keyspace() is not None  # through inspecting the scb
        assert resolve_session() is not None

    def test_init_scb_keyspace(self) -> None:
        _reset_cassio_globals()
        scb = os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"]
        tok = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
        kys = os.environ["ASTRA_DB_KEYSPACE"]
        cassio.init(secure_connect_bundle=scb, token=tok, keyspace=kys)
        assert resolve_session() is not None
        assert resolve_keyspace() is not None

    @pytest.mark.skipif(
        os.environ.get("ASTRA_DB_DATABASE_ID") is None,
        reason="requires the database ID to download the secure bundle",
    )
    def test_init_download_scb(self) -> None:
        _reset_cassio_globals()
        dbid = os.environ["ASTRA_DB_DATABASE_ID"]
        tok = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
        kys = os.environ["ASTRA_DB_KEYSPACE"]
        cassio.init(database_id=dbid, token=tok, keyspace=kys)
        assert resolve_session() is not None
        assert resolve_keyspace() is not None

    @pytest.mark.skipif(
        os.environ.get("ASTRA_DB_DATABASE_ID") is None,
        reason="requires the database ID to download the secure bundle",
    )
    def test_init_download_scb_url_template(self) -> None:
        _reset_cassio_globals()
        dbid = os.environ["ASTRA_DB_DATABASE_ID"]
        tok = os.environ["ASTRA_DB_APPLICATION_TOKEN"]
        kys = os.environ["ASTRA_DB_KEYSPACE"]
        with pytest.raises(Exception):
            cassio.init(
                database_id=dbid,
                token=tok,
                keyspace=kys,
                bundle_url_template="https://kittens/{database_id}/meow",
            )
        cassio.init(
            database_id=dbid,
            token=tok,
            keyspace=kys,
            bundle_url_template="https://api.astra.datastax.com/v2/databases/{database_id}/secureBundleURL",
        )
        assert resolve_session() is not None
        assert resolve_keyspace() is not None

    @pytest.mark.skipif(
        os.environ.get("ASTRA_DB_INIT_STRING") is None,
        reason="requires the init-string available as environment variable",
    )
    def test_init_init_string(self) -> None:
        _reset_cassio_globals()
        inst = os.environ["ASTRA_DB_INIT_STRING"]
        cassio.init(init_string=inst)
        assert resolve_session() is not None
        assert resolve_keyspace() is not None

    @pytest.mark.skipif(
        os.environ.get("ASTRA_DB_INIT_STRING") is None,
        reason="requires the init-string available as environment variable",
    )
    def test_init_init_string_overwrite_ks(self) -> None:
        _reset_cassio_globals()
        inst = os.environ["ASTRA_DB_INIT_STRING"]
        cassio.init(init_string=inst, keyspace="my_keyspace")
        assert resolve_session() is not None
        assert resolve_keyspace() == "my_keyspace"

    @pytest.mark.skipif(
        os.environ.get("ASTRA_DB_INIT_STRING") is None,
        reason="requires the init-string available as environment variable",
    )
    def test_init_init_string_overwrite_tk(self) -> None:
        _reset_cassio_globals()
        inst = os.environ["ASTRA_DB_INIT_STRING"]
        with pytest.raises(NoHostAvailable):
            cassio.init(init_string=inst, token="AstraCS:wrong")

    def test_init_auto(self) -> None:
        _reset_cassio_globals()
        stolen = _freeze_envvars(
            [
                "CASSANDRA_CONTACT_POINTS",
                "CASSANDRA_USERNAME",
                "CASSANDRA_PASSWORD",
                "CASSANDRA_KEYSPACE",
            ]
        )
        cassio.init(auto=True)
        assert resolve_session() is not None
        assert resolve_keyspace() == os.environ.get("ASTRA_DB_KEYSPACE")
        assert resolve_session("s") == "s"
        assert resolve_keyspace("k") == "k"
        _unfreeze_envvars(stolen)
