"""
Just importing everything to smoke-test python3.8+ syntax issues, etc.
TODO: make this more robust in making sure all code is imported.
"""


class TestImports:
    def test_import_db_extractor(self):
        from cassio.db_reader import CassandraExtractor  # type: ignore  # noqa: F401

    def test_import_history(self):
        from cassio.history import StoredBlobHistory  # type: ignore  # noqa: F401

    def test_import_keyvalue(self):
        from cassio.keyvalue import KVCache  # type: ignore  # noqa: F401

    def test_import_table(self):
        from cassio.table.tables import PlainCassandraTable  # noqa: F401

    def test_import_utils(self):
        from cassio.utils import distance_metrics  # noqa: F401

    def test_import_vector(self):
        from cassio.vector import VectorTable  # noqa: F401
