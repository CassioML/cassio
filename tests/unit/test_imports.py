"""
Just importing everything to smoke-test python3.8+ syntax issues, etc.
TODO: make this more robust in making sure all code is imported.
"""


class TestImports:
    def test_import_db_extractor(self) -> None:
        from cassio.db_reader import MultiTableCassandraReader  # noqa: F401

    def test_import_history(self) -> None:
        from cassio.history import StoredBlobHistory  # noqa: F401

    def test_import_keyvalue(self) -> None:
        from cassio.keyvalue import KVCache  # noqa: F401

    def test_import_table(self) -> None:
        from cassio.table.tables import PlainCassandraTable  # noqa: F401

    def test_import_utils(self) -> None:
        from cassio.utils import distance_metrics  # noqa: F401

    def test_import_vector(self) -> None:
        from cassio.vector import VectorTable  # noqa: F401
