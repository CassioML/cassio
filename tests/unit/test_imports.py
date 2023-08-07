"""
Just importing everything to smoke-test python3.8+ syntax issues, etc.
TODO: make this more robust in making sure all code is imported.
"""

import pytest


class TestImports:
    def test_import_cql(self):
        from cassio.cql import create_vector_table  # type: ignore

    def test_import_db_extractor(self):
        from cassio.db_extractor import CassandraExtractor  # type: ignore

    def test_import_history(self):
        from cassio.history import StoredBlobHistory  # type: ignore

    def test_import_keyvalue(self):
        from cassio.keyvalue import KVCache  # type: ignore

    def test_import_table(self):
        from cassio.table.tables import PlainCassandraTable

    def test_import_utils(self):
        from cassio.utils import distance_metrics

    def test_import_vector(self):
        from cassio.vector import VectorTable
