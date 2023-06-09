"""
Example test
"""

import pytest

import cassio
from cassio.vector import VectorDBTable

class TestExperimentalFlagForVectorSearch():
    """
    Tests for the deprecation of the 'experimental vector search' flag
    """

    def test_deprecation_when_setting_flag(self):
        with pytest.warns(DeprecationWarning):
            cassio.globals.enableExperimentalVectorSearch()

    def test_vectordbcreation_without_setting_flag(self):
        """sss"""
        cassio.globals._experimental_vector_search = False
        # "'NoneType' object has no attribute 'execute'", from the table creation cql
        with pytest.raises(AttributeError):
            _ = VectorDBTable(None, None, None, None, None)
