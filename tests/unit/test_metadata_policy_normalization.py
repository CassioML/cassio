"""
Normalization of metadata policy specification options
"""

from cassio.table.table_types import (
    MetadataIndexingMode,
)

from cassio.table.mixins import MetadataMixin


class TestNormalizeMetadataPolicy:
    def test_normalize_metadata_policy(self):
        #
        mdp1 = MetadataMixin._normalize_metadata_indexing_policy("all")
        assert mdp1 == (MetadataIndexingMode.DEFAULT_TO_SEARCHABLE, set())
        #
        mdp2 = MetadataMixin._normalize_metadata_indexing_policy("none")
        assert mdp2 == (MetadataIndexingMode.DEFAULT_TO_UNSEARCHABLE, set())
        #
        mdp3 = MetadataMixin._normalize_metadata_indexing_policy(
            ("default_to_Unsearchable", ["x", "y"]),
        )
        assert mdp3 == (MetadataIndexingMode.DEFAULT_TO_UNSEARCHABLE, {"x", "y"})
        #
        mdp4 = MetadataMixin._normalize_metadata_indexing_policy(
            ("DenyList", ["z"]),
        )
        assert mdp4 == (MetadataIndexingMode.DEFAULT_TO_SEARCHABLE, {"z"})
        # s
        mdp5 = MetadataMixin._normalize_metadata_indexing_policy(
            ("deny_LIST", "singlefield")
        )
        assert mdp5 == (MetadataIndexingMode.DEFAULT_TO_SEARCHABLE, {"singlefield"})
