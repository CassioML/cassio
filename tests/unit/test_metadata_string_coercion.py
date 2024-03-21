"""
Stringification of everything in the simple metadata handling
"""

from cassio.table.mixins import MetadataMixin


class TestNormalizeMetadataPolicy:
    def test_normalize_metadata_policy(self) -> None:
        md_mixin = MetadataMixin("s", "k", "t", skip_provisioning=True)

        stringified = md_mixin._split_metadata_fields(
            {
                "integer": 1,
                "float": 2.0,
                "boolean": True,
                "null": None,
                "string": "letter E",
                "something": RuntimeError("You cannot do this!"),
            }
        )

        expected = {
            "integer": "1.0",
            "float": "2.0",
            "boolean": "true",
            "null": "null",
            "string": "letter E",
            "something": str(RuntimeError("You cannot do this!")),
        }

        assert stringified == {"metadata_s": expected}
