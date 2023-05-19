"""
Example test
"""

import unittest


class TestPlaceholder(unittest.TestCase):
    """
    Tests for ...
    """

    def test_divbyzero(self):
        """Illegal math"""
        with self.assertRaises(ZeroDivisionError):
            val = 1 / 0
