"""
Query/predicates 'grammar' unit test
"""

import pytest

from cassio.table.query import PredicateOperator, Predicate


class TestQueryPredicates:
    def test_predicate_create_and_render(self) -> None:
        pred1 = Predicate('<', 12)
        pred2 = Predicate(PredicateOperator.GTE, "yoo")

        assert pred1.render() == ("<", 12)
        assert pred2.render() == (">=", "yoo")

    def test_predicate_wrong_operator(self) -> None:
        with pytest.raises(ValueError):
            Predicate("==", "boh")

