from __future__ import annotations

from enum import Enum
from typing import Any, Tuple, Union


class PredicateOperator(Enum):
    EQ = "="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="


class Predicate:
    operator: PredicateOperator
    value: Any

    def __init__(self, operator: Union[str, PredicateOperator], value: Any) -> None:
        if isinstance(operator, str):
            _operator = PredicateOperator(operator)
        else:
            _operator = operator
        self._operator = _operator
        self._value = value

    def render(self) -> Tuple[str, Any]:
        return (
            self._operator.value,
            self._value,
        )
