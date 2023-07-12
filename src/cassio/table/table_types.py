from typing import Any, List, Tuple, Union

ColumnSpecType = Tuple[str, str]
RowType = Any
SessionType = Any


def normalize_type_desc(type_desc: Union[str, List[str]]) -> List[str]:
    if isinstance(type_desc, str):
        return [type_desc]
    else:
        return type_desc
