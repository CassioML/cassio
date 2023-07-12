from typing import Any, Dict, List, Optional, Tuple, Union

ColumnSpecType = Tuple[str, str]
RowType = Any
SessionType = Any


def normalize_type_desc(type_desc: Union[str, List[str]]) -> List[str]:
    if isinstance(type_desc, str):
        return [type_desc]
    else:
        return type_desc


def rearrange_pk_type(
    pk_type: Union[str, List[str]],
    clustered: bool = False,
    num_elastic_keys: Optional[int] = None,
) -> Dict[str, List[str]]:
    """A compatibility layer with the 'primary_key_type' specifier on init."""
    _pk_type = normalize_type_desc(pk_type)
    if clustered:
        pk_type, rest_type = _pk_type[0:1], _pk_type[1:]
        if num_elastic_keys:
            assert len(rest_type) == num_elastic_keys
            return {
                "partition_id_type": pk_type,
            }
        else:
            return {
                "row_id_type": rest_type,
                "partition_id_type": pk_type,
            }
    else:
        if num_elastic_keys:
            assert len(_pk_type) == num_elastic_keys
            return {}
        else:
            return {
                "row_id_type": _pk_type,
            }
