import json
from typing import Any, Dict, List, cast

from cassio.table.table_types import ColumnSpecType

from .base_table import BaseTableMixin


class ElasticKeyMixin(BaseTableMixin):
    def __init__(self, *pargs: Any, keys: List[str], **kwargs: Any) -> None:
        if "row_id_type" in kwargs:
            raise ValueError("'row_id_type' not allowed for elastic tables.")
        self.keys = keys
        self.key_desc = self._serialize_key_list(self.keys)
        row_id_type = ["TEXT", "TEXT"]
        new_kwargs = {
            **{"row_id_type": row_id_type},
            **kwargs,
        }
        super().__init__(*pargs, **new_kwargs)

    @staticmethod
    def _serialize_key_list(key_vals: List[Any]) -> str:
        return json.dumps(key_vals, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _deserialize_key_list(keys_str: str) -> List[Any]:
        return cast(List[Any], json.loads(keys_str))

    def _normalize_row(self, raw_row: Any) -> Dict[str, Any]:
        pre_normalized = super()._normalize_row(raw_row)
        # after BaseTable unpacks this, pre_normalized contains
        # {"row_id": (serialized_keys, serialized_values), ...}
        if "row_id" in pre_normalized:
            keys_s, vals_s = pre_normalized["row_id"]
            keys = self._deserialize_key_list(keys_s)
            vals = self._deserialize_key_list(vals_s)
            assert keys == self.keys
            assert len(keys) == len(vals)
            restored_keys = {k: v for k, v in zip(keys, vals)}
            return {
                **restored_keys,
                **{k: v for k, v in pre_normalized.items() if k != "row_id"},
            }
        else:
            return pre_normalized

    def _normalize_kwargs(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        # transform provided "keys" into the elastic-representation two-val form
        key_args = {k: v for k, v in args_dict.items() if k in self.keys}
        # the "key" is passed all-or-nothing:
        assert set(key_args.keys()) == set(self.keys) or key_args == {}
        if key_args != {}:
            key_vals = self._serialize_key_list(
                [key_args[key_col] for key_col in self.keys]
            )
            #
            key_args_dict = {
                "key_vals": key_vals,
                "key_desc": self.key_desc,
            }
            other_args_dict = {k: v for k, v in args_dict.items() if k not in self.keys}
            new_args_dict = {
                **key_args_dict,
                **other_args_dict,
            }
        else:
            new_args_dict = args_dict
        return super()._normalize_kwargs(new_args_dict)

    @staticmethod
    def _schema_row_id() -> List[ColumnSpecType]:
        return [
            ("key_desc", "TEXT"),
            ("key_vals", "TEXT"),
        ]
