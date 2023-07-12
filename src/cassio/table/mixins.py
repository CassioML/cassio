from typing import Any, List, Dict, Iterable, Optional, Protocol, Set, Tuple, Type

from cassio.table.table_types import ColumnSpecType, RowType, SessionType
from cassio.table.base_table import BaseTable


class BaseTableMixin(BaseTable):
    """All other mixins should inherit from this one."""

    pass


class ClusteredMixin(BaseTableMixin):

    def _schema_pk(self) -> List[ColumnSpecType]:
        return [
            ("partition_id", "TEXT"),
        ]

    def _schema_cc(self) -> List[ColumnSpecType]:
        return self._schema_row_id()

    def delete_partition(self, partition_id: str) -> None:
        delete_p_cql = "DELETE_PARTITION: (partition_id)"
        delete_p_cql_vals = (partition_id,)
        self.execute_cql(delete_p_cql, delete_p_cql_vals)
        return

    def get_partition(
        self, partition_id: str, n: Optional[int] = None
    ) -> Iterable[RowType]:
        get_p_cql_vals: Tuple[Any, ...] = tuple()
        if n is None:
            get_p_cql = "GET_PARTITION: (partition_id)"
            get_p_cql_vals = (partition_id,)
        else:
            get_p_cql = "GET_PARTITION: (partition_id) LIMIT (n)"
            get_p_cql_vals = (partition_id, n)
        return self.execute_cql(get_p_cql, get_p_cql_vals)


class MetadataMixin(BaseTableMixin):

    def _schema_da(self) -> List[ColumnSpecType]:
        return super()._schema_da() + [
            ("metadata_s", "MAP<TEXT,TEXT>"),
            ("metadata_n", "MAP<TEXT,FLOAT>"),
            ("metadata_tags", "SET<TEXT>"),
        ]

    def db_setup(self) -> None:
        super().db_setup()
        self.execute_cql("CREATE_METADATA_SAIs")

    def _split_metadata(self, md_dict: Dict[str, Any]) -> Dict[str, Any]:
        # TODO: more care about types here
        stringy_part = {k: v for k, v in md_dict.items() if isinstance(v, str)}
        numeric_part = {
            k: float(v)
            for k, v in md_dict.items()
            if isinstance(v, int) or isinstance(v, float)
            if not isinstance(v, bool)
        }
        # these become 'tags'
        nully_part = {
            k for k, v in md_dict.items() if isinstance(v, bool) and v is True
        }
        assert set(stringy_part.keys()) | set(numeric_part.keys()) | nully_part == set(
            md_dict.keys()
        )
        assert len(stringy_part.keys()) + len(numeric_part.keys()) + len(
            nully_part
        ) == len(md_dict.keys())
        return {
            "metadata_s": stringy_part,
            "metadata_n": numeric_part,
            "metadata_tags": nully_part,
        }

    def put(self, /, **kwargs: Any):
        if "metadata" in kwargs:
            new_metadata_fields = self._split_metadata(kwargs["metadata"])
        else:
            new_metadata_fields = {}
        #
        new_kwargs = {
            **{k: v for k, v in kwargs.items() if k != "metadata"},
            **new_metadata_fields,
        }
        #
        super().put(**new_kwargs)


class VectorMixin(BaseTableMixin):

    def _schema_da(self) -> List[ColumnSpecType]:
        return super()._schema_da() + [("vector", "VECTOR<FLOAT,999>")]

    def db_setup(self) -> None:
        super().db_setup()
        self.execute_cql("CREATE_VECTOR_SAI")

    def ann_search(self, vector: List[float], **kwargs: Any) -> Iterable[RowType]:
        raise NotImplementedError


class ElasticKeyMixin(BaseTableMixin):
    def __init__(self, *pargs: Any, keys: List[str], **kwargs: Any) -> None:
        self.keys = keys
        self.key_desc = "/".join(self.keys)
        super().__init__(*pargs, **kwargs)

    @staticmethod
    def _serialize_key_vals(key_vals: List[Any]) -> str:
        return str(key_vals)

    def _split_row_args(self, arg_dict: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        # split in key/nonkey from a kwargs dict
        # and represent the former as one field
        key_args = {k: v for k, v in arg_dict.items() if k in self.keys}
        assert set(key_args.keys()) == set(self.keys)
        key_vals = self._serialize_key_vals(
            [key_args[key_col] for key_col in self.keys]
        )
        #
        other_kwargs = {k: v for k, v in arg_dict.items() if k not in self.keys}
        return key_vals, other_kwargs

    def delete(self, /, **kwargs: Any) -> None:
        key_vals, other_kwargs = self._split_row_args(kwargs)
        super().delete(key_desc=self.key_desc, key_vals=key_vals, **other_kwargs)

    def get(self, /, **kwargs: Any) -> RowType:
        key_vals, other_kwargs = self._split_row_args(kwargs)
        # TODO: unpack the key
        return super().get(key_desc=self.key_desc, key_vals=key_vals, **other_kwargs)

    def put(self, /, **kwargs: Any) -> None:
        key_vals, other_kwargs = self._split_row_args(kwargs)
        super().put(key_desc=self.key_desc, key_vals=key_vals, **other_kwargs)

    @staticmethod
    def _schema_row_id() -> List[ColumnSpecType]:
        return [
            ("key_desc", "TEXT"),
            ("key_vals", "TEXT"),
        ]
