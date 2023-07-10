from typing import Any, List, Dict, Protocol, Set, Tuple, Type

from cassio.table.table_types import ColumnSpecType, RowType, SessionType


class BaseTableMixinProtocol(Protocol):
    @classmethod
    def _schema_pk(cls) -> List[ColumnSpecType]:
        ...

    @classmethod
    def _schema_cc(cls) -> List[ColumnSpecType]:
        ...

    @classmethod
    def _schema_da(cls) -> List[ColumnSpecType]:
        ...


class BaseTableMixin:
    """All other mixins should inherit from this one."""

    @classmethod
    def _schema_pk(cls: Type[BaseTableMixinProtocol]) -> List[ColumnSpecType]:
        return super()._schema_pk()

    @classmethod
    def _schema_cc(cls: Type[BaseTableMixinProtocol]) -> List[ColumnSpecType]:
        return super()._schema_cc()

    @classmethod
    def _schema_da(cls: Type[BaseTableMixinProtocol]) -> List[ColumnSpecType]:
        return super()._schema_da()


class ClusteredMixin(BaseTableMixin):
    @classmethod
    def _schema_pk(cls):
        return [
            "partition_id",
        ]

    @classmethod
    def _schema_cc(cls):
        return cls._schema_row_id()

    def delete_partition(self, partition_id):
        delete_p_cql = "DELETE_PARTITION: (partition_id)"
        delete_p_cql_vals = (partition_id,)
        self.execute_cql(delete_p_cql, delete_p_cql_vals)

    def get_partition(self, partition_id, n=None):
        if n is None:
            get_p_cql = "GET_PARTITION: (partition_id)"
            get_p_cql_vals = (partition_id,)
        else:
            get_p_cql = "GET_PARTITION: (partition_id) LIMIT (n)"
            get_p_cql_vals = (partition_id, n)
        self.execute_cql(get_p_cql, get_p_cql_vals)


class MetadataMixin(BaseTableMixin):
    @classmethod
    def _schema_da(cls):
        return super()._schema_da() + [
            "metadata_s",
            "metadata_n",
            "metadata_tags",
        ]

    def db_setup(self):
        super().db_setup()
        self.execute_cql("CREATE_METADATA_SAIs")

    def _split_metadata(self, md_dict):
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

    def put(self, /, **kwargs):
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
    @classmethod
    def _schema_da(cls):
        return super()._schema_da() + ["vector"]

    def db_setup(self):
        super().db_setup()
        self.execute_cql("CREATE_VECTOR_SAI")

    def ann_search(self, vector, **kwargs):
        raise NotImplementedError


class ElasticKeyMixin:
    def __init__(self, *pargs, keys, **kwargs):
        self.keys = keys
        self.key_desc = "/".join(self.keys)
        super().__init__(*pargs, **kwargs)

    @staticmethod
    def _serialize_key_vals(key_vals: List[str]):
        return str(key_vals)

    def _split_row_args(self, arg_dict):
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

    def delete(self, /, **kwargs):
        key_vals, other_kwargs = self._split_row_args(kwargs)
        super().delete(key_desc=self.key_desc, key_vals=key_vals, **other_kwargs)

    def get(self, /, **kwargs):
        key_vals, other_kwargs = self._split_row_args(kwargs)
        # TODO: unpack the key
        return super().get(key_desc=self.key_desc, key_vals=key_vals, **other_kwargs)

    def put(self, /, **kwargs):
        key_vals, other_kwargs = self._split_row_args(kwargs)
        super().put(key_desc=self.key_desc, key_vals=key_vals, **other_kwargs)

    @staticmethod
    def _schema_row_id():
        return [
            "key_desc",
            "key_vals",
        ]
