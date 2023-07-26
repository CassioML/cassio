import json

from typing import (
    Any,
    List,
    Dict,
    Iterable,
    Optional,
    Protocol,
    Set,
    Tuple,
    Type,
    Union,
)

from cassandra.cluster import ResultSet  # type: ignore

from cassio.table.cql import (
    CQLOpType,
    DELETE_CQL_TEMPLATE,
    SELECT_CQL_TEMPLATE,
    CREATE_INDEX_CQL_TEMPLATE,
    # CREATE_KEYS_INDEX_CQL_TEMPLATE,
    CREATE_ENTRIES_INDEX_CQL_TEMPLATE,
    SELECT_ANN_CQL_TEMPLATE,
)
from cassio.table.table_types import (
    ColumnSpecType,
    RowType,
    SessionType,
    normalize_type_desc,
    rearrange_pk_type,
)
from cassio.table.base_table import BaseTable


class BaseTableMixin(BaseTable):
    """All other mixins should inherit from this one."""

    pass


class ClusteredMixin(BaseTableMixin):
    def __init__(
        self,
        *pargs: Any,
        partition_id_type: Union[str, List[str]] = ["TEXT"],
        partition_id: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        self.partition_id_type = normalize_type_desc(partition_id_type)
        self.partition_id = partition_id
        super().__init__(*pargs, **kwargs)

    def _schema_pk(self) -> List[ColumnSpecType]:
        assert len(self.partition_id_type) == 1
        return [
            ("partition_id", self.partition_id_type[0]),
        ]

    def _schema_cc(self) -> List[ColumnSpecType]:
        return self._schema_row_id()

    def delete_partition(self, partition_id: Optional[str] = None) -> None:
        _partition_id = self.partition_id if partition_id is None else partition_id
        #
        where_clause = "WHERE " + "partition_id = %s"
        delete_cql_vals = (_partition_id,)
        delete_cql = DELETE_CQL_TEMPLATE.format(
            where_clause=where_clause,
        )
        self.execute_cql(delete_cql, args=delete_cql_vals, op_type=CQLOpType.WRITE)
        return

    def _normalize_kwargs(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        # if partition id provided in call, takes precedence over instance value
        arg_pid = args_dict.get("partition_id")
        instance_pid = self.partition_id
        _partition_id = instance_pid if arg_pid is None else arg_pid
        new_args_dict = {
            **{"partition_id": _partition_id},
            **args_dict,
        }
        return super()._normalize_kwargs(new_args_dict)

    def get_partition(
        self, partition_id: Optional[str] = None, n: Optional[int] = None, **kwargs: Any
    ) -> Iterable[RowType]:
        _partition_id = self.partition_id if partition_id is None else partition_id
        get_p_cql_vals: Tuple[Any, ...] = tuple()
        #
        # TODO: work on a columns: Optional[List[str]] = None
        # (but with nuanced handling of the column-magic we have here)
        columns = None
        if columns is None:
            columns_desc = "*"
        else:
            # TODO: handle translations here?
            # columns_desc = ", ".join(columns)
            raise NotImplementedError
        # WHERE can admit other sources (e.g. medata if the corresponding mixin)
        # so we escalate to standard WHERE-creation route and reinject the partition
        n_kwargs = self._normalize_kwargs(
            {
                **{"partition_id": _partition_id},
                **kwargs,
            }
        )
        (args_dict, wc_blocks, wc_vals) = self._extract_where_clause_blocks(n_kwargs)
        # check for exhaustion:
        assert args_dict == {}
        where_clause = "WHERE " + " AND ".join(wc_blocks)
        where_cql_vals = list(wc_vals)
        #
        if n is None:
            limit_clause = ""
            limit_cql_vals = []
        else:
            limit_clause = f"LIMIT %s"
            limit_cql_vals = [n]
        #
        select_cql = SELECT_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        get_p_cql_vals = tuple(where_cql_vals + limit_cql_vals)
        return self.execute_cql(select_cql, args=get_p_cql_vals, op_type=CQLOpType.READ)


class MetadataMixin(BaseTableMixin):
    def _schema_da(self) -> List[ColumnSpecType]:
        return super()._schema_da() + [
            ("metadata_s", "MAP<TEXT,TEXT>"),
            ("metadata_n", "MAP<TEXT,FLOAT>"),
            ("metadata_tags", "SET<TEXT>"),
        ]

    def db_setup(self) -> None:
        # Currently this supports entries on:
        # the two entryful metadata parts + just-existence on the tags (set).
        # No indexes on key existence are created.
        super().db_setup()
        #
        entries_index_columns = ["metadata_s", "metadata_n"]
        index_columns = ["metadata_tags"]
        for index_column in index_columns:
            index_name = f"idx_{index_column}"
            index_column = f"{index_column}"
            create_index_cql = CREATE_INDEX_CQL_TEMPLATE.format(
                index_name=index_name,
                index_column=index_column,
            )
            self.execute_cql(create_index_cql, op_type=CQLOpType.SCHEMA)
        for entries_index_column in entries_index_columns:
            index_name = f"eidx_{entries_index_column}"
            index_column = f"{entries_index_column}"
            create_index_cql = CREATE_ENTRIES_INDEX_CQL_TEMPLATE.format(
                index_name=index_name,
                index_column=index_column,
            )
            self.execute_cql(create_index_cql, op_type=CQLOpType.SCHEMA)
        #
        return

    def _split_metadata_fields(self, md_dict: Dict[str, Any]) -> Dict[str, Any]:
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
        assert {
            k for k, v in md_dict.items() if isinstance(v, bool) and v is False
        } == set()
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

    def _normalize_row(self, raw_row: Any) -> Dict[str, Any]:
        md_columns_defaults: Dict[str, Any] = {
            "metadata_s": {},
            "metadata_n": {},
            "metadata_tags": set(),
        }
        pre_normalized = super()._normalize_row(raw_row)
        row_rest = {
            k: v for k, v in pre_normalized.items() if k not in md_columns_defaults
        }
        #
        mergee_md_fields = {
            k: v for k, v in pre_normalized.items() if k in md_columns_defaults
        }
        normalized_mergee_md_fields = {
            k: v if v is not None else md_columns_defaults[k]
            for k, v in mergee_md_fields.items()
        }
        r_md_from_tags = {
            tag: True for tag in normalized_mergee_md_fields["metadata_tags"]
        }
        r_md_from_n = {
            k: v for k, v in normalized_mergee_md_fields["metadata_n"].items()
        }
        r_md_from_s = {
            k: v for k, v in normalized_mergee_md_fields["metadata_s"].items()
        }
        #
        row_metadata = {
            "metadata": {
                **r_md_from_tags,
                **r_md_from_n,
                **r_md_from_s,
            },
        }
        #
        normalized = {
            **row_metadata,
            **row_rest,
        }
        return normalized

    def _normalize_kwargs(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        if "metadata" in args_dict:
            new_metadata_fields = {
                k: v
                for k, v in self._split_metadata_fields(args_dict["metadata"]).items()
                if v != {}
            }
        else:
            new_metadata_fields = {}
        #
        new_args_dict = {
            **{k: v for k, v in args_dict.items() if k != "metadata"},
            **new_metadata_fields,
        }
        return super()._normalize_kwargs(new_args_dict)

    def _extract_where_clause_blocks(
        self, args_dict: Any
    ) -> Tuple[Any, List[str], Tuple[Any, ...]]:
        # This always happens after a corresponding _normalize_kwargs,
        # so the metadata, if present, appears as split-fields.
        assert "metadata" not in args_dict
        md_keys = {"metadata_s", "metadata_n", "metadata_tags"}
        new_args_dict = {k: v for k, v in args_dict.items() if k not in md_keys}
        # Here the "metadata" entry is made into specific where clauses
        split_metadata = {k: v for k, v in args_dict.items() if k in md_keys}
        these_wc_blocks: List[str] = []
        these_wc_vals_list: List[Any] = []
        # WHERE creation:
        for v in sorted(split_metadata.get("metadata_tags", set())):
            these_wc_blocks.append(f"metadata_tags CONTAINS %s")
            these_wc_vals_list.append(v)
        for k, v in sorted(split_metadata.get("metadata_s", {}).items()):
            these_wc_blocks.append(f"metadata_s['{k}'] = %s")
            these_wc_vals_list.append(v)
        for k, v in sorted(split_metadata.get("metadata_n", {}).items()):
            these_wc_blocks.append(f"metadata_n['{k}'] = %s")
            these_wc_vals_list.append(v)
        # no new kwargs keys are created, all goes to WHERE
        this_args_dict: Dict[str, Any] = {}
        these_wc_vals = tuple(these_wc_vals_list)
        # ready to defer to superclass(es), then collate-and-return
        (s_args_dict, s_wc_blocks, s_wc_vals) = super()._extract_where_clause_blocks(
            new_args_dict
        )
        return (
            {**this_args_dict, **s_args_dict},
            these_wc_blocks + s_wc_blocks,
            tuple(list(these_wc_vals) + list(s_wc_vals)),
        )

    def search(self, n: int, **kwargs: Any) -> RowType:
        columns_desc, where_clause, get_cql_vals = self._parse_select_core_params(
            **kwargs
        )
        limit_clause = f"LIMIT %s"
        limit_cql_vals = [n]
        select_vals = tuple(list(get_cql_vals) + limit_cql_vals)
        #
        select_cql = SELECT_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        result_set = self.execute_cql(
            select_cql, args=select_vals, op_type=CQLOpType.READ
        )
        return (self._normalize_row(result) for result in result_set)


class VectorMixin(BaseTableMixin):
    def __init__(self, *pargs: Any, vector_dimension: int, **kwargs: Any) -> None:
        self.vector_dimension = vector_dimension
        super().__init__(*pargs, **kwargs)

    def _schema_da(self) -> List[ColumnSpecType]:
        return super()._schema_da() + [
            ("vector", f"VECTOR<FLOAT,{self.vector_dimension}>")
        ]

    def db_setup(self) -> None:
        super().db_setup()
        # index on the vector column:
        index_name = "idx_vector"
        index_column = "vector"
        create_index_cql = CREATE_INDEX_CQL_TEMPLATE.format(
            index_name=index_name,
            index_column=index_column,
        )
        self.execute_cql(create_index_cql, op_type=CQLOpType.SCHEMA)
        return

    def ann_search(
        self, vector: List[float], n: int, **kwargs: Any
    ) -> Iterable[RowType]:
        n_kwargs = self._normalize_kwargs(kwargs)
        # TODO: work on a columns: Optional[List[str]] = None
        # (but with nuanced handling of the column-magic we have here)
        columns = None
        if columns is None:
            columns_desc = "*"
        else:
            # TODO: handle translations here?
            # columns_desc = ", ".join(columns)
            raise NotImplementedError
        #
        vector_column = "vector"
        vector_cql_vals = [vector]
        #
        (
            rest_kwargs,
            where_clause_blocks,
            where_cql_vals,
        ) = self._extract_where_clause_blocks(n_kwargs)
        assert rest_kwargs == {}
        if where_clause_blocks == []:
            where_clause = ""
        else:
            where_clause = "WHERE " + " AND ".join(where_clause_blocks)
        #
        limit_clause = f"LIMIT %s"
        limit_cql_vals = [n]
        #
        select_ann_cql = SELECT_ANN_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            vector_column=vector_column,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        #
        select_ann_cql_vals = tuple(
            list(where_cql_vals) + vector_cql_vals + limit_cql_vals
        )
        result_set = self.execute_cql(
            select_ann_cql, args=select_ann_cql_vals, op_type=CQLOpType.READ
        )
        return (self._normalize_row(result) for result in result_set)


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
        return json.dumps(key_vals, separators=(",", ":"))

    @staticmethod
    def _deserialize_key_list(keys_str: str) -> List[Any]:
        return json.loads(keys_str)

    def _normalize_row(self, raw_row: Any) -> Dict[str, Any]:
        key_fields = {"key_desc", "key_vals"}
        pre_normalized = super()._normalize_row(raw_row)
        row_key = {k: v for k, v in pre_normalized.items() if k in key_fields}
        row_rest = {k: v for k, v in pre_normalized.items() if k not in key_fields}
        if row_key == {}:
            key_dict = {}
        else:
            # unpack the keys
            assert len(row_key) == 2
            assert self._deserialize_key_list(row_key["key_desc"]) == self.keys
            key_dict = {
                k: v
                for k, v in zip(
                    self.keys,
                    self._deserialize_key_list(row_key["key_vals"]),
                )
            }
        return {
            **key_dict,
            **row_rest,
        }

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


class TypeNormalizerMixin(BaseTableMixin):

    clustered: bool = False
    elastic: bool = False

    def __init__(self, *pargs: Any, **kwargs: Any) -> None:
        if "primary_key_type" in kwargs:
            pk_arg = kwargs["primary_key_type"]
            num_elastic_keys = len(kwargs["keys"]) if self.elastic else None
            col_type_map = rearrange_pk_type(pk_arg, self.clustered, num_elastic_keys)
            new_kwargs = {
                **col_type_map,
                **{k: v for k, v in kwargs.items() if k != "primary_key_type"},
            }
        else:
            new_kwargs = kwargs
        super().__init__(*pargs, **new_kwargs)
