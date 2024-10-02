from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from cassandra.cluster import ResponseFuture

from cassio.table.cql import DELETE_CQL_TEMPLATE, SELECT_CQL_TEMPLATE, CQLOpType
from cassio.table.table_types import ColumnSpecType, RowType, normalize_type_desc
from cassio.table.utils import (
    call_wrapped_async,
    handle_multicolumn_packing,
    handle_multicolumn_unpacking,
)

from .base_table import BaseTableMixin

PARTITION_ID_TYPE = Union[Any, Tuple[Any]]


class ClusteredMixin(BaseTableMixin):
    def __init__(
        self,
        *pargs: Any,
        partition_id_type: Union[str, List[str]] = ["TEXT"],
        partition_id: Optional[PARTITION_ID_TYPE] = None,
        ordering_in_partition: Union[str, List[str]] = "ASC",
        **kwargs: Any,
    ) -> None:
        self.partition_id_type = normalize_type_desc(partition_id_type)
        self.partition_id = partition_id
        if isinstance(ordering_in_partition, str):
            self.ordering_in_partition = ordering_in_partition.upper()
        else:
            self.ordering_in_partition = [
                ordering.upper() for ordering in ordering_in_partition
            ]
        super().__init__(*pargs, **kwargs)

    def _schema_pk(self) -> List[ColumnSpecType]:
        if len(self.partition_id_type) == 1:
            return [
                ("partition_id", self.partition_id_type[0]),
            ]
        else:
            return [
                (f"partition_id_{pk_i}", pk_typ)
                for pk_i, pk_typ in enumerate(self.partition_id_type)
            ]

    def _schema_cc(self) -> List[ColumnSpecType]:
        return self._schema_row_id()

    def _delete_partition(
        self, is_async: bool, partition_id: Optional[PARTITION_ID_TYPE] = None
    ) -> Union[None, ResponseFuture]:
        _partition_id = self.partition_id if partition_id is None else partition_id
        #
        _pid_dict = handle_multicolumn_unpacking(
            {"partition_id": _partition_id},
            "partition_id",
            [col for col, _ in self._schema_pk()],
        )
        (
            rest_kwargs,
            where_clause_blocks,
            delete_cql_vals,
        ) = self._extract_where_clause_blocks(_pid_dict)
        assert rest_kwargs == {}
        where_clause = "WHERE " + " AND ".join(where_clause_blocks)
        delete_cql = DELETE_CQL_TEMPLATE.format(
            where_clause=where_clause,
        )
        if is_async:
            return self.execute_cql_async(
                delete_cql, args=delete_cql_vals, op_type=CQLOpType.WRITE
            )
        else:
            self.execute_cql(delete_cql, args=delete_cql_vals, op_type=CQLOpType.WRITE)
            return None

    def delete_partition(
        self, partition_id: Optional[PARTITION_ID_TYPE] = None
    ) -> None:
        self._delete_partition(is_async=False, partition_id=partition_id)
        return None

    def delete_partition_async(
        self, partition_id: Optional[PARTITION_ID_TYPE] = None
    ) -> ResponseFuture:
        return self._delete_partition(is_async=True, partition_id=partition_id)

    async def adelete_partition(
        self, partition_id: Optional[PARTITION_ID_TYPE] = None
    ) -> None:
        await call_wrapped_async(self.delete_partition_async, partition_id=partition_id)

    def _normalize_kwargs(
        self, args_dict: Dict[str, Any], is_write: bool
    ) -> Dict[str, Any]:
        # if partition id provided in call, takes precedence over instance value
        arg_pid = args_dict.get("partition_id")
        instance_pid = self.partition_id
        _partition_id = instance_pid if arg_pid is None else arg_pid
        new_args_dict0 = {
            **{"partition_id": _partition_id},
            **args_dict,
        }
        # in case of multicolumn-key schema, do the tuple unpacking:
        new_args_dict = handle_multicolumn_unpacking(
            new_args_dict0,
            "partition_id",
            [col for col, _ in self._schema_pk()],
        )

        return super()._normalize_kwargs(new_args_dict, is_write=is_write)

    def _normalize_row(self, raw_row: Any) -> Dict[str, Any]:
        pre_normalized = super()._normalize_row(raw_row)
        repacked_row = handle_multicolumn_packing(
            unpacked_row=pre_normalized,
            key_name="partition_id",
            unpacked_keys=[col for col, _ in self._schema_pk()],
        )
        return repacked_row

    def _get_get_partition_cql(
        self,
        partition_id: Optional[PARTITION_ID_TYPE] = None,
        n: Optional[int] = None,
        **kwargs: Any,
    ) -> Tuple[str, Tuple[Any, ...]]:
        _partition_id = self.partition_id if partition_id is None else partition_id
        #
        # TODO: work on a columns: Optional[List[str]] = None
        # (but with nuanced handling of the column-magic we have here)
        columns = None
        if columns is None:
            columns_desc = "*"
        else:
            # TODO: handle translations here?
            # columns_desc = ", ".join(columns)
            raise NotImplementedError("Column selection is not implemented.")
        # WHERE can admit other sources (e.g. medata if the corresponding mixin)
        # so we escalate to standard WHERE-creation route and reinject the partition
        n_kwargs = self._normalize_kwargs(
            {
                **{"partition_id": _partition_id},
                **kwargs,
            },
            is_write=False,
        )
        (
            rest_kwargs,
            where_clause_blocks,
            select_cql_vals,
        ) = self._extract_where_clause_blocks(n_kwargs)

        # check for exhaustion:
        assert rest_kwargs == {}
        where_clause = "WHERE " + " AND ".join(where_clause_blocks)
        where_cql_vals = list(select_cql_vals)
        #
        if n is None:
            limit_clause = ""
            limit_cql_vals = []
        else:
            limit_clause = "LIMIT %s"
            limit_cql_vals = [n]
        #
        select_cql = SELECT_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        get_p_cql_vals = tuple(where_cql_vals + limit_cql_vals)
        return select_cql, get_p_cql_vals

    def get_partition(
        self,
        partition_id: Optional[PARTITION_ID_TYPE] = None,
        n: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterable[RowType]:
        select_cql, get_p_cql_vals = self._get_get_partition_cql(
            partition_id, n, **kwargs
        )
        return (
            self._normalize_row(raw_row)
            for raw_row in self.execute_cql(
                select_cql,
                args=get_p_cql_vals,
                op_type=CQLOpType.READ,
            )
        )

    def get_partition_async(
        self,
        partition_id: Optional[PARTITION_ID_TYPE] = None,
        n: Optional[int] = None,
        **kwargs: Any,
    ) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")

    async def aget_partition(
        self,
        partition_id: Optional[PARTITION_ID_TYPE] = None,
        n: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterable[RowType]:
        select_cql, get_p_cql_vals = self._get_get_partition_cql(
            partition_id, n, **kwargs
        )
        return (
            self._normalize_row(raw_row)
            for raw_row in await self.aexecute_cql(
                select_cql,
                args=get_p_cql_vals,
                op_type=CQLOpType.READ,
            )
        )
