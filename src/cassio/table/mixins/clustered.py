from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from cassandra.cluster import ResponseFuture

from cassio.table.cql import DELETE_CQL_TEMPLATE, SELECT_CQL_TEMPLATE, CQLOpType
from .base_table import BaseTableMixin
from cassio.table.table_types import ColumnSpecType, RowType, normalize_type_desc
from cassio.table.utils import call_wrapped_async


class ClusteredMixin(BaseTableMixin):
    def __init__(
        self,
        *pargs: Any,
        partition_id_type: Union[str, List[str]] = ["TEXT"],
        partition_id: Optional[Any] = None,
        ordering_in_partition: str = "ASC",
        **kwargs: Any,
    ) -> None:
        self.partition_id_type = normalize_type_desc(partition_id_type)
        self.partition_id = partition_id
        self.ordering_in_partition = ordering_in_partition.upper()
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

    def _allowed_colnames(self) -> List[str]:
        names = super()._allowed_colnames()
        names.append("partition_id")
        return names

    # def _extract_where_clause_blocks(
    #     self, args_dict: Any
    # ) -> Tuple[Any, List[str], Tuple[Any, ...]]:
    #     print(f"clustered._extract_where_clause_blocks() args_dict: {args_dict}")
    #     """
    #     If a null partition_id or row_id arrives to WHERE construction, it is silently
    #     discarded from the set of conditions to create.
    #     This enables e.g. ANN vector search across partitions of a clustered table.

    #     It is the database's responsibility to raise an error if unacceptable.
    #     """
    #     these_wc_blocks: List[str] = []
    #     these_wc_vals_list: List[Any] = []
    #     for col in ["row_id", "partition_id"]:
    #         if col not in args_dict:
    #             continue

    #         value = args_dict[col]
    #         del args_dict[col]
    #         if value is None:
    #             continue

    #         if not isinstance(value, Tuple):
    #             value = (value,)

    #         if len(value) == 1:
    #             these_wc_blocks.append(f"{col} = %s")
    #             these_wc_vals_list.append(value[0])
    #         else:
    #             for i, v in enumerate(value):
    #                 these_wc_blocks.append(f"{col}_{i} = %s")
    #                 these_wc_vals_list.append(v)

    #     # no new kwargs keys are created, all goes to WHERE
    #     this_args_dict: Dict[str, Any] = {}
    #     these_wc_vals = tuple(these_wc_vals_list)
    #     # ready to defer to superclass(es), then collate-and-return
    #     (s_args_dict, s_wc_blocks, s_wc_vals) = super()._extract_where_clause_blocks(
    #         args_dict
    #     )
    #     return (
    #         {**s_args_dict, **this_args_dict},
    #          s_wc_blocks + these_wc_blocks,
    #         tuple(list(s_wc_vals) + list(these_wc_vals)),
    #     )

    def _delete_partition(
        self, is_async: bool, partition_id: Optional[str] = None
    ) -> Union[None, ResponseFuture]:
        _partition_id = self.partition_id if partition_id is None else partition_id
        #
        where_clause = "WHERE " + "partition_id = %s"
        delete_cql_vals = (_partition_id,)
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

    def delete_partition(self, partition_id: Optional[str] = None) -> None:
        self._delete_partition(is_async=False, partition_id=partition_id)
        return None

    def delete_partition_async(
        self, partition_id: Optional[str] = None
    ) -> ResponseFuture:
        return self._delete_partition(is_async=True, partition_id=partition_id)

    async def adelete_partition(self, partition_id: Optional[str] = None) -> None:
        await call_wrapped_async(self.delete_partition_async, partition_id=partition_id)

    def _normalize_kwargs(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        # if partition id provided in call, takes precedence over instance value
        print(f"clustered._normalize_kwargs() args_dict: {args_dict}")
        arg_pid = args_dict.get("partition_id")
        instance_pid = self.partition_id
        _partition_id = instance_pid if arg_pid is None else arg_pid
        new_args_dict = {
            **{"partition_id": _partition_id},
            **args_dict,
        }

        for col in ["row_id", "partition_id"]:
            if col not in new_args_dict:
                continue

            value = new_args_dict[col]
            del new_args_dict[col]

            #TODO: maybe don't do this?
            if value is None:
                continue

            if not isinstance(value, Tuple):
                value = (value,)

            if len(value) == 1:
                new_args_dict[col] = value[0]
            else:
                for i, v in enumerate(value):
                    new_args_dict[f"{col}_{i}"] = v

        return super()._normalize_kwargs(new_args_dict)

    def _get_get_partition_cql(
        self, partition_id: Optional[str] = None, n: Optional[int] = None, **kwargs: Any
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
            }
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
        self, partition_id: Optional[str] = None, n: Optional[int] = None, **kwargs: Any
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
        self, partition_id: Optional[str] = None, n: Optional[int] = None, **kwargs: Any
    ) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")

    async def aget_partition(
        self, partition_id: Optional[str] = None, n: Optional[int] = None, **kwargs: Any
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
