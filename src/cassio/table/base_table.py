import asyncio
import json
import logging
from asyncio import InvalidStateError, Task
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union, cast

from cassandra.cluster import ResponseFuture, ResultSet
from cassandra.query import PreparedStatement, SimpleStatement

from cassio.config import check_resolve_keyspace, check_resolve_session
from cassio.table.cql import (
    CREATE_INDEX_CQL_TEMPLATE,
    CREATE_TABLE_CQL_TEMPLATE,
    DELETE_CQL_TEMPLATE,
    INSERT_ROW_CQL_TEMPLATE,
    SELECT_CQL_TEMPLATE,
    TRUNCATE_TABLE_CQL_TEMPLATE,
    CQLOpType,
)
from cassio.table.query import Predicate
from cassio.table.table_types import (
    ColumnSpecType,
    RowType,
    SessionType,
    normalize_type_desc,
)
from cassio.table.utils import (
    call_wrapped_async,
    handle_multicolumn_packing,
    handle_multicolumn_unpacking,
)


class CustomLogger(logging.Logger):
    def trace(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if self.isEnabledFor(5):
            self._log(5, msg, args, **kwargs)


logging.addLevelName(5, "TRACE")


logging.setLoggerClass(CustomLogger)


logger = logging.getLogger(__name__)


class BaseTable:
    ordering_in_partition: Optional[Union[str, List[str]]] = None

    def __init__(
        self,
        table: str,
        session: Optional[SessionType] = None,
        keyspace: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        row_id_type: Union[str, List[str]] = ["TEXT"],
        skip_provisioning: bool = False,
        async_setup: bool = False,
        body_index_options: Optional[List[Tuple[str, Any]]] = None,
        body_type: str = "TEXT",
    ) -> None:
        self.session = check_resolve_session(session)
        self.keyspace = check_resolve_keyspace(keyspace)
        self.table = table
        self.ttl_seconds = ttl_seconds
        self.row_id_type = normalize_type_desc(row_id_type)
        self.body_type = body_type
        self.skip_provisioning = skip_provisioning
        self._prepared_statements: Dict[str, PreparedStatement] = {}
        self._body_index_options = body_index_options
        self.db_setup_task: Optional[Task[None]] = None
        if async_setup:
            self.db_setup_task = asyncio.create_task(self.adb_setup())
        else:
            self.db_setup()

    def _schema_row_id(self) -> List[ColumnSpecType]:
        if len(self.row_id_type) == 1:
            return [
                ("row_id", self.row_id_type[0]),
            ]
        else:
            return [
                (f"row_id_{row_i}", row_typ)
                for row_i, row_typ in enumerate(self.row_id_type)
            ]

    def _schema_pk(self) -> List[ColumnSpecType]:
        return self._schema_row_id()

    def _schema_cc(self) -> List[ColumnSpecType]:
        return []

    def _schema_da(self) -> List[ColumnSpecType]:
        return [
            ("body_blob", self.body_type),
        ]

    async def _aschema_da(self) -> List[ColumnSpecType]:
        return self._schema_da()

    def _schema(self) -> Dict[str, List[ColumnSpecType]]:
        return {
            "pk": self._schema_pk(),
            "cc": self._schema_cc(),
            "da": self._schema_da(),
        }

    async def _aschema(self) -> Dict[str, List[ColumnSpecType]]:
        return {
            "pk": self._schema_pk(),
            "cc": self._schema_cc(),
            "da": await self._aschema_da(),
        }

    def _schema_primary_key(self) -> List[ColumnSpecType]:
        return self._schema_pk() + self._schema_cc()

    def _schema_collist(self) -> List[ColumnSpecType]:
        full_list = self._schema_da() + self._schema_cc() + self._schema_pk()
        return full_list

    def _schema_colnameset(self) -> Set[str]:
        full_list = self._schema_collist()
        full_set = set(col for col, _ in full_list)
        assert len(full_list) == len(full_set)
        return full_set

    def _desc_table(self) -> str:
        columns = self._schema()
        col_str = (
            "[("
            + ", ".join("%s(%s)" % colspec for colspec in columns["pk"])
            + ") "
            + ", ".join("%s(%s)" % colspec for colspec in columns["cc"])
            + "] "
            + ", ".join("%s(%s)" % colspec for colspec in columns["da"])
        )
        return col_str

    def _extract_where_clause_blocks(
        self, args_dict: Any
    ) -> Tuple[Any, List[str], Tuple[Any, ...]]:
        # Removes some of the passed kwargs and returns the remaining,
        # plus the pieces for a WHERE
        _allowed_colspecs = self._schema_collist()
        passed_columns = sorted(
            [col for col, _ in _allowed_colspecs if col in args_dict]
        )
        residual_args = {k: v for k, v in args_dict.items() if k not in passed_columns}

        where_clause_blocks = []
        where_clause_vals = []
        for col in passed_columns:
            value = args_dict[col]
            if isinstance(value, Predicate):
                pred_op_name, pred_value = value.render()
                where_clause_blocks.append(f"{col} {pred_op_name} %s")
                where_clause_vals.append(pred_value)
            else:
                where_clause_blocks.append(f"{col} = %s")
                where_clause_vals.append(value)

        return (
            residual_args,
            where_clause_blocks,
            tuple(where_clause_vals),
        )

    def _normalize_kwargs(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        new_args_dict = handle_multicolumn_unpacking(
            args_dict,
            "row_id",
            [col for col, _ in self._schema_row_id()],
        )
        return new_args_dict

    def _normalize_row(self, raw_row: Any) -> Dict[str, Any]:
        if isinstance(raw_row, dict):
            dict_row = raw_row
        else:
            dict_row = raw_row._asdict()
        #
        repacked_row = handle_multicolumn_packing(
            unpacked_row=dict_row,
            key_name="row_id",
            unpacked_keys=[col for col, _ in self._schema_row_id()],
        )
        return repacked_row

    def _delete(self, is_async: bool, **kwargs: Any) -> Union[None, ResponseFuture]:
        n_kwargs = self._normalize_kwargs(kwargs)
        (
            rest_kwargs,
            where_clause_blocks,
            delete_cql_vals,
        ) = self._extract_where_clause_blocks(n_kwargs)
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

    def delete(self, **kwargs: Any) -> None:
        self._ensure_db_setup()
        self._delete(is_async=False, **kwargs)
        return None

    def delete_async(self, **kwargs: Any) -> ResponseFuture:
        self._ensure_db_setup()
        return self._delete(is_async=True, **kwargs)

    async def adelete(self, **kwargs: Any) -> None:
        await self._aensure_db_setup()
        await call_wrapped_async(self.delete_async, **kwargs)

    def _clear(self, is_async: bool) -> Union[None, ResponseFuture]:
        truncate_table_cql = TRUNCATE_TABLE_CQL_TEMPLATE.format()
        if is_async:
            return self.execute_cql_async(
                truncate_table_cql, args=tuple(), op_type=CQLOpType.WRITE
            )
        else:
            self.execute_cql(truncate_table_cql, args=tuple(), op_type=CQLOpType.WRITE)
            return None

    def clear(self) -> None:
        self._ensure_db_setup()
        self._clear(is_async=False)
        return None

    def clear_async(self) -> ResponseFuture:
        self._ensure_db_setup()
        return self._clear(is_async=True)

    async def aclear(self) -> None:
        await self._aensure_db_setup()
        await call_wrapped_async(self.clear_async)

    def _has_index_analyzers(self) -> bool:
        if not self._body_index_options:
            return False
        for option in self._body_index_options:
            if option[0] == "index_analyzer":
                return True
        return False

    def _extract_index_analyzers(
        self, args_dict: Any
    ) -> Tuple[Any, List[str], Tuple[Any, ...]]:
        rest_args = args_dict.copy()
        where_clause_blocks: List[str] = []
        where_clause_vals: List[Any] = []
        if "body_search" in args_dict:
            if not self._has_index_analyzers():
                raise ValueError(
                    "Cannot do body search because no index analyzer "
                    "was configured on the table"
                )
            body_search_texts = rest_args.pop("body_search")
            if not isinstance(body_search_texts, list):
                body_search_texts = [body_search_texts]
            for text in body_search_texts:
                where_clause_blocks.append("body_blob : %s")
                where_clause_vals.append(text)
        return rest_args, where_clause_blocks, tuple(where_clause_vals)

    def _parse_select_core_params(
        self, **kwargs: Any
    ) -> Tuple[str, str, Tuple[Any, ...]]:
        n_kwargs = self._normalize_kwargs(kwargs)
        # TODO: work on a columns: Optional[List[str]] = None
        # (but with nuanced handling of the column-magic we have here)
        columns = None
        if columns is None:
            columns_desc = "*"
        else:
            # TODO: handle translations here?
            # columns_desc = ", ".join(columns)
            raise NotImplementedError("Column selection is not implemented.")
        #
        (
            rest_kwargs,
            where_clause_blocks,
            select_cql_vals,
        ) = self._extract_where_clause_blocks(n_kwargs)

        (
            rest_kwargs,
            analyzer_clause_blocks,
            analyzer_cql_vals,
        ) = self._extract_index_analyzers(rest_kwargs)

        assert rest_kwargs == {}

        all_where_clauses = where_clause_blocks + analyzer_clause_blocks
        if not all_where_clauses:
            where_clause = ""
        else:
            where_clause = "WHERE " + " AND ".join(all_where_clauses)
        return columns_desc, where_clause, select_cql_vals + analyzer_cql_vals

    def _get_select_cql(self, **kwargs: Any) -> Tuple[str, Tuple[Any, ...]]:
        columns_desc, where_clause, get_cql_vals = self._parse_select_core_params(
            **kwargs
        )
        limit_clause = ""
        limit_cql_vals: List[Any] = []
        select_vals = tuple(list(get_cql_vals) + limit_cql_vals)
        #
        select_cql = SELECT_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        return select_cql, select_vals

    def _normalize_result_set(
        self, result_set: Iterable[RowType]
    ) -> Optional[Dict[str, Any]]:
        if isinstance(result_set, ResultSet):
            result = result_set.one()
        else:
            result = None
        #
        if result is None:
            return result
        else:
            return self._normalize_row(result)

    def get(self, **kwargs: Any) -> Optional[RowType]:
        self._ensure_db_setup()
        select_cql, select_vals = self._get_select_cql(**kwargs)
        # dancing around the result set (to comply with type checking):
        result_set = self.execute_cql(
            select_cql, args=select_vals, op_type=CQLOpType.READ
        )
        return self._normalize_result_set(result_set)

    def get_async(self, **kwargs: Any) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")

    async def aget(self, **kwargs: Any) -> Optional[RowType]:
        await self._aensure_db_setup()
        select_cql, select_vals = self._get_select_cql(**kwargs)
        # dancing around the result set (to comply with type checking):
        result_set = await self.aexecute_cql(
            select_cql, args=select_vals, op_type=CQLOpType.READ
        )
        return self._normalize_result_set(result_set)

    def _put(self, is_async: bool, **kwargs: Any) -> Union[None, ResponseFuture]:
        n_kwargs = self._normalize_kwargs(kwargs)
        primary_key = self._schema_primary_key()
        assert set(col for col, _ in primary_key) - set(n_kwargs.keys()) == set()
        columns = [col for col, _ in self._schema_collist() if col in n_kwargs]
        columns_desc = ", ".join(columns)
        insert_cql_vals = [n_kwargs[col] for col in columns]
        value_placeholders = ", ".join("%s" for _ in columns)
        #
        ttl_seconds = (
            n_kwargs["ttl_seconds"] if "ttl_seconds" in n_kwargs else self.ttl_seconds
        )
        if ttl_seconds is not None:
            ttl_spec = "USING TTL %s"
            ttl_vals = [ttl_seconds]
        else:
            ttl_spec = ""
            ttl_vals = []
        #
        insert_cql_args = tuple(insert_cql_vals + ttl_vals)
        insert_cql = INSERT_ROW_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            value_placeholders=value_placeholders,
            ttl_spec=ttl_spec,
        )
        #
        if is_async:
            return self.execute_cql_async(
                insert_cql, args=insert_cql_args, op_type=CQLOpType.WRITE
            )
        else:
            self.execute_cql(insert_cql, args=insert_cql_args, op_type=CQLOpType.WRITE)
            return None

    def put(self, **kwargs: Any) -> None:
        self._ensure_db_setup()
        self._put(is_async=False, **kwargs)
        return None

    def put_async(self, **kwargs: Any) -> ResponseFuture:
        self._ensure_db_setup()
        return self._put(is_async=True, **kwargs)

    async def aput(self, **kwargs: Any) -> None:
        await self._aensure_db_setup()
        await call_wrapped_async(self.put_async, **kwargs)

    def _get_db_setup_cql(self, schema: Dict[str, List[ColumnSpecType]]) -> str:
        column_specs = [
            f"{col_spec[0]} {col_spec[1]}"
            for _schema_grp in ["pk", "cc", "da"]
            for col_spec in schema[_schema_grp]
        ]
        pk_spec = ", ".join(col for col, _ in schema["pk"])
        cc_spec = ", ".join(col for col, _ in schema["cc"])
        primkey_spec = f"( ( {pk_spec} ) {',' if schema['cc'] else ''} {cc_spec} )"

        table_options = []

        if schema["cc"]:
            if self.ordering_in_partition is None:
                raise ValueError("Unspecified ordering for clustering column(s)")
            if isinstance(self.ordering_in_partition, str):
                _cc_orderings = [self.ordering_in_partition for _ in schema["cc"]]
            else:
                # must be a list
                assert len(self.ordering_in_partition) == len(schema["cc"])
                _cc_orderings = self.ordering_in_partition
            clu_core = ", ".join(
                f"{col} {ordering}"
                for (col, _), ordering in zip(schema["cc"], _cc_orderings)
            )
            table_options.append(f"CLUSTERING ORDER BY ({clu_core})")

        if len(table_options) > 0:
            options_clause = "WITH " + " AND ".join(table_options)
        else:
            options_clause = ""

        create_table_cql = CREATE_TABLE_CQL_TEMPLATE.format(
            columns_spec=" ".join(f"  {cs}," for cs in column_specs),
            primkey_spec=primkey_spec,
            options_clause=options_clause,
        )
        return create_table_cql

    @staticmethod
    def _get_create_index_cql(
        index_name: str, index_column: str, index_options: List[Tuple[str, Any]]
    ) -> str:
        options_clause = ""
        if len(index_options) > 0:
            formatted_options = []
            for option in index_options:
                key, value = option
                if isinstance(value, dict):
                    value = json.dumps(value).replace("{", "{{").replace("}", "}}")
                    formatted_options.append(f"'{key}': '{value}'")
                elif isinstance(value, str):
                    try:
                        json.loads(value)
                        value = value.replace("{", "{{").replace("}", "}}")
                    except ValueError:
                        pass
                    try:
                        unescaped = value.replace("{{", "{").replace("}}", "}")
                        json.loads(unescaped)
                        logger.warning(
                            "Escaping JSON values with double braces is not needed anymore"
                        )
                    except ValueError:
                        pass
                    formatted_options.append(f"'{key}': '{value}'")
                elif isinstance(value, bool):
                    if value:
                        formatted_options.append(f"'{key}': true")
                    else:
                        formatted_options.append(f"'{key}': false")
                else:
                    raise ValueError("Unsupported index_option format")

            formatted_options.sort()

            options_text = ", ".join(formatted_options)

            # this is double escaped because the cql will go through
            # another format method before being executed
            options_clause = f"WITH OPTIONS = {{{{ {options_text} }}}}"

        return CREATE_INDEX_CQL_TEMPLATE.format(
            index_name=index_name,
            index_column=index_column,
            options_clause=options_clause,
        )

    @staticmethod
    def _get_create_analyzer_index_cql(index_options: List[Tuple[str, Any]]) -> str:
        index_name = "idx_body"
        index_column = "body_blob"
        return BaseTable._get_create_index_cql(
            index_name=index_name,
            index_column=index_column,
            index_options=index_options,
        )

    def db_setup(self) -> None:
        create_table_cql = self._get_db_setup_cql(self._schema())
        self.execute_cql(create_table_cql, op_type=CQLOpType.SCHEMA)
        if self._body_index_options:
            self.execute_cql(
                self._get_create_analyzer_index_cql(self._body_index_options),
                op_type=CQLOpType.SCHEMA,
            )

    async def adb_setup(self) -> None:
        schema = await self._aschema()
        create_table_cql = self._get_db_setup_cql(schema)
        await self.aexecute_cql(create_table_cql, op_type=CQLOpType.SCHEMA)
        if self._body_index_options:
            await self.aexecute_cql(
                self._get_create_analyzer_index_cql(self._body_index_options),
                op_type=CQLOpType.SCHEMA,
            )

    def _ensure_db_setup(self) -> None:
        if self.db_setup_task:
            try:
                self.db_setup_task.result()
            except InvalidStateError:
                raise ValueError(
                    "Asynchronous setup of the DB not finished. "
                    "NB: Table sync methods shouldn't be called from the "
                    "event loop. Consider using their async equivalents."
                )

    async def _aensure_db_setup(self) -> None:
        if self.db_setup_task:
            await self.db_setup_task

    def _finalize_cql_semitemplate(self, cql_semitemplate: str) -> str:
        table_fqname = f"{self.keyspace}.{self.table}"
        table_name = self.table
        final_cql = cql_semitemplate.format(
            table_fqname=table_fqname, table_name=table_name
        )
        return final_cql

    def _obtain_prepared_statement(self, final_cql: str) -> PreparedStatement:
        # TODO: improve this placeholder handling
        _preparable_cql = final_cql.replace("%s", "?")
        # handle the cache of prepared statements
        if _preparable_cql not in self._prepared_statements:
            logger.debug(f'Preparing statement "{_preparable_cql}"')
            self._prepared_statements[_preparable_cql] = self.session.prepare(
                _preparable_cql
            )
        return self._prepared_statements[_preparable_cql]

    def execute_cql(
        self,
        cql_semitemplate: str,
        op_type: CQLOpType,
        args: Tuple[Any, ...] = tuple(),
    ) -> Iterable[RowType]:
        final_cql = self._finalize_cql_semitemplate(cql_semitemplate)
        #
        if op_type == CQLOpType.SCHEMA and self.skip_provisioning:
            # these operations are not executed for this instance:
            logger.debug(f'Not executing statement "{final_cql}"')
            return []
        if op_type == CQLOpType.SCHEMA:
            # schema operations are not to be 'prepared'
            statement = SimpleStatement(final_cql)
            logger.debug(f'Executing statement "{final_cql}" as simple (unprepared)')
        else:
            statement = self._obtain_prepared_statement(final_cql)
            logger.debug(f'Executing statement "{final_cql}" as prepared')
        logger.trace(f'Statement "{final_cql}" has args: "{str(args)}"')  # type: ignore
        return cast(Iterable[RowType], self.session.execute(statement, args))

    def execute_cql_async(
        self,
        cql_semitemplate: str,
        op_type: CQLOpType,
        args: Tuple[Any, ...] = tuple(),
    ) -> ResponseFuture:
        final_cql = self._finalize_cql_semitemplate(cql_semitemplate)
        #
        if op_type == CQLOpType.SCHEMA:
            raise RuntimeError("Schema operations cannot be asynchronous")
        statement = self._obtain_prepared_statement(final_cql)
        logger.debug(f'Executing_async statement "{final_cql}" as prepared')
        logger.trace(f'Statement "{final_cql}" has args: "{str(args)}"')  # type: ignore
        return self.session.execute_async(statement, args)

    async def aexecute_cql(
        self,
        cql_semitemplate: str,
        op_type: CQLOpType,
        args: Tuple[Any, ...] = tuple(),
    ) -> Iterable[RowType]:
        final_cql = self._finalize_cql_semitemplate(cql_semitemplate)
        #
        if op_type == CQLOpType.SCHEMA and self.skip_provisioning:
            # these operations are not executed for this instance:
            logger.debug(f'Not aexecuting statement "{final_cql}"')
            return []
        if op_type == CQLOpType.SCHEMA:
            # schema operations are not to be 'prepared'
            statement = SimpleStatement(final_cql)
            logger.debug(f'aExecuting statement "{final_cql}" as simple (unprepared)')
        else:
            statement = self._obtain_prepared_statement(final_cql)
            logger.debug(f'aExecuting statement "{final_cql}" as prepared')
        logger.trace(f'Statement "{final_cql}" has args: "{str(args)}"')  # type: ignore
        return cast(
            Iterable[RowType],
            await call_wrapped_async(self.session.execute_async, statement, args),
        )
