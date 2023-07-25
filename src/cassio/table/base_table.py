from typing import Any, List, Dict, Optional, Protocol, Set, Tuple, Union

from cassandra.query import SimpleStatement, PreparedStatement  # type: ignore

from cassio.table.table_types import (
    ColumnSpecType,
    RowType,
    SessionType,
    normalize_type_desc,
)
from cassio.table.cql import (
    CQLOpType,
    CREATE_TABLE_CQL_TEMPLATE,
    TRUNCATE_TABLE_CQL_TEMPLATE,
    DELETE_CQL_TEMPLATE,
    SELECT_CQL_TEMPLATE,
    INSERT_ROW_CQL_TEMPLATE,
)


class BaseTable:
    def __init__(
        self,
        session: SessionType,
        keyspace: str,
        table: str,
        /,
        ttl_seconds: Optional[int] = None,
        row_id_type: Union[str, List[str]] = ["TEXT"],
        skip_provisioning=False,
    ) -> None:
        self.session = session
        self.keyspace = keyspace
        self.table = table
        self.ttl_seconds = ttl_seconds
        self.row_id_type = normalize_type_desc(row_id_type)
        self.skip_provisioning = skip_provisioning
        self._prepared_statements: Dict[str, PreparedStatement] = {}
        self.db_setup()

    def _schema_row_id(self) -> List[ColumnSpecType]:
        assert len(self.row_id_type) == 1
        return [
            ("row_id", self.row_id_type[0]),
        ]

    def _schema_pk(self) -> List[ColumnSpecType]:
        return self._schema_row_id()

    def _schema_cc(self) -> List[ColumnSpecType]:
        return []

    def _schema_da(self) -> List[ColumnSpecType]:
        return [
            ("body_blob", "TEXT"),
        ]

    def _schema(self) -> Dict[str, List[ColumnSpecType]]:
        return {
            "pk": self._schema_pk(),
            "cc": self._schema_cc(),
            "da": self._schema_da(),
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

    def delete(self, **kwargs: Any) -> None:
        primary_key = self._schema_primary_key()
        assert set(kwargs.keys()) == set(col for col, _ in primary_key)
        where_clause_blocks = [f"{pk_col} = %s" for pk_col, _ in primary_key]
        where_clause = " AND ".join(where_clause_blocks)
        delete_cql_vals = tuple(kwargs[pk_col] for pk_col, _ in primary_key)
        delete_cql = DELETE_CQL_TEMPLATE.format(
            where_clause=where_clause,
        )
        self.execute_cql(delete_cql, args=delete_cql_vals, op_type=CQLOpType.WRITE)

    def clear(self) -> None:
        truncate_table_cql = TRUNCATE_TABLE_CQL_TEMPLATE.format()
        self.execute_cql(truncate_table_cql, args=tuple(), op_type=CQLOpType.WRITE)

    def get(self, **kwargs: Any) -> List[RowType]:
        primary_key = self._schema_primary_key()
        assert set(kwargs.keys()) == set(col for col, _ in primary_key)
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
        #
        where_clause_blocks = [f"{pk_col} = %s" for pk_col, _ in primary_key]
        where_clause = " AND ".join(where_clause_blocks)
        get_cql_vals = tuple(kwargs[pk_col] for pk_col, _ in primary_key)
        limit_clause = ""
        #
        select_cql = SELECT_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        return self.execute_cql(select_cql, args=get_cql_vals, op_type=CQLOpType.READ)

    def put(self, **kwargs: Any) -> None:
        primary_key = self._schema_primary_key()
        assert set(col for col, _ in primary_key) - set(kwargs.keys()) == set()
        columns = [col for col, _ in self._schema_collist() if col in kwargs]
        columns_desc = ", ".join(columns)
        insert_cql_vals = tuple([kwargs[col] for col in columns])
        value_placeholders = ", ".join("%s" for _ in columns)
        #
        ttl_seconds = (
            kwargs["ttl_seconds"] if "ttl_seconds" in kwargs else self.ttl_seconds
        )
        if ttl_seconds is not None:
            ttl_spec = f"USING TTL {ttl_seconds}"
        else:
            ttl_spec = ""
        #
        insert_cql = INSERT_ROW_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            value_placeholders=value_placeholders,
            ttl_spec=ttl_spec,
        )
        #
        self.execute_cql(insert_cql, args=insert_cql_vals, op_type=CQLOpType.WRITE)

    def db_setup(self) -> None:
        _schema = self._schema()
        column_specs = [
            f"{col_spec[0]} {col_spec[1]}"
            for _schema_grp in ["pk", "cc", "da"]
            for col_spec in _schema[_schema_grp]
        ]
        pk_spec = ", ".join(col for col, _ in _schema["pk"])
        cc_spec = ", ".join(col for col, _ in _schema["cc"])
        primkey_spec = f"( ( {pk_spec} ) {',' if _schema['cc'] else ''} {cc_spec} )"
        if _schema["cc"]:
            clu_core = ", ".join(f"{col} ASC" for col, _ in _schema["cc"])
            clustering_spec = f"WITH CLUSTERING ORDER BY ({clu_core})"
        else:
            clustering_spec = ""
        #
        create_table_cql = CREATE_TABLE_CQL_TEMPLATE.format(
            columns_spec=" ".join(f"  {cs}," for cs in column_specs),
            primkey_spec=primkey_spec,
            clustering_spec=clustering_spec,
        )
        self.execute_cql(create_table_cql, op_type=CQLOpType.SCHEMA)

    def execute_cql(
        self,
        cql_semitemplate: str,
        op_type: CQLOpType,
        args: Tuple[Any, ...] = tuple(),
    ) -> List[RowType]:
        table_fqname = f"{self.keyspace}.{self.table}"
        final_cql = cql_semitemplate.format(table_fqname=table_fqname)
        #
        if op_type == CQLOpType.SCHEMA and self.skip_provisioning:
            # these operations are not executed for this instance:
            return []
        else:
            if op_type == CQLOpType.SCHEMA:
                # schema operations are not to be 'prepared'
                statement = SimpleStatement(final_cql)
            else:
                # TODO: improve this placeholder handling
                _preparable_cql = final_cql.replace("%s", "?")
                # handle the cache of prepared statements
                if _preparable_cql not in self._prepared_statements:
                    self._prepared_statements[_preparable_cql] = self.session.prepare(
                        _preparable_cql
                    )
                statement = self._prepared_statements[_preparable_cql]
            return self.session.execute(statement, args)
