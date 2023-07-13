from typing import Any, List, Dict, Optional, Protocol, Set, Tuple, Union

from cassio.table.table_types import (
    ColumnSpecType,
    RowType,
    SessionType,
    normalize_type_desc,
)
from cassio.table.cql import (
    CREATE_TABLE_CQL_TEMPLATE,
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
        delete_cql = f"DELETE_ROW: ({', '.join(col for col, _ in primary_key)})"
        delete_cql_vals = tuple(kwargs[c] for c, _ in primary_key)
        self.execute_cql(delete_cql, delete_cql_vals)

    def clear(self) -> None:
        self.execute_cql("TRUNCATE", tuple())

    def get(self, **kwargs: Any) -> List[RowType]:
        primary_key = self._schema_primary_key()
        assert set(kwargs.keys()) == set(col for col, _ in primary_key)
        get_cql = f"GET_ROW: ({', '.join(col for col, _ in primary_key)})"
        get_cql_vals = tuple(kwargs[c] for c, _ in primary_key)
        return self.execute_cql(get_cql, get_cql_vals)

    def put(self, **kwargs: Any) -> None:
        primary_key = self._schema_primary_key()
        assert set(col for col, _ in primary_key) - set(kwargs.keys()) == set()
        columns = [col for col, _ in self._schema_collist() if col in kwargs]
        col_vals = tuple([kwargs[col] for col in columns])
        ttl_seconds = (
            kwargs["ttl_seconds"] if "ttl_seconds" in kwargs else self.ttl_seconds
        )
        put_cql = f"PUT_ROW: ({', '.join(columns)} TTL={str(ttl_seconds)})"
        self.execute_cql(put_cql, col_vals)

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
            clustering_spec = f" WITH CLUSTERING ORDER BY ({clu_core})"
        else:
            clustering_spec = ""
        #
        create_table_cql = CREATE_TABLE_CQL_TEMPLATE.format(
            columns_spec="\n".join(f"  {cs}," for cs in column_specs),
            primkey_spec=primkey_spec,
            clustering_spec=clustering_spec,
        )
        self.execute_cql(create_table_cql, is_provision=True)

    def execute_cql(
        self,
        cql_semitemplate: str,
        args: Tuple[Any, ...] = tuple(),
        is_provision=False,
    ) -> List[RowType]:
        cls_name = self.__class__.__name__
        table_fqname = f"{self.keyspace}.{self.table}"
        final_cql = cql_semitemplate.format(table_fqname=table_fqname)
        if is_provision and self.skip_provisioning:
            print(f"NO-EXEC CQL({cls_name:<32}) << {final_cql} >> {str(args)}")
        else:
            print(f"CQL({cls_name:<32}) << {final_cql} >> {str(args)}")
        return []
