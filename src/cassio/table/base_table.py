from typing import Any, List, Dict, Protocol, Set, Tuple, Union

from cassio.table.table_types import ColumnSpecType, RowType, SessionType


class BaseTable:
    def __init__(self, session: SessionType, keyspace: str, table: str) -> None:
        self.session = session
        self.keyspace = keyspace
        self.table = table

    def _schema_row_id(self) -> List[ColumnSpecType]:
        return [
            ("row_id", "TEXT"),
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

    def _schema_colset(self) -> Set[ColumnSpecType]:
        full_list = self._schema_collist()
        full_set = set(full_list)
        assert len(full_list) == len(full_set)
        return full_set

    def _desc_table(self) -> str:
        columns = self._schema()
        col_str = (
            "[("
            + ", ".join('%s(%s)' % colspec for colspec in columns["pk"])
            + ") "
            + ", ".join('%s(%s)' % colspec for colspec in columns["cc"])
            + "] "
            + ", ".join('%s(%s)' % colspec for colspec in columns["da"])
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
        put_cql = f"PUT_ROW: ({', '.join(columns)})"
        self.execute_cql(put_cql, col_vals)

    def db_setup(self) -> None:
        self.execute_cql(f"MKTABLE: {self._desc_table()}")

    def execute_cql(self, query: str, args: Tuple[Any, ...] = tuple()) -> List[RowType]:
        cls_name = self.__class__.__name__
        ftqual = f"{self.keyspace}.{self.table}"
        print(f"CQL({cls_name:<32}/{ftqual}) '{query}' {str(args)}")
        return []
