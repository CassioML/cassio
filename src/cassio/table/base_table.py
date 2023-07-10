from typing import Any, List, Dict, Protocol, Set, Tuple

from cassio.table.table_types import ColumnSpecType, RowType, SessionType


class BaseTable:
    def __init__(self, session: SessionType, keyspace: str, table: str) -> None:
        self.session = session
        self.keyspace = keyspace
        self.table = table

    @classmethod
    def _schema_row_id(cls) -> List[ColumnSpecType]:
        return [
            "row_id",
        ]

    @classmethod
    def _schema_pk(cls) -> List[ColumnSpecType]:
        return cls._schema_row_id()

    @classmethod
    def _schema_cc(cls) -> List[ColumnSpecType]:
        return []

    @classmethod
    def _schema_da(cls) -> List[ColumnSpecType]:
        return [
            "body_blob",
        ]

    @classmethod
    def _schema(cls) -> Dict[str, List[ColumnSpecType]]:
        return {
            "pk": cls._schema_pk(),
            "cc": cls._schema_cc(),
            "da": cls._schema_da(),
        }

    @classmethod
    def _schema_primary_key(cls) -> List[ColumnSpecType]:
        return cls._schema_pk() + cls._schema_cc()

    @classmethod
    def _schema_collist(cls) -> List[ColumnSpecType]:
        full_list = cls._schema_da() + cls._schema_cc() + cls._schema_pk()
        return full_list

    @classmethod
    def _schema_colset(cls) -> Set[ColumnSpecType]:
        full_list = cls._schema_collist()
        full_set = set(full_list)
        assert len(full_list) == len(full_set)
        return full_set

    def _desc_table(self) -> str:
        columns = self._schema()
        col_str = (
            "[("
            + ", ".join(columns["pk"])
            + ") "
            + ", ".join(columns["cc"])
            + "] "
            + ", ".join(columns["da"])
        )
        return col_str

    def delete(self, **kwargs: Any) -> None:
        primary_key = self._schema_primary_key()
        assert set(kwargs.keys()) == set(primary_key)
        delete_cql = f"DELETE_ROW: ({', '.join(primary_key)})"
        delete_cql_vals = tuple(kwargs[c] for c in primary_key)
        self.execute_cql(delete_cql, delete_cql_vals)

    def clear(self) -> None:
        self.execute_cql("TRUNCATE", tuple())

    def get(self, **kwargs: Any) -> List[RowType]:
        primary_key = self._schema_primary_key()
        assert set(kwargs.keys()) == set(primary_key)
        get_cql = f"GET_ROW: ({', '.join(primary_key)})"
        get_cql_vals = tuple(kwargs[c] for c in primary_key)
        return self.execute_cql(get_cql, get_cql_vals)

    def put(self, **kwargs: Any) -> None:
        primary_key = self._schema_primary_key()
        assert set(primary_key) - set(kwargs.keys()) == set()
        columns = [col for col in self._schema_collist() if col in kwargs]
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
