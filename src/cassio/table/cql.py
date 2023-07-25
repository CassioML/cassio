from typing import Union
from enum import Enum

from cassandra.query import SimpleStatement, PreparedStatement  # type: ignore


class CQLOpType(Enum):
    SCHEMA = 1
    WRITE = 2
    READ = 3


CREATE_TABLE_CQL_TEMPLATE = """CREATE TABLE {{table_fqname}} ({columns_spec} PRIMARY KEY {primkey_spec}) {clustering_spec};"""

TRUNCATE_TABLE_CQL_TEMPLATE = """TRUNCATE TABLE {{table_fqname}};"""

DELETE_CQL_TEMPLATE = """DELETE FROM {{table_fqname}} {where_clause};"""

SELECT_CQL_TEMPLATE = (
    """SELECT {columns_desc} FROM {{table_fqname}} {where_clause} {limit_clause};"""
)

INSERT_ROW_CQL_TEMPLATE = """INSERT INTO {{table_fqname}} ({columns_desc}) VALUES ({value_placeholders}) {ttl_spec} ;"""

CREATE_INDEX_CQL_TEMPLATE = """CREATE CUSTOM INDEX IF NOT EXISTS {index_name} ON {{table_fqname}} ({index_column}) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';"""

SELECT_ANN_CQL_TEMPLATE = """SELECT {columns_desc} FROM {{table_fqname}} ORDER BY {vector_column} ANN OF %s {where_clause} {limit_clause};"""

CQLStatementType = Union[str, SimpleStatement, PreparedStatement]

# Mock DB session
class MockDBSession:
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.statements = []

    @staticmethod
    def getStatementBody(statement: CQLStatementType) -> str:
        if isinstance(statement, str):
            _statement = statement
        elif isinstance(statement, SimpleStatement):
            _statement = statement.query_string
        elif isinstance(statement, PreparedStatement):
            _statement = statement.query_string
        else:
            raise ValueError()
        return _statement

    @staticmethod
    def normalizeCQLStatement(statement: CQLStatementType) -> str:
        _statement = MockDBSession.getStatementBody(statement)
        _s = (
            _statement.replace(";", " ")
            .replace("%s", " %s ")
            .replace("?", " ? ")
            .replace("=", " = ")
            .replace(")", " ) ")
            .replace("(", " ( ")
            .replace("\n", " ")
        )
        return " ".join(
            tok.lower() for tok in (_t.strip() for _t in _s.split(" ") if _t.strip())
        )

    @staticmethod
    def prepare(statement):
        # A very unusable 'prepared statement' just for tracing/debugging:
        return PreparedStatement(None, 0, 0, statement, "keyspace", None, None, None)

    def execute(self, statement, arguments=tuple()):
        if self.verbose:
            #
            st_body = self.getStatementBody(statement)
            if isinstance(statement, str):
                st_type = "STR"
                placeholder_count = st_body.count("%s")
                assert "?" not in st_body
            elif isinstance(statement, SimpleStatement):
                st_type = "SIM"
                placeholder_count = st_body.count("%s")
                assert "?" not in st_body
            elif isinstance(statement, PreparedStatement):
                st_type = "PRE"
                placeholder_count = st_body.count("?")
                assert "%s" not in st_body
            #
            assert placeholder_count == len(arguments)
            #
            print(f"CQL_EXECUTE [{st_type}]:")
            print(f"    {st_body}")
            if arguments:
                print(f"    {str(arguments)}")
        self.statements.append((statement, arguments))
        return []

    def last_raw(self, n):
        if n <= 0:
            return []
        else:
            return self.statements[-n:]

    def last(self, n):
        return [
            (
                self.normalizeCQLStatement(stmt),
                data,
            )
            for stmt, data in self.last_raw(n)
        ]
