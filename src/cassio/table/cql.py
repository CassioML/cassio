from typing import Union
from enum import Enum

from cassandra.query import SimpleStatement, PreparedStatement  # type: ignore


class CQLOpType(Enum):
    SCHEMA = 1
    WRITE = 2
    READ = 3


CREATE_TABLE_CQL_TEMPLATE = """CREATE TABLE IF NOT EXISTS {{table_fqname}} ({columns_spec} PRIMARY KEY {primkey_spec}) {clustering_spec};"""  # noqa: E501

TRUNCATE_TABLE_CQL_TEMPLATE = """TRUNCATE TABLE {{table_fqname}};"""

DELETE_CQL_TEMPLATE = """DELETE FROM {{table_fqname}} {where_clause};"""

SELECT_CQL_TEMPLATE = (
    """SELECT {columns_desc} FROM {{table_fqname}} {where_clause} {limit_clause};"""
)

INSERT_ROW_CQL_TEMPLATE = """INSERT INTO {{table_fqname}} ({columns_desc}) VALUES ({value_placeholders}) {ttl_spec} ;"""  # noqa: E501

CREATE_INDEX_CQL_TEMPLATE = """CREATE CUSTOM INDEX IF NOT EXISTS {index_name}_{{table_name}} ON {{table_fqname}} ({index_column}) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';"""  # noqa: E501

CREATE_KEYS_INDEX_CQL_TEMPLATE = """CREATE CUSTOM INDEX IF NOT EXISTS {index_name}_{{table_name}} ON {{table_fqname}} (KEYS({index_column})) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';"""  # noqa: E501

CREATE_ENTRIES_INDEX_CQL_TEMPLATE = """CREATE CUSTOM INDEX IF NOT EXISTS {index_name}_{{table_name}} ON {{table_fqname}} (ENTRIES({index_column})) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';"""  # noqa: E501

SELECT_ANN_CQL_TEMPLATE = """SELECT {columns_desc} FROM {{table_fqname}} {where_clause} ORDER BY {vector_column} ANN OF %s {limit_clause};"""  # noqa: E501

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

    def assert_last_equal(self, expected_statements):
        # used for testing
        last_executed = self.last(len(expected_statements))
        assert len(last_executed) == len(expected_statements)
        for s_exe, s_expe in zip(last_executed, expected_statements):
            assert s_exe[1] == s_expe[1], f"EXE#{str(s_exe[1])}# != EXPE#{s_expe[1]}#"
            exe_cql = self.normalizeCQLStatement(s_exe[0])
            expe_cql = self.normalizeCQLStatement(s_expe[0])
            assert exe_cql == expe_cql, f"EXE#{exe_cql}# != EXPE#{expe_cql}#"
