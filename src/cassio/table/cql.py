from enum import Enum


class CQLOpType(Enum):
    SCHEMA = 1
    WRITE = 2
    READ = 3


CREATE_TABLE_CQL_TEMPLATE = """CREATE TABLE {{table_fqname}} ({columns_spec} PRIMARY KEY {primkey_spec}) {clustering_spec};"""

TRUNCATE_TABLE_CQL_TEMPLATE = """TRUNCATE TABLE {{table_fqname}};"""

DELETE_CQL_TEMPLATE = """DELETE FROM {{table_fqname}} WHERE {where_clause};"""

SELECT_CQL_TEMPLATE = """SELECT {columns_desc} FROM {{table_fqname}} WHERE {where_clause} {limit_clause};"""

INSERT_ROW_CQL_TEMPLATE = """INSERT INTO {{table_fqname}} ({columns_desc}) VALUES ({value_placeholders}) {ttl_spec} ;"""

CREATE_INDEX_CQL_TEMPLATE = """CREATE CUSTOM INDEX IF NOT EXISTS {index_name} ON {{table_fqname}} ({index_column}) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';"""
