CREATE_TABLE_CQL_TEMPLATE = """CREATE TABLE {{table_fqname}} (
{columns_spec}
  PRIMARY KEY {primkey_spec}
){clustering_spec};"""
