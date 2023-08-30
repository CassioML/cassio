"""
An extractor able to resolve single-row lookups from Cassandra tables in
a keyspace, with a fair amount of metadata inspection.
"""

from functools import reduce
from typing import List

from cassandra.query import SimpleStatement


RETRIEVE_ONE_ROW_CQL_TEMPLATE = 'SELECT * FROM {keyspace}.{table} WHERE {whereClause} LIMIT 1'


def _table_primary_key_columns(session, keyspace, table) -> List[str]:
    table = session.cluster.metadata.keyspaces[keyspace].tables[table]
    return [
        col.name for col in table.partition_key
    ] + [
        col.name for col in table.clustering_key
    ]


def _ensure_full_extraction_tuple(tpl, admit_nulls):
    if len(tpl) < 2:
        raise ValueError("At least table and column names are required in the field_mapper.")
    elif len(tpl) == 2:
        return tuple(list(tpl) + [admit_nulls, None])
    elif len(tpl) == 3:
        return tuple(list(tpl) + [None])
    elif len(tpl) == 4:
        return tpl
    else:
        raise ValueError("Cannot specify more than (table, column_or_function, admit_nulls, default) in the field_mapper.")


class CassandraExtractor:

    def __init__(self, session, keyspace, field_mapper, admit_nulls):
        self.session = session
        self.keyspace = keyspace
        #
        _field_mapper = {
            k: _ensure_full_extraction_tuple(v, admit_nulls)
            for k, v in field_mapper.items()
        }
        self.field_mapper = _field_mapper
        # derived fields
        self.tables_needed = {fmv[0] for fmv in field_mapper.values()}
        self.primary_key_map = {
            table: _table_primary_key_columns(self.session, self.keyspace, table)
            for table in self.tables_needed
        }
        # all primary-key values needed across tables
        self.input_parameters = set(reduce(lambda accum, nw: accum | set(nw), self.primary_key_map.values(), set()))
        self.output_parameters = set(field_mapper.keys())

        def _dbc(args_dict):
            return self(**args_dict)

        self.dictionary_based_call = _dbc


        # TODOs:
        #   move this getter creation someplace else
        #   query a table only once (grouping required variables by source table,
        #   selecting only those unless function passed)
        def _getter(**kwargs):
            def _retrieve_field(_field, _table2, _key_columns, _column_or_function, _admit_nulls, _default, _all_pkey_value_map):
                selector = SimpleStatement(RETRIEVE_ONE_ROW_CQL_TEMPLATE.format(
                    keyspace=keyspace,
                    table=_table2,
                    whereClause=' AND '.join(
                        f'{kc} = %s'
                        for kc in _key_columns
                    ),
                ))
                values = tuple([
                    _all_pkey_value_map[kc]
                    for kc in _key_columns
                ])
                row = session.execute(selector, values).one()
                if row:
                    # this for robustness against row_factory vs dict_factory of the Session:
                    _rowdict = row if isinstance(row, dict) else row._asdict()
                    if callable(_column_or_function):
                        _retval = _column_or_function(_rowdict)
                    else:
                        _retval = _rowdict[_column_or_function]
                else:
                    _retval = None

                if _retval is None:
                    if _admit_nulls:
                        return _default
                    else:
                        raise ValueError('Null data found for "%s"' % _field)
                else:
                    return _retval

            return {
                field: _retrieve_field(
                    _field=field,
                    _table2=table,
                    _key_columns=self.primary_key_map[table],
                    _column_or_function=column_or_function,
                    _admit_nulls=admit_nulls,
                    _default=default,
                    _all_pkey_value_map=kwargs,
                )
                for field, (table, column_or_function, admit_nulls, default) in self.field_mapper.items()
            }

        self.getter = _getter

    def __call__(self, **kwargs):
        return self.getter(**kwargs)
