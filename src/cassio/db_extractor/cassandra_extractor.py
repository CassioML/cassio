"""
An extractor able to resolve single-row lookups from Cassandra tables in
a keyspace, with a fair amount of metadata inspection.
"""

from functools import reduce

from cassandra.query import SimpleStatement

from cassio.inspection import (
    _table_primary_key_columns,
)

_retrieve_one_row_cql_template = 'SELECT * FROM {keyspace}.{tableName} WHERE {whereClause} LIMIT 1'


class CassandraExtractor:

    def __init__(self, session, keyspace, field_mapper, literal_nones):
        self.session = session
        self.keyspace = keyspace
        self.field_mapper = field_mapper
        self.literal_nones = literal_nones  # TODO: handle much better
        # derived fields
        self.tables_needed = {fmv[0] for fmv in field_mapper.values()}
        self.primary_key_map = {
            table_name: _table_primary_key_columns(self.session, self.keyspace, table_name)
            for table_name in self.tables_needed
        }
        # all primary-key values needed across tables
        self.requiredParameters = list(reduce(lambda accum, nw: accum | set(nw), self.primary_key_map.values(), set()))

        # TODOs:
        #   move this getter creation someplace else
        #   query a table only once (grouping required variables by source table,
        #   selecting only those unless function passed)
        def _getter(**kwargs):
            def _retrieve_field(_table_name2, _key_columns, _column_or_extractor, _key_value_map):
                selector = SimpleStatement(_retrieve_one_row_cql_template.format(
                    keyspace=keyspace,
                    tableName=_table_name2,
                    whereClause=' AND '.join(
                        f'{kc} = %s'
                        for kc in _key_columns
                    ),
                ))
                values = tuple([
                    _key_value_map[kc]
                    for kc in _key_columns
                ])
                row = session.execute(selector, values).one()
                if row:
                    if callable(_column_or_extractor):
                        return _column_or_extractor(row)
                    else:
                        return getattr(row, _column_or_extractor)
                else:
                    if literal_nones:
                        return None
                    else:
                        raise ValueError('No data found for %s from %s.%s' % (
                            str(_column_or_extractor),
                            keyspace,
                            _table_name2,
                        ))

            return {
                field: _retrieve_field(tableName, self.primary_key_map[tableName], columnOrExtractor, kwargs)
                for field, (tableName, columnOrExtractor) in field_mapper.items()
            }

        self.getter = _getter

    def __call__(self, **kwargs):
        return self.getter(**kwargs)
