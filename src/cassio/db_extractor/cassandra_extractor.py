"""
An extractor able to resolve single-row lookups from Cassandra tables in
a keyspace, with a fair amount of metadata inspection.
"""

from functools import reduce

from cassio.inspection import (
    _table_primary_key_columns,
)


class CassandraExtractor():

    def __init__(self, session, keyspace, field_mapper, literal_nones):
        self.session = session
        self.keyspace = keyspace
        self.field_mapper = field_mapper
        self.literal_nones = literal_nones # TODO: handle much better
        # derived fields
        self.tablesNeeded = {fmv[0] for fmv in field_mapper.values()}
        self.primaryKeyMap = {
            tableName: _table_primary_key_columns(self.session, self.keyspace, tableName)
            for tableName in self.tablesNeeded
        }
        # all primary-key values needed across tables
        self.requiredParameters = list(reduce(lambda accum, nw: accum | set(nw), self.primaryKeyMap.values(), set()))
        # TODOs:
        #   move this getter creation someplace else
        #   query a table only once (grouping required variables by source table, selecting only those unless function passed)
        #   ignore 'deps' probably is better (we have access to session/ks anyway)
        def _getter(deps, **kwargs):
            _session = deps['session']
            _keyspace = deps['keyspace']
            def _retrieve_field(_session2, _keyspace2, _tableName2, _keyColumns, _columnOrExtractor, _keyValueMap):
                selector = 'SELECT * FROM {keyspace}.{tableName} WHERE {whereClause} LIMIT 1;'.format(
                    keyspace=_keyspace2,
                    tableName=_tableName2,
                    whereClause=' AND '.join(
                        f'{kc} = %s'
                        for kc in _keyColumns
                    ),
                )
                values = tuple([
                    _keyValueMap[kc]
                    for kc in _keyColumns
                ])
                row = _session2.execute(selector, values).one()
                if row:
                    if callable(_columnOrExtractor):
                        return _columnOrExtractor(row)
                    else:
                        return getattr(row, _columnOrExtractor)
                else:
                    if literal_nones:
                        return None
                    else:
                        raise ValueError('No data found for %s from %s.%s' % (
                            str(_columnOrExtractor),
                            _keyspace2,
                            _tableName2,
                        ))
            
            return {
                field: _retrieve_field(_session, _keyspace, tableName, self.primaryKeyMap[tableName], columnOrExtractor, kwargs)
                for field, (tableName, columnOrExtractor) in field_mapper.items()
            }
        self.getter = _getter

    def __call__(self, deps, **kwargs):
        return self.getter(deps, **kwargs)
