"""
handling of key-value storage on a Cassandra table.
One row per partition, serializes a multiple partition key into a string
"""

from typing import Union, List, Any

from cassandra.cluster import Session

# CQL templates
_create_table_cql_template = """
CREATE TABLE IF NOT EXISTS {keyspace}.{table_name} (
    key_desc TEXT,
    cache_key TEXT,
    cache_value TEXT,
    PRIMARY KEY (( key_desc, cache_key ))
);
"""
_get_cached_item_cql_template = """
SELECT cache_value
    FROM {keyspace}.{table_name}
WHERE key_desc=%s
    AND cache_key=%s;
"""
_delete_cached_item_cql_template = """
DELETE FROM {keyspace}.{table_name}
WHERE key_desc=%s
    AND cache_key=%s;
"""
_storeCachedItemCQLTemplate = """
INSERT INTO {keyspace}.{table_name} (
    key_desc,
    cache_key,
    cache_value
) VALUES (
    %s,
    %s,
    %s
){ttlSpec};
"""
_truncate_table_cql_template = """
TRUNCATE TABLE {keyspace}.{table_name};
"""


class KVCache:

    def __init__(self, session: Session, keyspace: str, table_name: str, keys: List[Any]):
        self.session = session
        self.keyspace = keyspace
        self.table_name = table_name
        self.keys = keys
        self.key_desc = '/'.join(self.keys)
        # Schema creation, if needed
        cql = _create_table_cql_template.format(
            keyspace=self.keyspace,
            table_name=self.table_name,
        )
        session.execute(cql)

    def clear(self):
        cql = _truncate_table_cql_template.format(
            keyspace=self.keyspace,
            table_name=self.table_name,
        )
        self.session.execute(
            cql
        )

    def put(self, key_dict, cache_value, ttl_seconds):
        if ttl_seconds:
            ttl_spec = f' USING TTL {ttl_seconds}'
        else:
            ttl_spec = ''
        cache_key = self._serializeKey([
            key_dict[k]
            for k in self.keys
        ])
        cql = _storeCachedItemCQLTemplate.format(
            keyspace=self.keyspace,
            table_name=self.table_name,
            ttlSpec=ttl_spec,
        )
        self.session.execute(
            cql,
            (
                self.key_desc,
                cache_key,
                cache_value,
            ),
        )

    def get(self, key_dict) -> Union[None, str]:
        cache_key = self._serializeKey([
            key_dict[k]
            for k in self.keys
        ])
        cql = _get_cached_item_cql_template.format(
            keyspace=self.keyspace,
            table_name=self.table_name,
        )
        row = self.session.execute(
            cql,
            (self.key_desc, cache_key),
        ).one()
        if row:
            return row.cache_value
        else:
            return None

    def delete(self, key_dict) -> None:
        """ Will not complain if the row does not exist. """
        cache_key = self._serializeKey([
            key_dict[k]
            for k in self.keys
        ])
        cql = _delete_cached_item_cql_template.format(
            keyspace=self.keyspace,
            table_name=self.table_name,
        )
        self.session.execute(
            cql,
            (self.key_desc, cache_key),
        )

    def _serializeKey(self, keys: List[str]):
        return str(keys)
