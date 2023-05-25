"""
handling of key-value storage on a Cassandra table.
One row per partition, serializes a multiple partition key into a string
"""

from typing import Union, List, Any

from cassandra.cluster import Session

# CQL templates
_createTableCQLTemplate = """
CREATE TABLE IF NOT EXISTS {keyspace}.{tableName} (
    key_desc TEXT,
    cache_key TEXT,
    cache_value TEXT,
    PRIMARY KEY (( key_desc, cache_key ))
);
"""
_getCachedItemCQLTemplate = """
SELECT cache_value
    FROM {keyspace}.{tableName}
WHERE key_desc=%s
    AND cache_key=%s;
"""
_deleteCachedItemCQLTemplate = """
DELETE FROM {keyspace}.{tableName}
WHERE key_desc=%s
    AND cache_key=%s;
"""
_storeCachedItemCQLTemplate = """
INSERT INTO {keyspace}.{tableName} (
    key_desc,
    cache_key,
    cache_value
) VALUES (
    %s,
    %s,
    %s
){ttlSpec};
"""
_truncateTableCQLTemplate = """
TRUNCATE TABLE {keyspace}.{tableName};
"""


class KVCache():

    def __init__(self, session: Session, keyspace: str, tableName: str, keys: List[Any]):
        self.session = session
        self.keyspace = keyspace
        self.tableName = tableName
        self.keys = keys
        self.keyDesc = '/'.join(self.keys)
        # Schema creation, if needed
        createTableCQL = _createTableCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
        )
        session.execute(createTableCQL)

    def clear(self):
        truncateTableCQL = _truncateTableCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
        )
        self.session.execute(
            truncateTableCQL
        )

    def put(self, keyDict, cacheValue, ttlSeconds):
        if ttlSeconds:
            ttlSpec = f' USING TTL {ttlSeconds}'
        else:
            ttlSpec = ''
        cacheKey = self._serializeKey([
            keyDict[k]
            for k in self.keys
        ])
        storeCachedItemCQL = _storeCachedItemCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
            ttlSpec=ttlSpec,
        )
        self.session.execute(
            storeCachedItemCQL,
            (
                self.keyDesc,
                cacheKey,
                cacheValue,
            ),
        )

    def get(self, keyDict) -> Union[None, str]:
        cacheKey = self._serializeKey([
            keyDict[k]
            for k in self.keys
        ])
        getCachedItemCQL = _getCachedItemCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
        )
        foundRow = self.session.execute(
            getCachedItemCQL,
            (self.keyDesc, cacheKey),
        ).one()
        if foundRow:
            return foundRow.cache_value
        else:
            return None

    def delete(self, keyDict) -> None:
        """ Will not complain if the row does not exist. """
        cacheKey = self._serializeKey([
            keyDict[k]
            for k in self.keys
        ])
        deleteCachedItemCQL = _deleteCachedItemCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
        )
        self.session.execute(
            deleteCachedItemCQL,
            (self.keyDesc, cacheKey),
        )

    def _serializeKey(self, keys: List[str]):
        return str(keys)
