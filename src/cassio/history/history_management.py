"""
management of "history" of stored blobs, grouped
by some 'session id'. Overwrites are not supported by design.
"""

from cassandra.query import SimpleStatement

_create_table_cql_template = """
CREATE TABLE IF NOT EXISTS {keyspace}.{tableName} (
    session_id TEXT,
    blob_id TIMEUUID,
    blob TEXT,
    PRIMARY KEY (( session_id ) , blob_id )
) WITH CLUSTERING ORDER BY (blob_id ASC)
"""
_get_session_blobs_cql_template = """
SELECT blob
    FROM {keyspace}.{tableName}
WHERE session_id=%s
"""
_store_session_blob_cql_template = """
INSERT INTO {keyspace}.{tableName} (
    session_id,
    blob_id,
    blob
) VALUES (
    %s,
    now(),
    %s
){ttlSpec}
"""
_clear_session_cql_template = """
DELETE FROM {keyspace}.{tableName} WHERE session_id = %s
"""


class StoredBlobHistory:

    def __init__(self, session, keyspace, table_name):
        self.session = session
        self.keyspace = keyspace
        self.table_name = table_name
        # Schema creation, if needed
        cql = SimpleStatement(_create_table_cql_template.format(
            keyspace=self.keyspace,
            tableName=self.table_name,
        ))
        session.execute(cql)

    def store(self, session_id, blob, ttl_seconds):
        if ttl_seconds:
            ttl_spec = f' USING TTL {ttl_seconds}'
        else:
            ttl_spec = ''
        #
        cql = SimpleStatement(_store_session_blob_cql_template.format(
            keyspace=self.keyspace,
            tableName=self.table_name,
            ttlSpec=ttl_spec,
        ))
        self.session.execute(
            cql,
            (
                session_id,
                blob,
            )
        )

    def retrieve(self, session_id, max_count=None):
        pass
        cql = SimpleStatement(_get_session_blobs_cql_template.format(
            keyspace=self.keyspace,
            tableName=self.table_name,
        ))
        rows = self.session.execute(
            cql,
            (session_id,)
        )
        return (
            row.blob
            for row in rows
        )

    def clear_session_id(self, session_id):
        pass
        cql = SimpleStatement(_clear_session_cql_template.format(
            keyspace=self.keyspace,
            tableName=self.table_name,
        ))
        self.session.execute(cql, (session_id,))
