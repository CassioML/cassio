"""
management of "history" of stored blobs, grouped
by some 'session id'. Overwrites are not supported by design.
"""

from cassandra.query import SimpleStatement

_createTableCQLTemplate = """
CREATE TABLE IF NOT EXISTS {keyspace}.{tableName} (
    session_id TEXT,
    blob_id TIMEUUID,
    blob TEXT,
    PRIMARY KEY (( session_id ) , blob_id )
) WITH CLUSTERING ORDER BY (blob_id ASC);
"""
_getSessionBlobsCQLTemplate = """
SELECT blob
    FROM {keyspace}.{tableName}
WHERE session_id=%s;
"""
_storeSessionBlobCQLTemplate = """
INSERT INTO {keyspace}.{tableName} (
    session_id,
    blob_id,
    blob
) VALUES (
    %s,
    now(),
    %s
){ttlSpec};
"""
_clearSessionCQLTemplate = """
DELETE FROM {keyspace}.{tableName} WHERE session_id = %s;
"""


class StoredBlobHistory():

    def __init__(self, session, keyspace, tableName):
        self.session = session
        self.keyspace = keyspace
        self.tableName = tableName
        # Schema creation, if needed
        createTableCQL = SimpleStatement(_createTableCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
        ))
        session.execute(createTableCQL)

    def store(self, sessionId, blob, ttlSeconds):
        if ttlSeconds:
            ttlSpec = f' USING TTL {ttlSeconds}'
        else:
            ttlSpec = ''
        #
        storeSessionBlobCQL = SimpleStatement(_storeSessionBlobCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
            ttlSpec=ttlSpec,
        ))
        self.session.execute(
            storeSessionBlobCQL,
            (
                sessionId,
                blob,
            )
        )

    def retrieve(self, sessionId, maxCount=None):
        pass
        getSessionBlobsCQL = SimpleStatement(_getSessionBlobsCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
        ))
        blobRows = self.session.execute(
            getSessionBlobsCQL,
            (sessionId, )
        )
        return (
            row.blob
            for row in blobRows
        )

    def clearSessionId(self, sessionId):
        pass
        clearSessionCQL = SimpleStatement(_clearSessionCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
        ))
        self.session.execute(clearSessionCQL, (sessionId, ))
