"""
A common driver to operate on tables with vector-similarity-search indices.
"""

from operator import itemgetter
import json

from cassandra.cluster import Session
from cassandra.query import SimpleStatement

from cassio.globals.globals import globals
from cassio.utils.vector.distance_metrics import distanceMetricsMap

EXPERIMENTAL_VECTOR_SEARCH_ERROR = (
    "Vector search is an experimental feature and should "
    "be first enabled with 'cassio.globals.enableExperimentalVectorSearch()`"
)

_createVectorDBTableCQLTemplate = """
CREATE TABLE IF NOT EXISTS {keyspace}.{tableName} (
    document_id {idType} PRIMARY KEY,
    embedding_vector FLOAT VECTOR[{embeddingDimension}],
    document TEXT,
    metadata_blob TEXT
);
"""
_createVectorDBTableIndexCQLTemplate = """
CREATE CUSTOM INDEX IF NOT EXISTS {indexName} ON {keyspace}.{tableName} (embedding_vector)
USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' ;
"""
_storeCachedVSSItemCQLTemplate = """
INSERT INTO {keyspace}.{tableName} (
    document_id,
    embedding_vector,
    document,
    metadata_blob
) VALUES (
    {documentIdPlaceholder},
    %s,
    %s,
    %s
);
"""
_getVectorDBTableItemCQLTemplate = """
SELECT
    document_id, embedding_vector, document, metadata_blob
FROM {keyspace}.{tableName}
    WHERE document_id=%s;
"""
_searchVectorDBTableItemCQLTemplate = """
SELECT
    embedding_vector, document, metadata_blob
FROM {keyspace}.{tableName}
    WHERE embedding_vector ANN OF %s
    LIMIT %s
"""
_truncateVectorDBTableCQLTemplate = """
TRUNCATE TABLE {keyspace}.{tableName};
"""


class VectorDBMixin():

    def _createIndex(self):
        indexName = f'{self.tableName}_embedding_idx'
        createVectorDBTableIndexCQL = SimpleStatement(_createVectorDBTableIndexCQLTemplate.format(
            indexName=indexName,
            keyspace=self.keyspace,
            tableName=self.tableName
        ))
        self._executeCQL(createVectorDBTableIndexCQL, tuple())

    def ANNSearch(self, embedding_vector, numRows):
        searchVectorDBTableItemCQL = SimpleStatement(_searchVectorDBTableItemCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName
        ))
        return self._executeCQL(searchVectorDBTableItemCQL, (embedding_vector, numRows))


class VectorDBTable(VectorDBMixin):

    def __init__(self, session: Session, keyspace: str, tableName: str, embeddingDimension: int, autoID: bool = True):
        if not globals.experimentalVectorSearch:
            raise RuntimeError(EXPERIMENTAL_VECTOR_SEARCH_ERROR)
        #
        self.session = session
        self.keyspace = keyspace
        self.tableName = tableName
        self.embeddingDimension = embeddingDimension
        #
        self.autoID = autoID
        #
        self._createTable()
        self._createIndex()

    def put(self, document, embedding_vector, document_id=None, metadata={}):
        # document_id, if not autoID, must be str
        if not self.autoID and document_id is None:
            raise ValueError('\'document_id\' must be specified unless autoID')
        if self.autoID and document_id is not None:
            raise ValueError('\'document_id\' cannot be passes if autoID')
        storeCachedVSSItemCQL = SimpleStatement(_storeCachedVSSItemCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
            documentIdPlaceholder='now()' if self.autoID else '%s',
        ))
        metadataBlob = json.dumps(metadata)
        values0 = (embedding_vector, document, metadataBlob)
        values = values0 if self.autoID else tuple([document_id] + list(values0))
        self._executeCQL(storeCachedVSSItemCQL, values)

    def get(self, document_id):
        if self.autoID:
            raise ValueError('\'get\' not supported if autoID')
        else:
            getVectorDBTableItemCQL = SimpleStatement(_getVectorDBTableItemCQLTemplate.format(
                keyspace=self.keyspace,
                tableName=self.tableName,
            ))
            hits = self._executeCQL(getVectorDBTableItemCQL, (document_id, ))
            hit = hits.one()
            if hit:
                return VectorDBTable._jsonifyHit(hit)
            else:
                return None

    def search(self, embedding_vector, topK, maxRowsToRetrieve, metric, metricThreshold):
        # get rows by ANN
        rows = list(self.ANNSearch(embedding_vector, maxRowsToRetrieve))
        if rows:
            # sort, cut, validate and prepare for returning (if any)
            #
            # evaluate metric
            distanceFunction = distanceMetricsMap[metric]
            rowEmbeddings = [
                row.embedding_vector
                for row in rows
            ]
            # enrich with their metric score
            rowsWithMetric = list(zip(
                distanceFunction[0](rowEmbeddings, embedding_vector),
                rows,
            ))
            # sort rows by metric score
            if distanceFunction[1]:
                def _thresholder(mtx, thr): return mtx >= thr
            else:
                def _thresholder(mtx, thr): return mtx <= thr
            #
            sortedPassingWinners = sorted(
                (
                    pair
                    for pair in rowsWithMetric
                    if _thresholder(pair[0], metricThreshold)
                ),
                key=itemgetter(0),
                reverse=distanceFunction[1],
            )[:topK]
            # we discard the scores and return an iterable of hits (as JSONs)
            return [
                VectorDBTable._jsonifyHit(hit)
                for _, hit in sortedPassingWinners
            ]
        else:
            return []

    @staticmethod
    def _jsonifyHit(hit):
        return {
            'metadata': json.loads(hit.metadata_blob),
            'document': hit.document,
            'embedding_vector': hit.embedding_vector,
        }

    def clear(self):
        truncateVectorDBTableCQL = SimpleStatement(_truncateVectorDBTableCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
        ))
        self._executeCQL(truncateVectorDBTableCQL, tuple())

    def _createTable(self):
        createVectorDBTableCQL = SimpleStatement(_createVectorDBTableCQLTemplate.format(
            keyspace=self.keyspace,
            tableName=self.tableName,
            idType='UUID' if self.autoID else 'TEXT',
            embeddingDimension=self.embeddingDimension,
        ))
        self._executeCQL(createVectorDBTableCQL, tuple())

    def _executeCQL(self, statement, params):
        return self.session.execute(statement, params)
