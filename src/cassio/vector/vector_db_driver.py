"""
A common driver to operate on tables with vector-similarity-search indices.
"""

import json
from operator import itemgetter

from cassandra.cluster import Session
from cassandra.query import SimpleStatement

from cassio.utils.vector.distance_metrics import distanceMetricsMap

_create_vector_db_table_cql_template = """
CREATE TABLE IF NOT EXISTS {keyspace}.{tableName} (
    document_id {idType} PRIMARY KEY,
    embedding_vector VECTOR<FLOAT, {embeddingDimension}>,
    document TEXT,
    metadata_blob TEXT
);
"""
_create_vector_db_table_index_cql_template = """
CREATE CUSTOM INDEX IF NOT EXISTS {indexName} ON {keyspace}.{tableName} (embedding_vector)
USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' ;
"""
_store_cached_vss_item_cql_template = """
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
){ttlSpec};
"""
_get_vector_db_table_item_cql_template = """
SELECT
    document_id, embedding_vector, document, metadata_blob
FROM {keyspace}.{tableName}
    WHERE document_id=%s;
"""
_search_vector_db_table_item_cql_template = """
SELECT
    document_id, embedding_vector, document, metadata_blob
FROM {keyspace}.{tableName}
    ORDER BY embedding_vector ANN OF %s
    LIMIT %s
    ALLOW FILTERING;
"""

_truncate_vector_db_table_cql_template = """
TRUNCATE TABLE {keyspace}.{tableName};
"""
_delete_vector_db_table_item_cql_template = """
DELETE FROM {keyspace}.{tableName}
WHERE document_id = %s;
"""
_count_rows_cql_template = """
    SELECT COUNT(*) FROM {keyspace}.{tableName};
"""


class VectorMixin:
    def _create_index(self):
        index_name = f'{self.table_name}_embedding_idx'
        cql = SimpleStatement(_create_vector_db_table_index_cql_template.format(
            indexName=index_name,
            keyspace=self.keyspace,
            tableName=self.table_name
        ))
        self._execute_cql(cql, tuple())

    def ann_search(self, embedding_vector, numRows):
        cql = SimpleStatement(_search_vector_db_table_item_cql_template.format(
            keyspace=self.keyspace,
            tableName=self.table_name
        ))
        return self._execute_cql(cql, (embedding_vector, numRows))

    def _count_rows(self):
        cql = SimpleStatement(_count_rows_cql_template.format(
            keyspace=self.keyspace,
            tableName=self.table_name
        ))
        return self._execute_cql(cql, tuple()).one().count


class VectorTable(VectorMixin):

    def __init__(self, session: Session, keyspace: str, table_name: str, embedding_dimension: int, auto_id: bool):
        self.session = session
        self.keyspace = keyspace
        self.table_name = table_name
        self.embedding_dimension = embedding_dimension
        #
        self.auto_id = auto_id
        #
        self._create_table()
        self._create_index()

    def put(self, document, embedding_vector, document_id, metadata, ttl_seconds):
        # document_id, if not autoID, must be str
        if not self.auto_id and document_id is None:
            raise ValueError('\'document_id\' must be specified unless autoID')
        if self.auto_id and document_id is not None:
            raise ValueError('\'document_id\' cannot be passes if autoID')
        if ttl_seconds:
            ttl_spec = f' USING TTL {ttl_seconds}'
        else:
            ttl_spec = ''
        cql = SimpleStatement(_store_cached_vss_item_cql_template.format(
            keyspace=self.keyspace,
            tableName=self.table_name,
            documentIdPlaceholder='now()' if self.auto_id else '%s',
            ttlSpec=ttl_spec,
        ))
        metadata_blob = json.dumps(metadata)
        # depending on autoID, the size of the values tuple changes:
        values0 = (embedding_vector, document, metadata_blob)
        values = values0 if self.auto_id else tuple([document_id] + list(values0))
        self._execute_cql(cql, values)

    def get(self, document_id):
        if self.auto_id:
            raise ValueError('\'get\' not supported if autoID')
        else:
            cql = SimpleStatement(_get_vector_db_table_item_cql_template.format(
                keyspace=self.keyspace,
                tableName=self.table_name,
            ))
            hits = self._execute_cql(cql, (document_id, ))
            hit = hits.one()
            if hit:
                return VectorTable._jsonifyHit(hit, distance=None)
            else:
                return None

    def delete(self, document_id) -> None:
        """This operation goes through even if the row does not exist."""
        cql = SimpleStatement(_delete_vector_db_table_item_cql_template.format(
            keyspace=self.keyspace,
            tableName=self.table_name,
        ))
        self._execute_cql(cql, (document_id, ))

    def search(self, embedding_vector, top_k, max_rows_to_retrieve, metric, metric_threshold):
        # get rows by ANN
        rows = list(self.ann_search(embedding_vector, max_rows_to_retrieve))
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
            # sort rows by metric score. First handle metric/threshold
            if metric_threshold is not None:
                if distanceFunction[1]:
                    def _thresholder(mtx, thr): return mtx >= thr
                else:
                    def _thresholder(mtx, thr): return mtx <= thr
            else:
                # no hits are discarded
                def _thresholder(mtx, thr): return True
            #
            sortedPassingWinners = sorted(
                (
                    pair
                    for pair in rowsWithMetric
                    if _thresholder(pair[0], metric_threshold)
                ),
                key=itemgetter(0),
                reverse=distanceFunction[1],
            )[:top_k]
            # we discard the scores and return an iterable of hits (as JSONs)
            return [
                VectorTable._jsonifyHit(hit, distance=distance)
                for distance, hit in sortedPassingWinners
            ]
        else:
            return []

    @staticmethod
    def _jsonifyHit(hit, distance):
        if distance is not None:
            distDict = {'distance': distance}
        else:
            distDict = {}
        return {
            **{
                'document_id': hit.document_id,
                'metadata': json.loads(hit.metadata_blob),
                'document': hit.document,
                'embedding_vector': hit.embedding_vector,
            },
            **distDict,
        }

    def clear(self):
        cql = SimpleStatement(_truncate_vector_db_table_cql_template.format(
            keyspace=self.keyspace,
            tableName=self.table_name,
        ))
        self._execute_cql(cql, tuple())

    def _create_table(self):
        cql = SimpleStatement(_create_vector_db_table_cql_template.format(
            keyspace=self.keyspace,
            tableName=self.table_name,
            idType='UUID' if self.auto_id else 'TEXT',
            embeddingDimension=self.embedding_dimension,
        ))
        self._execute_cql(cql, tuple())

    def _execute_cql(self, statement, params):
        return self.session.execute(statement, params)
