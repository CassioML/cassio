import inspect
from operator import itemgetter
from typing import Any, Awaitable, Iterable, List, Optional, Tuple, Union

from cassandra.cluster import ResponseFuture

from cassio.table.base_table import BaseTable
from cassio.table.cql import SELECT_ANN_CQL_TEMPLATE, CQLOpType
from cassio.table.table_types import ColumnSpecType, RowType, RowWithDistanceType
from cassio.utils.vector.distance_metrics import distance_metrics

from .base_table import BaseTableMixin


class VectorMixin(BaseTableMixin):
    def __init__(
        self,
        *pargs: Any,
        vector_dimension: Union[int, Awaitable[int]],
        vector_similarity_function: Optional[str] = None,
        vector_source_model: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        if inspect.isawaitable(vector_dimension) and not kwargs.get(
            "async_setup", False
        ):
            raise ValueError(
                "Cannot use an awaitable embedding_dimension "
                "with async_setup set to False"
            )
        self.vector_dimension = vector_dimension
        self.vector_index_options = []
        if vector_similarity_function is not None:
            self.vector_index_options.append(
                ("similarity_function", vector_similarity_function)
            )
        if vector_source_model is not None:
            self.vector_index_options.append(("source_model", vector_source_model))
        super().__init__(*pargs, **kwargs)

    def _schema_da(self) -> List[ColumnSpecType]:
        return super()._schema_da() + [
            ("vector", f"VECTOR<FLOAT,{self.vector_dimension}>")
        ]

    async def _aschema_da(self) -> List[ColumnSpecType]:
        if inspect.isawaitable(self.vector_dimension):
            self.vector_dimension = await self.vector_dimension
        return self._schema_da()

    @staticmethod
    def _get_create_vector_index_cql(
        vector_index_options: List[Tuple[str, Any]]
    ) -> str:
        index_name = "idx_vector"
        index_column = "vector"
        return BaseTable._get_create_index_cql(
            index_name=index_name,
            index_column=index_column,
            index_options=vector_index_options,
        )

    def db_setup(self) -> None:
        super().db_setup()
        # index on the vector column:
        create_index_cql = self._get_create_vector_index_cql(self.vector_index_options)
        self.execute_cql(create_index_cql, op_type=CQLOpType.SCHEMA)

    async def adb_setup(self) -> None:
        await super().adb_setup()
        # index on the vector column:
        create_index_cql = self._get_create_vector_index_cql(self.vector_index_options)
        await self.aexecute_cql(create_index_cql, op_type=CQLOpType.SCHEMA)

    def _get_ann_search_cql(
        self, vector: List[float], n: int, **kwargs: Any
    ) -> Tuple[str, Tuple[Any, ...]]:
        n_kwargs = self._normalize_kwargs(kwargs, is_write=False)
        # TODO: work on a columns: Optional[List[str]] = None
        # (but with nuanced handling of the column-magic we have here)
        columns = None
        if columns is None:
            columns_desc = "*"
        else:
            # TODO: handle translations here?
            # columns_desc = ", ".join(columns)
            raise NotImplementedError("Column selection is not implemented.")
        #
        if all(x == 0 for x in vector):
            # TODO: lift/relax this constraint when non-cosine metrics are there.
            raise ValueError("Cannot use identically-zero vectors in cos/ANN search.")
        #
        vector_column = "vector"
        vector_cql_vals = (vector,)
        #
        (
            rest_kwargs,
            where_clause_blocks,
            where_cql_vals,
        ) = self._extract_where_clause_blocks(n_kwargs)

        (
            rest_kwargs,
            analyzer_clause_blocks,
            analyzer_cql_vals,
        ) = self._extract_index_analyzers(rest_kwargs)

        assert rest_kwargs == {}

        all_where_clauses = where_clause_blocks + analyzer_clause_blocks
        if not all_where_clauses:
            where_clause = ""
        else:
            where_clause = "WHERE " + " AND ".join(all_where_clauses)
        #
        limit_clause = "LIMIT %s"
        limit_cql_vals = (n,)
        #
        select_ann_cql = SELECT_ANN_CQL_TEMPLATE.format(
            columns_desc=columns_desc,
            vector_column=vector_column,
            where_clause=where_clause,
            limit_clause=limit_clause,
        )
        #
        select_ann_cql_vals = (
            where_cql_vals + analyzer_cql_vals + vector_cql_vals + limit_cql_vals
        )

        return select_ann_cql, select_ann_cql_vals

    def ann_search(
        self, vector: List[float], n: int, **kwargs: Any
    ) -> Iterable[RowType]:
        select_ann_cql, select_ann_cql_vals = self._get_ann_search_cql(
            vector, n, **kwargs
        )
        result_set = self.execute_cql(
            select_ann_cql, args=select_ann_cql_vals, op_type=CQLOpType.READ
        )
        return (self._normalize_row(result) for result in result_set)

    def ann_search_async(
        self, vector: List[float], n: int, **kwargs: Any
    ) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")

    async def aann_search(
        self, vector: List[float], n: int, **kwargs: Any
    ) -> Iterable[RowType]:
        select_ann_cql, select_ann_cql_vals = self._get_ann_search_cql(
            vector, n, **kwargs
        )
        result_set = await self.aexecute_cql(
            select_ann_cql, args=select_ann_cql_vals, op_type=CQLOpType.READ
        )
        return (self._normalize_row(result) for result in result_set)

    @staticmethod
    def _get_rows_with_distance(
        rows: Iterable[RowType],
        vector: List[float],
        metric: str,
        metric_threshold: Optional[float] = None,
    ) -> Iterable[RowWithDistanceType]:
        if rows == []:
            return []
        else:
            # sort, cut, validate and prepare for returning
            # evaluate metric
            distance_function, distance_reversed = distance_metrics[metric]
            row_vectors = [row["vector"] for row in rows]
            # enrich with their metric score
            rows_with_metric = list(
                zip(
                    distance_function(row_vectors, vector),
                    rows,
                )
            )
            # sort rows by metric score. First handle metric/threshold
            if metric_threshold is not None:
                _used_thr = metric_threshold
                if distance_reversed:

                    def _thresholder(mtx: float, thr: float) -> bool:
                        return mtx >= thr

                else:

                    def _thresholder(mtx: float, thr: float) -> bool:
                        return mtx <= thr

            else:
                # this to satisfy the type checker
                _used_thr = 0.0

                # no hits are discarded
                def _thresholder(mtx: float, thr: float) -> bool:
                    return True

            #
            sorted_passing_rows = sorted(
                (pair for pair in rows_with_metric if _thresholder(pair[0], _used_thr)),
                key=itemgetter(0),
                reverse=distance_reversed,
            )
            # return a list of hits with their distance (as JSON)
            enriched_hits = (
                {
                    **hit,
                    **{"distance": distance},
                }
                for distance, hit in sorted_passing_rows
            )
            return enriched_hits

    def metric_ann_search(
        self,
        vector: List[float],
        n: int,
        metric: str,
        metric_threshold: Optional[float] = None,
        **kwargs: Any,
    ) -> Iterable[RowWithDistanceType]:
        rows = list(self.ann_search(vector, n, **kwargs))
        return self._get_rows_with_distance(rows, vector, metric, metric_threshold)

    def metric_ann_search_async(
        self, vector: List[float], n: int, **kwargs: Any
    ) -> ResponseFuture:
        raise NotImplementedError("Asynchronous reads are not supported.")

    async def ametric_ann_search(
        self,
        vector: List[float],
        n: int,
        metric: str,
        metric_threshold: Optional[float] = None,
        **kwargs: Any,
    ) -> Iterable[RowWithDistanceType]:
        rows = list(await self.aann_search(vector, n, **kwargs))
        return self._get_rows_with_distance(rows, vector, metric, metric_threshold)
