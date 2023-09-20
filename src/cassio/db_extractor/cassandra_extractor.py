"""
An extractor able to resolve single-row lookups from Cassandra tables in
a keyspace, with a fair amount of metadata inspection.
"""

from functools import reduce
from itertools import groupby
from operator import itemgetter
from typing import (
    Any,
    Callable,
    cast,
    Dict,
    Iterable,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from cassandra.query import PreparedStatement  # type: ignore
from cassandra.concurrent import execute_concurrent, ExecutionResult  # type: ignore
from cassandra.cluster import Session  # type: ignore

from cassio.utils.db_inspection import table_partitionkey
from cassio.table.cql import SELECT_CQL_TEMPLATE


C = TypeVar("C")
ColumnOrFunctionType = Union[str, Callable[[Dict[str, Any]], C]]


def _ensure_full_extraction_tuple(
    tpl: Tuple[Any, ...], admit_nulls: bool
) -> Tuple[Any, ...]:
    if len(tpl) < 2:
        raise ValueError(
            "At least table and column names are required in the field_mapper."
        )
    elif len(tpl) == 2:
        return tuple(list(tpl) + [admit_nulls, None])
    elif len(tpl) == 3:
        return tuple(list(tpl) + [None])
    elif len(tpl) == 4:
        return tpl
    else:
        raise ValueError(
            "Cannot specify more than (table, column_or_function, "
            "admit_nulls, default) in the field_mapper."
        )


def _extract_first_row(e_r: ExecutionResult) -> Union[None, Dict[str, Any]]:
    """this acts on the items returned in a list by execute_concurrent."""
    if not e_r[0]:
        raise ValueError(f"Error reading from DB: {e_r[1]}")
    else:
        row = e_r[1].one()
        if row:
            if isinstance(row, dict):
                return row
            else:
                return cast(Dict[str, Any], row._asdict())
        else:
            return None


def _pick_value(
    field_name: str,
    row_dict: Optional[Dict[str, Any]],
    column_or_function: ColumnOrFunctionType[C],
    admit_nulls: bool,
    default: C,
) -> Union[C, None]:
    if row_dict is None:
        _v = None
    else:
        if callable(column_or_function):
            _v = column_or_function(row_dict)
        else:
            _v = cast(C, row_dict[column_or_function])
    #
    if _v is None:
        if admit_nulls:
            return default
        else:
            raise ValueError('Null data found for "%s"' % field_name)
    else:
        return _v


class CassandraExtractor:
    def __init__(
        self,
        session: Session,
        keyspace: str,
        field_mapper: Dict[str, Tuple[Any, ...]],
        admit_nulls: bool,
    ):
        self.session = session
        self.keyspace = keyspace
        #
        _field_mapper = {
            k: _ensure_full_extraction_tuple(v, admit_nulls)
            for k, v in field_mapper.items()
        }
        self.field_mapper = _field_mapper
        # Survey what columns (or '*') to query from which tables
        by_table = groupby(self.field_mapper.values(), itemgetter(0))
        _columns_by_table = {
            table_name: {
                col_name if not callable(col_name) else "*"
                for (_, col_name, _, _) in table_group
            }
            for table_name, table_group in by_table
        }
        self.columns_by_table = {
            table_name: sorted(column_name_set if "*" not in column_name_set else {"*"})
            for table_name, column_name_set in _columns_by_table.items()
        }
        # Prepare map of primary key columns needed per table
        self.table_names = sorted(self.columns_by_table.keys())
        self.primary_key_map = {
            table: sorted(
                [
                    col_name
                    for col_name, _ in table_partitionkey(
                        self.session,
                        self.keyspace,
                        table,
                    )
                ]
            )
            for table in self.table_names
        }
        # Prepare queries in a table_name -> statement map
        query_cql_map: Dict[str, str] = {
            table_name: SELECT_CQL_TEMPLATE.format(
                columns_desc=", ".join(sorted(column_name_set)),
                where_clause="WHERE "
                + " AND ".join(
                    f"{primary_key_col_name} = ?"
                    for primary_key_col_name in self.primary_key_map[table_name]
                ),
                limit_clause="LIMIT 1",
            ).format(table_fqname=f"{self.keyspace}.{table_name}")
            for table_name, column_name_set in self.columns_by_table.items()
        }
        self.query_statements: Dict[str, PreparedStatement] = {
            table_name: self.session.prepare(cql_statement)
            for table_name, cql_statement in query_cql_map.items()
        }
        # reduction across all tables to all primary-key values needed
        # (this merger function makes the type checker happy over a lambda)
        def _set_merger(s1: Iterable[str], s2: Iterable[str]) -> Set[str]:
            return {itm for s in (s1, s2) for itm in s}

        self.input_parameters: Set[str] = reduce(
            _set_merger,
            self.primary_key_map.values(),
            cast(Set[str], set()),
        )
        self.output_parameters = set(field_mapper.keys())

    def dictionary_based_call(self, args_dict: Dict[str, Any]) -> Dict[str, Any]:
        return self(**args_dict)

    def __call__(self, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        # prepare value tuples for the queries
        values_map = {
            table_name: tuple(kwargs[primary_key] for primary_key in primary_keys)
            for table_name, primary_keys in self.primary_key_map.items()
        }
        # launch the queries
        results0 = execute_concurrent(
            self.session,
            [
                (self.query_statements[table_name], values_map[table_name])
                for table_name in self.table_names
            ],
            raise_on_first_error=True,
            results_generator=False,
        )
        # normalize the results
        query_result_map = {
            table_name: _extract_first_row(exc_result)
            for table_name, exc_result in zip(self.table_names, results0)
        }
        # finalize as requested in the field mapper
        results = {
            output_field: _pick_value(
                output_field,
                query_result_map[table_name],
                column_or_function,
                admit_nulls,
                default,
            )
            for output_field, (
                table_name,
                column_or_function,
                admit_nulls,
                default,
            ) in self.field_mapper.items()
        }
        return results
