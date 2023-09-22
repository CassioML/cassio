"""
Test for the extractor (single rows from several values, picking columns
and performing client-side joins hiding the table schema away).
"""

import pytest

from cassio.db_reader import MultiTableCassandraReader


@pytest.mark.usefixtures("db_session", "db_keyspace", "extractor_tables")
class TestMultiTableCassandraReader:
    """
    Tests for the extractor.
    """

    def test_extractor(self, db_session, db_keyspace, extractor_tables):
        p_table, c_table = extractor_tables
        f_mapper = {
            "r_age": (p_table, "age"),
            "r_age2": (p_table, "age"),
            "r_name": (p_table, "name"),
            "r_nickname": (c_table, "nickname"),
            "r_nickname2": (c_table, "nickname"),
            "r_nickname3": (c_table, lambda row: row["nickname"].upper()),
            "r_city": (c_table, "city"),
        }
        ext = MultiTableCassandraReader(
            session=db_session,
            keyspace=db_keyspace,
            field_mapper=f_mapper,
            admit_nulls=False,
        )
        res1 = ext(city="milan", name="alba")
        assert res1 == {
            "r_age": 11,
            "r_age2": 11,
            "r_name": "alba",
            "r_nickname": "Taaac",
            "r_nickname2": "Taaac",
            "r_nickname3": "TAAAC",
            "r_city": "milan",
        }
        res_d = ext.dictionary_based_call({"city": "milan", "name": "alba"})
        assert res_d == res1
        #
        assert set(ext.table_names) == {p_table, c_table}
        assert set(ext.input_parameters) == {"city", "name"}
        assert set(ext.output_parameters) == {
            "r_age",
            "r_age2",
            "r_name",
            "r_nickname",
            "r_nickname2",
            "r_nickname3",
            "r_city",
        }

    def test_admit_nulls(self, db_session, db_keyspace, extractor_tables):
        p_table, c_table = extractor_tables
        f_mapper = {
            "r_age_t": (p_table, "age", True),
            "r_age_t_d": (p_table, "age", True, 999),
            "r_age": (p_table, "age"),
        }
        ext_f = MultiTableCassandraReader(
            session=db_session,
            keyspace=db_keyspace,
            field_mapper=f_mapper,
            admit_nulls=False,
        )
        res_f = ext_f(city="milan", name="alba")
        assert res_f == {
            "r_age_t": 11,
            "r_age_t_d": 11,
            "r_age": 11,
        }
        with pytest.raises(ValueError):
            _ = ext_f(city="milan", name="albax")
        #
        ext_t = MultiTableCassandraReader(
            session=db_session,
            keyspace=db_keyspace,
            field_mapper=f_mapper,
            admit_nulls=True,
        )
        res_t = ext_t(city="milanx", name="albax")
        assert res_t == {
            "r_age_t": None,
            "r_age_t_d": 999,
            "r_age": None,
        }
