"""
Extractor (name tbd) test TODO
"""

import pytest

from cassio.db_extractor import CassandraExtractor


@pytest.mark.usefixtures("db_session", "db_keyspace")
class TestCassandraExtractor:
    """
    TODO
    """

    def test_extractor(self, db_session, db_keyspace):
        p_table_name1 = "people"
        # TODO: drop, create, populate tables as class fixture
        f_mapper = {
            'r_age': ('people', 'age'),
            'r_age2': ('people', 'age'),
            'r_name': ('people', 'name'),
            'r_nickname': ('nickname_by_city', 'nickname'),
            'r_nickname2': ('nickname_by_city', 'nickname'),
            'r_nickname3': ('nickname_by_city', lambda row: row['nickname'].upper()),
            'r_city': ('nickname_by_city', 'city'),
        }
        ext = CassandraExtractor(
            session=db_session,
            keyspace=db_keyspace,
            field_mapper=f_mapper,
            admit_nulls=False,
        )
        res1 = ext(city='milan', name='alba')
        assert res1 == {
            'r_age': 11,
            'r_age2': 11,
            'r_name': 'alba',
            'r_nickname': 'Taaac',
            'r_nickname2': 'Taaac',
            'r_nickname3': 'TAAAC',
            'r_city': 'milan',
        }
        res_d = ext.dictionary_based_call({'city': 'milan', 'name': 'alba'})
        assert res_d == res1

    def test_admit_nulls(self, db_session, db_keyspace):
        p_table_name1 = "people"
        f_mapper = {
            'r_age_t': ('people', 'age', True),
            'r_age_t_d': ('people', 'age', True, 999),
            'r_age': ('people', 'age'),
        }
        ext_f = CassandraExtractor(
            session=db_session,
            keyspace=db_keyspace,
            field_mapper=f_mapper,
            admit_nulls=False,
        )
        res_f = ext_f(city='milan', name='alba')
        assert res_f == {
            'r_age_t': 11,
            'r_age_t_d': 11,
            'r_age': 11,
        }
        with pytest.raises(ValueError):
            _ = ext_f(city='milan', name='albax')
        #
        ext_t = CassandraExtractor(
            session=db_session,
            keyspace=db_keyspace,
            field_mapper=f_mapper,
            admit_nulls=True,
        )
        res_t = ext_t(city='milanx', name='albax')
        assert res_t == {
            'r_age_t': None,
            'r_age_t_d': 999,
            'r_age': None,
        }


if __name__ == "__main__":
    from ..conftest import createDBSessionSingleton, getDBKeyspace

    s = createDBSessionSingleton()
    k = getDBKeyspace()

    TestCassandraExtractor().test_extractor(s, k)
