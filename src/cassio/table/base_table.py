from typing import List

class BaseTable():

    def __init__(self, session, keyspace, table_name):
        self.session = session
        self.keyspace = keyspace
        self.table_name = table_name

    @classmethod
    def _schema_row_id(cls):
        return [
            'row_id'
        ]

    @classmethod
    def _schema_pk(cls):
        return cls._schema_row_id()

    @classmethod
    def _schema_cc(cls):
        return []

    @classmethod
    def _schema_da(cls):
        return [
            'body_blob',
        ]

    @classmethod
    def _schema(cls):
        return {
            'pk': cls._schema_pk(),
            'cc': cls._schema_cc(),
            'da': cls._schema_da(),
        }

    @classmethod
    def _schema_primary_key(cls):
        return cls._schema_pk() + cls._schema_cc()

    @classmethod
    def _schema_collist(cls):
        full_list = cls._schema_da() + cls._schema_cc() + cls._schema_pk()
        return full_list

    @classmethod
    def _schema_colset(cls):
        full_set = set(full_list)
        assert(len(full_list) == len(full_set))
        return full_set

    def _desc_table(self):
        columns = self._schema()
        col_str = '[(' + ', '.join(columns['pk']) \
            + ') ' + ', '.join(columns['cc']) \
            + '] ' + ', '.join(columns['da'])
        return col_str

    def delete(self, **kwargs):
        primary_key = self._schema_primary_key()
        assert(set(kwargs.keys()) == set(primary_key))
        delete_cql = f"DELETE_ROW: ({', '.join(primary_key)})"
        delete_cql_vals = tuple(kwargs[c] for c in primary_key)
        self.execute_cql(delete_cql, delete_cql_vals)

    def clear(self):
        self.execute_cql('TRUNCATE', tuple())

    def get(self, **kwargs):
        primary_key = self._schema_primary_key()
        assert(set(kwargs.keys()) == set(primary_key))
        get_cql = f"GET_ROW: ({', '.join(primary_key)})"
        get_cql_vals = tuple(kwargs[c] for c in primary_key)
        return self.execute_cql(get_cql, get_cql_vals)

    def put(self, **kwargs):
        primary_key = self._schema_primary_key()
        assert(set(primary_key) - set(kwargs.keys()) == set())
        columns = [col for col in self._schema_collist() if col in kwargs]
        col_vals = tuple([kwargs[col] for col in columns])
        put_cql = f"PUT_ROW: ({', '.join(columns)})"
        self.execute_cql(put_cql, col_vals)

    def db_setup(self):
        self.execute_cql(f"MKTABLE: {self._desc_table()}")

    def execute_cql(self, query, args=tuple()):
        cls_name = self.__class__.__name__
        ftqual = f"{self.keyspace}.{self.table_name}"
        print(f"CQL({cls_name:<32}/{ftqual}) '{query}' {str(args)}")
        return []


class BaseTableMixin():

    @classmethod
    def _schema_pk(cls):
        return super()._schema_pk()

    @classmethod
    def _schema_cc(cls):
        return super()._schema_cc()

    @classmethod
    def _schema_da(cls):
        return super()._schema_da()


class ClusteredMixin(BaseTableMixin):

    @classmethod
    def _schema_pk(cls):
        return [
            'partition_id',
        ]

    @classmethod
    def _schema_cc(cls):
        return cls._schema_row_id()

    def delete_partition(self, partition_id):
        delete_p_cql = 'DELETE_PARTITION: (partition_id)'
        delete_p_cql_vals = (partition_id, )
        self.execute_cql(delete_p_cql, delete_p_cql_vals)

    def get_partition(self, partition_id, n = None):
        if n is None:
            get_p_cql = 'GET_PARTITION: (partition_id)'
            get_p_cql_vals = (partition_id, )
        else:
            get_p_cql = 'GET_PARTITION: (partition_id) LIMIT (n)'
            get_p_cql_vals = (partition_id, n)
        self.execute_cql(get_p_cql, get_p_cql_vals)


class MetadataMixin(BaseTableMixin):

    @classmethod
    def _schema_da(cls):
        return super()._schema_da() + [
            'metadata_s',
            'metadata_n',
            'metadata_tags',
        ]

    def db_setup(self):
        super().db_setup()
        self.execute_cql('CREATE_METADATA_SAIs')

    def _split_metadata(self, md_dict):
        # TODO: more care about types here
        stringy_part = {
            k: v
            for k, v in md_dict.items()
            if isinstance(v, str)
        }
        numeric_part = {
            k: float(v)
            for k, v in md_dict.items()
            if isinstance(v, int) or isinstance(v, float)
            if not isinstance(v, bool)
        }
        # these become 'tags'
        nully_part = {
            k
            for k, v in md_dict.items()
            if isinstance(v, bool) and v is True
        }
        assert(set(stringy_part.keys()) | set(numeric_part.keys()) | nully_part == set(md_dict.keys()))
        assert(len(stringy_part.keys()) + len(numeric_part.keys()) + len(nully_part) == len(md_dict.keys()))
        return {
            'metadata_s': stringy_part,
            'metadata_n': numeric_part,
            'metadata_tags': nully_part,
        }

    def put(self, /, **kwargs):
        if 'metadata' in kwargs:
            new_metadata_fields = self._split_metadata(kwargs['metadata'])
        else:
            new_metadata_fields = {}
        #
        new_kwargs = {
            **{
                k: v
                for k, v in kwargs.items()
                if k != 'metadata'
            },
            **new_metadata_fields,
        }
        #
        super().put(**new_kwargs)


class VectorMixin(BaseTableMixin):

    @classmethod
    def _schema_da(cls):
        return super()._schema_da() + [
            'vector'
        ]

    def db_setup(self):
        super().db_setup()
        self.execute_cql('CREATE_VECTOR_SAI')

    def ann_search(self, vector, **kwargs):
        raise NotImplementedError

class ElasticKeyMixin():

    def __init__(self, *pargs, keys, **kwargs):
        self.keys = keys
        self.key_desc = "/".join(self.keys)
        super().__init__(*pargs, **kwargs)

    @staticmethod
    def _serialize_key_vals(key_vals: List[str]):
        return str(key_vals)

    def _split_row_args(self, arg_dict):
        # split in key/nonkey from a kwargs dict
        # and represent the former as one field
        key_args = {
            k: v
            for k, v in arg_dict.items()
            if k in self.keys
        }
        assert(set(key_args.keys()) == set(self.keys))
        key_vals = self._serialize_key_vals([
            key_args[key_col]
            for key_col in self.keys
        ])
        #
        other_kwargs = {
            k: v
            for k, v in arg_dict.items()
            if k not in self.keys
        }
        return key_vals, other_kwargs

    def delete(self, /, **kwargs):
        key_vals, other_kwargs = self._split_row_args(kwargs)
        super().delete(key_desc=self.key_desc, key_vals=key_vals, **other_kwargs)

    def get(self, /, **kwargs):
        key_vals, other_kwargs = self._split_row_args(kwargs)
        # TODO: unpack the key
        return super().get(key_desc=self.key_desc, key_vals=key_vals, **other_kwargs)

    def put(self, /, **kwargs):
        key_vals, other_kwargs = self._split_row_args(kwargs)
        super().put(key_desc=self.key_desc, key_vals=key_vals, **other_kwargs)

    @staticmethod
    def _schema_row_id():
        return [
            'key_desc',
            'key_vals',
        ]


# FINAL CLASSES TO USE:

class ClusteredTable(ClusteredMixin, BaseTable):
    pass


class ClusteredMetadataTable(MetadataMixin, ClusteredMixin, BaseTable):
    pass


class MetadataTable(MetadataMixin, BaseTable):
    pass


class VectorTable(VectorMixin, BaseTable):
    pass


class ClusteredVectorTable(ClusteredMixin, VectorMixin, BaseTable):
    pass


class ClusteredMetadataVectorTable(MetadataMixin, ClusteredMixin, VectorMixin, BaseTable):
    pass


class MetadataVectorTable(MetadataMixin, VectorMixin, BaseTable):
    pass


class ElasticTable(ElasticKeyMixin, BaseTable):
    pass


class ClusteredElasticTable(ClusteredMixin, ElasticKeyMixin, BaseTable):
    pass


class ClusteredElasticMetadataTable(MetadataMixin, ElasticKeyMixin, ClusteredMixin, BaseTable):
    pass


class ElasticMetadataTable(MetadataMixin, ElasticKeyMixin, BaseTable):
    pass


class ElasticVectorTable(VectorMixin, ElasticKeyMixin, BaseTable):
    pass


class ClusteredElasticVectorTable(ClusteredMixin, ElasticKeyMixin, VectorMixin, BaseTable):
    pass


class ClusteredElasticMetadataVectorTable(MetadataMixin, ElasticKeyMixin, ClusteredMixin, VectorMixin, BaseTable):
    pass


class ElasticMetadataVectorTable(MetadataMixin, ElasticKeyMixin, VectorMixin, BaseTable):
    pass


if __name__ == '__main__':
    print('='*80)
    t = BaseTable('s', 'k', 'tn')
    t.db_setup()
    t.delete(row_id='ROWID')
    t.get(row_id='ROWID')
    t.put(row_id='ROWID')
    t.put(row_id='ROWID', body_blob='BODYBLOB')
    t.clear()

    # ct = ClusteredTable('s', 'k', 'tn')
    # ct.db_setup()

    # cmt = ClusteredMetadataTable('s', 'k', 'tn')
    # cmt.db_setup()

    # mt = MetadataTable('s', 'k', 'tn')
    # mt.db_setup()

    print('='*80)
    bt = VectorTable('s', 'k', 'tn')
    bt.db_setup()
    bt.delete(row_id='ROWID')
    bt.get(row_id='ROWID')
    bt.put(row_id='ROWID', body_blob='BODYBLOB', vector='VECTOR')
    bt.clear()

    print('='*80)
    cvt = ClusteredVectorTable('s', 'k', 'tn')
    cvt.db_setup()
    cvt.delete(partition_id='PARTITIONID', row_id='ROWID')
    cvt.delete_partition(partition_id='PARTITIONID')
    cvt.get(partition_id='PARTITIONID', row_id='ROWID')
    cvt.get_partition(partition_id='PARTITIONID')
    cvt.put(partition_id='PARTITIONID', row_id='ROWID', body_blob='BODYBLOB', vector='VECTOR')
    cvt.put(partition_id='PARTITIONID', row_id='ROWID', body_blob='BODYBLOB')
    cvt.put(partition_id='PARTITIONID', row_id='ROWID', vector='VECTOR')
    cvt.clear()

    # cmvt = ClusteredMetadataVectorTable('s', 'k', 'tn')
    # cmvt.db_setup()

    # mvt = MetadataVectorTable('s', 'k', 'tn')
    # mvt.db_setup()

    print('='*80)
    et = ElasticTable('s', 'k', 'tn', keys=['a','b'])
    et.db_setup()
    et.delete(a='A', b='B')
    et.get(a='A', b='B')
    et.put(a='A', b='B', body_blob='BODYBLOB')
    et.clear()

    # cet = ClusteredElasticTable('s', 'k', 'tn')
    # cet.db_setup()

    # cemt = ClusteredElasticMetadataTable('s', 'k', 'tn')
    # cemt.db_setup()

    # emt = ElasticMetadataTable('s', 'k', 'tn')
    # emt.db_setup()

    # evt = ElasticVectorTable('s', 'k', 'tn')
    # evt.db_setup()

    # cevt = ClusteredElasticVectorTable('s', 'k', 'tn')
    # cevt.db_setup()

    print('='*80)
    cemvt = ClusteredElasticMetadataVectorTable('s', 'k', 'tn', keys=['a','b'])
    cemvt.db_setup()
    cemvt.delete(partition_id='PARTITIONID', a='A', b='B')
    cemvt.get(partition_id='PARTITIONID', a='A', b='B')
    cemvt.delete_partition(partition_id='PARTITION_ID')
    cemvt.put(partition_id='PARTITIONID', a='A', b='B', body_blob='BODYBLOB', vector='VECTOR')
    md1 = {'num1': 123, 'num2': 456, 'str1': 'STR1', 'tru1': True}
    md2 = {'tru1': True, 'tru2': True}
    cemvt.put(partition_id='PARTITIONID', a='A', b='B', body_blob='BODYBLOB', vector='VECTOR', metadata=md1)
    cemvt.put(partition_id='PARTITIONID', a='A', b='B', body_blob='BODYBLOB', vector='VECTOR', metadata=md2)
    cemvt.put(partition_id='PARTITIONID', a='A', b='B', metadata=md2)
    cemvt.get_partition(partition_id='PARTITIONID', n=10)
    cemvt.get_partition(partition_id='PARTITIONID')
    cemvt.clear()

    # emvt = ElasticMetadataVectorTable('s', 'k', 'tn')
    # emvt.db_setup()
