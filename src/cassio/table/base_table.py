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
    def _schema_colset(cls):
        full_list = cls._schema_da() + cls._schema_cc() + cls._schema_pk()
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


    def db_setup(self):
        self.execute_cql(f"MKTABLE: {self._desc_table()}")

    def execute_cql(self, query, args=tuple()):
        cls_name = self.__class__.__name__
        ftqual = f"{self.keyspace}.{self.table_name}"
        print(f"CQL({cls_name:<32}/{ftqual}) '{query}' ({str(args)})")


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
        self.execute_cql('METADATA_SAIS')

class VectorMixin(BaseTableMixin):

    @classmethod
    def _schema_da(cls):
        return super()._schema_da() + [
            'vector'
        ]

    def db_setup(self):
        super().db_setup()
        self.execute_cql('VECTOR_SAI')

class ElasticKeyMixin():

    @staticmethod
    def _schema_row_id():
        return [
            'key_desc',
            'key_vals',
        ]

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
    t = BaseTable('s', 'k', 'tn')
    t.db_setup()
    t.delete(row_id='ROWID')

    # ct = ClusteredTable('s', 'k', 'tn')
    # ct.db_setup()

    # cmt = ClusteredMetadataTable('s', 'k', 'tn')
    # cmt.db_setup()

    # mt = MetadataTable('s', 'k', 'tn')
    # mt.db_setup()

    bt = VectorTable('s', 'k', 'tn')
    bt.db_setup()
    bt.delete(row_id='ROWID')

    # cvt = ClusteredVectorTable('s', 'k', 'tn')
    # cvt.db_setup()

    # cmvt = ClusteredMetadataVectorTable('s', 'k', 'tn')
    # cmvt.db_setup()

    # mvt = MetadataVectorTable('s', 'k', 'tn')
    # mvt.db_setup()

    et = ElasticTable('s', 'k', 'tn')
    et.db_setup()
    et.delete(key_desc='KEYDESC', key_vals='KEYVALS')

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

    cemvt = ClusteredElasticMetadataVectorTable('s', 'k', 'tn')
    cemvt.db_setup()
    cemvt.delete(partition_id='PARTITIONID', key_desc='KEYDESC', key_vals='KEYVALS')

    # emvt = ElasticMetadataVectorTable('s', 'k', 'tn')
    # emvt.db_setup()
