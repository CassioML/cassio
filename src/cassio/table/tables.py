from cassio.table.base_table import BaseTable
from cassio.table.mixins import (
    ClusteredMixin,
    MetadataMixin,
    VectorMixin,
    ElasticKeyMixin,
    #
    TypeNormalizerMixin,
)


class PlainTable(TypeNormalizerMixin, BaseTable):
    pass


class ClusteredTable(TypeNormalizerMixin, ClusteredMixin, BaseTable):
    clustered = True
    pass


class ClusteredMetadataTable(
    TypeNormalizerMixin, MetadataMixin, ClusteredMixin, BaseTable
):
    clustered = True
    pass


class MetadataTable(TypeNormalizerMixin, MetadataMixin, BaseTable):
    pass


class VectorTable(TypeNormalizerMixin, VectorMixin, BaseTable):
    pass


class ClusteredVectorTable(TypeNormalizerMixin, ClusteredMixin, VectorMixin, BaseTable):
    clustered = True
    pass


class ClusteredMetadataVectorTable(
    TypeNormalizerMixin, MetadataMixin, ClusteredMixin, VectorMixin, BaseTable
):
    clustered = True
    pass


class MetadataVectorTable(TypeNormalizerMixin, MetadataMixin, VectorMixin, BaseTable):
    pass


class ElasticTable(TypeNormalizerMixin, ElasticKeyMixin, BaseTable):
    elastic = True
    pass


class ClusteredElasticTable(
    TypeNormalizerMixin, ClusteredMixin, ElasticKeyMixin, BaseTable
):
    clustered = True
    elastic = True
    pass


class ClusteredElasticMetadataTable(
    TypeNormalizerMixin, MetadataMixin, ElasticKeyMixin, ClusteredMixin, BaseTable
):
    clustered = True
    elastic = True
    pass


class ElasticMetadataTable(
    TypeNormalizerMixin, MetadataMixin, ElasticKeyMixin, BaseTable
):
    elastic = True
    pass


class ElasticVectorTable(TypeNormalizerMixin, VectorMixin, ElasticKeyMixin, BaseTable):
    elastic = True
    pass


class ClusteredElasticVectorTable(
    TypeNormalizerMixin, ClusteredMixin, ElasticKeyMixin, VectorMixin, BaseTable
):
    clustered = True
    elastic = True
    pass


class ClusteredElasticMetadataVectorTable(
    TypeNormalizerMixin,
    MetadataMixin,
    ElasticKeyMixin,
    ClusteredMixin,
    VectorMixin,
    BaseTable,
):
    clustered = True
    elastic = True
    pass


class ElasticMetadataVectorTable(
    MetadataMixin, ElasticKeyMixin, VectorMixin, BaseTable
):
    elastic = True
    pass


if __name__ == "__main__":
    print("=" * 80)
    # t = PlainTable("s", "k", "tn", row_id_type="UUID")
    t = PlainTable("s", "k", "tn", primary_key_type="UUID", skip_provisioning=True)
    t.delete(row_id="ROWID")
    t.get(row_id="ROWID")
    t.put(row_id="ROWID")
    t.put(row_id="ROWID", body_blob="BODYBLOB")
    t.clear()

    # ct = ClusteredTable('s', 'k', 'tn')

    # cmt = ClusteredMetadataTable('s', 'k', 'tn')

    # mt = MetadataTable('s', 'k', 'tn')

    print("=" * 80)
    # bt = VectorTable("s", "k", "tn", row_id_type="UUID")
    bt = VectorTable("s", "k", "tn", vector_dimension=765, primary_key_type="UUID")
    bt.delete(row_id="ROWID")
    bt.get(row_id="ROWID")
    bt.put(row_id="ROWID", body_blob="BODYBLOB", vector="VECTOR")
    bt.clear()

    print("=" * 80)
    # cvt = ClusteredVectorTable("s", "k", "tn", row_id_type="UUID", partition_id_type="PUUID")
    # cvt = ClusteredVectorTable(
    #     "s", "k", "tn", vector_dimension=765, primary_key_type=["PUUID", "UUID"]
    # )
    cvt = ClusteredVectorTable(
        "s",
        "k",
        "tn",
        vector_dimension=765,
        primary_key_type=["PUUID", "UUID"],
        partition_id="PRE-PART-ID",
    )
    cvt.delete(row_id="ROWID")
    cvt.delete_partition(partition_id="PARTITIONID")
    cvt.get(partition_id="PARTITIONID", row_id="ROWID")
    cvt.get_partition(partition_id="PARTITIONID")
    cvt.put(
        partition_id="PARTITIONID",
        row_id="ROWID",
        body_blob="BODYBLOB",
        vector="VECTOR",
    )
    cvt.put(partition_id="PARTITIONID", row_id="ROWID", body_blob="BODYBLOB")
    cvt.put(partition_id="PARTITIONID", row_id="ROWID", vector="VECTOR")
    cvt.clear()

    # cmvt = ClusteredMetadataVectorTable('s', 'k', 'tn')

    # mvt = MetadataVectorTable('s', 'k', 'tn')

    print("=" * 80)
    # et = ElasticTable("s", "k", "tn", keys=["a", "b"])
    et = ElasticTable("s", "k", "tn", keys=["a", "b"], primary_key_type=["AT", "BT"])
    et.delete(a="A", b="B")
    et.get(a="A", b="B")
    et.put(a="A", b="B", body_blob="BODYBLOB", ttl_seconds=444)
    et.clear()

    # cet = ClusteredElasticTable('s', 'k', 'tn')

    # cemt = ClusteredElasticMetadataTable('s', 'k', 'tn')

    # emt = ElasticMetadataTable('s', 'k', 'tn')

    # evt = ElasticVectorTable('s', 'k', 'tn')

    # cevt = ClusteredElasticVectorTable('s', 'k', 'tn')

    print("=" * 80)
    # cemvt = ClusteredElasticMetadataVectorTable("s", "k", "tn", keys=["a", "b"], partition_id_type="PUUID")
    cemvt = ClusteredElasticMetadataVectorTable(
        "s",
        "k",
        "tn",
        keys=["a", "b"],
        vector_dimension=765,
        primary_key_type=["PUUID", "AT", "BT"],
        ttl_seconds=123,
    )
    cemvt.delete(partition_id="PARTITIONID", a="A", b="B")
    cemvt.get(partition_id="PARTITIONID", a="A", b="B")
    cemvt.delete_partition(partition_id="PARTITION_ID")
    cemvt.put(
        partition_id="PARTITIONID", a="A", b="B", body_blob="BODYBLOB", vector="VECTOR"
    )
    md1 = {"num1": 123, "num2": 456, "str1": "STR1", "tru1": True}
    md2 = {"tru1": True, "tru2": True}
    cemvt.put(
        partition_id="PARTITIONID",
        a="A",
        b="B",
        body_blob="BODYBLOB",
        vector="VECTOR",
        metadata=md1,
    )
    cemvt.put(
        partition_id="PARTITIONID",
        a="A",
        b="B",
        body_blob="BODYBLOB",
        vector="VECTOR",
        metadata=md2,
    )
    cemvt.put(partition_id="PARTITIONID", a="A", b="B", metadata=md2)
    cemvt.get_partition(partition_id="PARTITIONID", n=10)
    cemvt.get_partition(partition_id="PARTITIONID")
    cemvt.clear()

    # emvt = ElasticMetadataVectorTable('s', 'k', 'tn')
