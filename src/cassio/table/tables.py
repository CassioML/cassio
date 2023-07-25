from cassio.table.base_table import BaseTable
from cassio.table.mixins import (
    ClusteredMixin,
    MetadataMixin,
    VectorMixin,
    ElasticKeyMixin,
    #
    TypeNormalizerMixin,
)


class PlainCassandraTable(TypeNormalizerMixin, BaseTable):
    pass


class ClusteredCassandraTable(TypeNormalizerMixin, ClusteredMixin, BaseTable):
    clustered = True
    pass


class ClusteredMetadataCassandraTable(
    TypeNormalizerMixin, MetadataMixin, ClusteredMixin, BaseTable
):
    clustered = True
    pass


class MetadataCassandraTable(TypeNormalizerMixin, MetadataMixin, BaseTable):
    pass


class VectorCassandraTable(TypeNormalizerMixin, VectorMixin, BaseTable):
    pass


class ClusteredVectorCassandraTable(
    TypeNormalizerMixin, ClusteredMixin, VectorMixin, BaseTable
):
    clustered = True
    pass


class ClusteredMetadataVectorCassandraTable(
    TypeNormalizerMixin, MetadataMixin, ClusteredMixin, VectorMixin, BaseTable
):
    clustered = True
    pass


class MetadataVectorCassandraTable(
    TypeNormalizerMixin, MetadataMixin, VectorMixin, BaseTable
):
    pass


class ElasticCassandraTable(TypeNormalizerMixin, ElasticKeyMixin, BaseTable):
    elastic = True
    pass


class ClusteredElasticCassandraTable(
    TypeNormalizerMixin, ClusteredMixin, ElasticKeyMixin, BaseTable
):
    clustered = True
    elastic = True
    pass


class ClusteredElasticMetadataCassandraTable(
    TypeNormalizerMixin, MetadataMixin, ElasticKeyMixin, ClusteredMixin, BaseTable
):
    clustered = True
    elastic = True
    pass


class ElasticMetadataCassandraTable(
    TypeNormalizerMixin, MetadataMixin, ElasticKeyMixin, BaseTable
):
    elastic = True
    pass


class ElasticVectorCassandraTable(
    TypeNormalizerMixin, VectorMixin, ElasticKeyMixin, BaseTable
):
    elastic = True
    pass


class ClusteredElasticVectorCassandraTable(
    TypeNormalizerMixin, ClusteredMixin, ElasticKeyMixin, VectorMixin, BaseTable
):
    clustered = True
    elastic = True
    pass


class ClusteredElasticMetadataVectorCassandraTable(
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


class ElasticMetadataVectorCassandraTable(
    MetadataMixin, ElasticKeyMixin, VectorMixin, BaseTable
):
    elastic = True
    pass


if __name__ == "__main__":
    #
    from cassio.table.cql import MockDBSession

    session = MockDBSession(verbose=True)
    #
    print("=" * 80, "PlainCassandraTable")
    # t = PlainTable(session, "k", "tn", row_id_type="UUID")
    t = PlainCassandraTable(
        session, "k", "tn", primary_key_type="UUID", skip_provisioning=True
    )
    t.delete(row_id="ROWID")
    t.get(row_id="ROWID")
    t.put(row_id="ROWID")
    t.put(row_id="ROWID", body_blob="BODYBLOB")
    t.clear()

    # ct = ClusteredCassandraTable(session, "k", "tn")

    # cmt = ClusteredMetadataCassandraTable(session, "k", "tn")

    # mt = MetadataCassandraTable(session, "k", "tn")

    print("=" * 80, "VectorCassandraTable")
    # bt = VectorCassandraTable(session, "k", "tn", row_id_type="UUID")
    bt = VectorCassandraTable(
        session, "k", "tn", vector_dimension=765, primary_key_type="UUID"
    )
    bt.delete(row_id="ROWID")
    bt.get(row_id="ROWID")
    bt.put(row_id="ROWID", body_blob="BODYBLOB", vector="VECTOR")
    bt.clear()

    print("=" * 80, "ClusteredVectorCassandraTable")
    # cvt = ClusteredVectorCassandraTable(session, "k", "tn", row_id_type="UUID", partition_id_type="PUUID")
    # cvt = ClusteredVectorCassandraTable(
    #     session, "k", "tn", vector_dimension=765, primary_key_type=["PUUID", "UUID"]
    # )
    cvt = ClusteredVectorCassandraTable(
        session,
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

    # cmvt = ClusteredMetadataVectorCassandraTable(session, "k", "tn")

    # mvt = MetadataVectorCassandraTable(session, "k", "tn")

    print("=" * 80, "ElasticCassandraTable")
    # et = ElasticCassandraTable(session, "k", "tn", keys=["a", "b"])
    et = ElasticCassandraTable(
        session, "k", "tn", keys=["a", "b"], primary_key_type=["AT", "BT"]
    )
    et.delete(a="A", b="B")
    et.get(a="A", b="B")
    et.put(a="A", b="B", body_blob="BODYBLOB", ttl_seconds=444)
    et.clear()

    # cet = ClusteredElasticCassandraTable(session, "k", "tn")

    # cemt = ClusteredElasticMetadataCassandraTable(session, "k", "tn")

    # emt = ElasticMetadataCassandraTable(session, "k", "tn")

    # evt = ElasticVectorCassandraTable(session, "k", "tn")

    # cevt = ClusteredElasticVectorCassandraTable(session, "k", "tn")

    print("=" * 80, "ClusteredElasticMetadataVectorCassandraTable")
    # cemvt = ClusteredElasticMetadataVectorCassandraTable(session, "k", "tn", keys=["a", "b"], partition_id_type="PUUID")
    cemvt = ClusteredElasticMetadataVectorCassandraTable(
        session,
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

    # emvt = ElasticMetadataVectorCassandraTable(session, "k", "tn")
