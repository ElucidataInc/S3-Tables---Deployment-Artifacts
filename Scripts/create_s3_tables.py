from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, LongType, DoubleType, FloatType, ListType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import TruncateTransform, BucketTransform, IdentityTransform
from pyiceberg.table.sorting import SortOrder, SortField, NullOrder
from pyiceberg.transforms import IdentityTransform


catalog = load_catalog(
    "s3tables",
    **{
        "type": "rest",
        "uri": "https://s3tables.ap-southeast-1.amazonaws.com/iceberg",
        "warehouse": "", # ARN_S3_TABLES_BUCKET,
        "rest.sigv4-enabled": "true",
        "rest.signing-region": "ap-southeast-1",
        "rest.signing-name": "s3tables",
    }
)

def create_variants_table():
    schema = Schema(
        NestedField(1,  "sample_name",               StringType(),  required=False),
        NestedField(2,  "contig",                     StringType(),  required=False),
        NestedField(3,  "pos_start",                  IntegerType(), required=False),
        NestedField(4,  "pos_end",                    IntegerType(), required=False),
        NestedField(5,  "ref",                        StringType(),  required=False),
        NestedField(6,  "alt",                        StringType(),  required=False),
        NestedField(7,  "qual",                       DoubleType(),  required=False),
        NestedField(8,  "filters",                    StringType(),  required=False),
        NestedField(9,  "genotype",                   StringType(),  required=False),
        NestedField(10, "read_depth",                 IntegerType(), required=False),
        NestedField(11, "allele_depth",               ListType(12, IntegerType()), required=False),
        NestedField(13, "genotype_quality",           IntegerType(), required=False),
        NestedField(14, "variant_allele_frequency",   FloatType(),   required=False),
        NestedField(15, "somalone_somatic_score",     FloatType(),   required=False),
        NestedField(16, "vep_gene",                   StringType(),  required=False),
        NestedField(17, "vep_symbol",                 StringType(),  required=False),
        NestedField(18, "vep_consequence",            StringType(),  required=False),
        NestedField(19, "vep_impact",                 StringType(),  required=False),
        NestedField(20, "gnomad_af",                  FloatType(),   required=False),
        NestedField(21, "gnomad_af_popmax",           FloatType(),   required=False),
        NestedField(22, "pathogenecity",              StringType(),  required=False),
        NestedField(23, "somalone_predicted_origin",  StringType(),  required=False),
        NestedField(22, "data_source",              StringType(),  required=False),
        NestedField(22, "data_type",              StringType(),  required=False),
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=2,  field_id=1000, transform=IdentityTransform(),        name="contig_part"),
        PartitionField(source_id=3,  field_id=1001, transform=TruncateTransform(5000000), name="pos_trunc"),
        PartitionField(source_id=1,  field_id=1002, transform=BucketTransform(16),        name="sample_bucket"),
    )

    sort_order = SortOrder(
        SortField(source_id=3,  transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # pos_start
        SortField(source_id=5,  transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # ref
        SortField(source_id=6,  transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # alt
    )

    catalog.create_table(
        identifier="variants.variants",
        schema=schema,
        partition_spec=partition_spec,
        sort_order=sort_order,
        properties={
            "write.target-file-size-bytes": "268435456",
            "write.parquet.bloom-filter-enabled.column.ref": "true",
            "write.parquet.bloom-filter-enabled.column.alt": "true",
            "write.parquet.bloom-filter-enabled.column.vep_gene": "true",
            "write.parquet.bloom-filter-enabled.column.vep_symbol": "true",
        },
    )
    print("Created table: variants.variants")


def create_annotations_table(tbl_name="variants.annotations"):
    schema = Schema(
        NestedField(1, "chrom",            StringType(),  required=False),
        NestedField(2, "pos",              IntegerType(), required=False),
        NestedField(3, "ref",              StringType(),  required=False),
        NestedField(4, "alt",              StringType(),  required=False),
        NestedField(5, "gene",             StringType(),  required=False),
        NestedField(6, "symbol",           StringType(),  required=False),
        NestedField(7, "consequence",      StringType(),  required=False),
        NestedField(8, "impact",           StringType(),  required=False),
        NestedField(9, "gnomad_af",        FloatType(),   required=False),
        NestedField(10, "gnomad_af_popmax", FloatType(),  required=False),
        NestedField(11, "pathogenecity",   StringType(),  required=False),
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(),        name="chrom_part"),
        PartitionField(source_id=2, field_id=1001, transform=TruncateTransform(5000000), name="pos_trunc"),
    )

    sort_order = SortOrder(
        SortField(source_id=2, transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # pos
        SortField(source_id=3, transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # ref
        SortField(source_id=4, transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # alt
    )

    catalog.create_table(
        identifier=tbl_name,
        schema=schema,
        partition_spec=partition_spec,
        sort_order=sort_order,
        properties={
            "write.target-file-size-bytes": "134217728",
            "write.parquet.bloom-filter-enabled.column.ref": "true",
            "write.parquet.bloom-filter-enabled.column.alt": "true",
            "write.parquet.bloom-filter-enabled.column.vep_gene": "true",
            "write.parquet.bloom-filter-enabled.column.vep_symbol": "true",
        },
    )
    print(f"Created table: {tbl_name}")


def create_variants_by_sample_table():
    schema = Schema(
        NestedField(1,  "sample_name",               StringType(),  required=False),
        NestedField(2,  "contig",                     StringType(),  required=False),
        NestedField(3,  "pos_start",                  IntegerType(), required=False),
        NestedField(4,  "pos_end",                    IntegerType(), required=False),
        NestedField(5,  "ref",                        StringType(),  required=False),
        NestedField(6,  "alt",                        StringType(),  required=False),
        NestedField(7,  "qual",                       DoubleType(),  required=False),
        NestedField(8,  "filters",                    StringType(),  required=False),
        NestedField(9,  "genotype",                   StringType(),  required=False),
        NestedField(10, "read_depth",                 IntegerType(), required=False),
        NestedField(11, "allele_depth",               ListType(12, IntegerType()), required=False),
        NestedField(13, "genotype_quality",           IntegerType(), required=False),
        NestedField(14, "variant_allele_frequency",   FloatType(),   required=False),
        NestedField(15, "somalone_somatic_score",     FloatType(),   required=False),
        NestedField(16, "vep_gene",                   StringType(),  required=False),
        NestedField(17, "vep_symbol",                 StringType(),  required=False),
        NestedField(18, "vep_consequence",            StringType(),  required=False),
        NestedField(19, "vep_impact",                 StringType(),  required=False),
        NestedField(20, "gnomad_af",                  FloatType(),   required=False),
        NestedField(21, "gnomad_af_popmax",           FloatType(),   required=False),
        NestedField(22, "pathogenecity",              StringType(),  required=False),
        NestedField(23, "somalone_predicted_origin",  StringType(),  required=False),
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=1,  field_id=1000, transform=BucketTransform(4096),       name="sample_bucket"),
        PartitionField(source_id=2,  field_id=1001, transform=IdentityTransform(),         name="contig_part"),
        PartitionField(source_id=3,  field_id=1002, transform=TruncateTransform(50000000), name="pos_trunc"),
    )

    sort_order = SortOrder(
        SortField(source_id=3, transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # pos_start
        SortField(source_id=5, transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # ref
        SortField(source_id=6, transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # alt
    )

    catalog.create_table(
        identifier="variants.variants_by_sample",
        schema=schema,
        partition_spec=partition_spec,
        sort_order=sort_order,
        properties={
            "write.target-file-size-bytes": "134217728",
            "write.parquet.bloom-filter-enabled.column.ref": "true",
            "write.parquet.bloom-filter-enabled.column.alt": "true",
        },
    )
    print("Created table: variants.variants_by_sample")


def create_tpm_table(tbl_name="variants.tpm"):
    schema = Schema(
        NestedField(1, "sample_id",            StringType(),  required=False),
        NestedField(2, "gene_symbol",          StringType(),  required=False),
        NestedField(3, "expression",           FloatType(),   required=False),
        NestedField(4, "tissue",               StringType(),  required=False),
        NestedField(5, "gene_id",              StringType(),  required=False),
        NestedField(6, "dataset",              StringType(),  required=False),
        NestedField(7, "dataset_version",      StringType(),  required=False),
        NestedField(8, "genome_build",         StringType(),  required=False),
        NestedField(9, "annotation",           StringType(),  required=False),
        NestedField(10, "quant_method",        StringType(),  required=False),
        NestedField(11, "unit",                StringType(),  required=False),
    )

    # 350k samples * ~28k genes = 10B rows.
    # 128 sample buckets -> ~2,700 samples per bucket -> ~78M rows/bucket
    # 64 gene buckets    -> ~1,500 genes  per bucket
    # 128 * 64 = 8,192 partitions -> ~1.2M rows/partition (~50-100 MB compressed).
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=BucketTransform(128), name="sample_bucket"),
        PartitionField(source_id=2, field_id=1001, transform=BucketTransform(64),  name="gene_bucket"),
    )

    # Sort inside each partition by the keys that get filtered/joined on.
    # This gives tight parquet row-group min/max stats for file/row-group pruning.
    sort_order = SortOrder(
        SortField(source_id=1, transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # sample_id
        SortField(source_id=2, transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # gene_symbol
        SortField(source_id=4, transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # tissue
    )

    catalog.create_table(
        identifier=tbl_name,
        schema=schema,
        partition_spec=partition_spec,
        sort_order=sort_order,
        properties={
            "write.target-file-size-bytes": "134217728",
            "write.parquet.bloom-filter-enabled.column.sample_id":   "true",
            "write.parquet.bloom-filter-enabled.column.gene_symbol": "true",
            "write.parquet.bloom-filter-enabled.column.gene_id":     "true",
            "write.distribution-mode": "hash",
            "write.parquet.compression-codec": "zstd",
        },
    )
    print(f"Created table: {tbl_name}")
    

def create_cnv_table():
    schema = Schema(
        NestedField(1,  "importjobid",        StringType(),  required=False),
        NestedField(2,  "sampleid",           StringType(),  required=False),
        NestedField(3,  "technology",         StringType(),  required=False),
        NestedField(4,  "pipeline",           StringType(),  required=False),
        NestedField(5,  "pipeline_version",   StringType(),  required=False),
        NestedField(6,  "out_path",           StringType(),  required=False),
        NestedField(7,  "record_type",        StringType(),  required=False),
        NestedField(8,  "contigname",         StringType(),  required=False),
        NestedField(9,  "start",              IntegerType(),    required=False),
        NestedField(10, "end",                IntegerType(),    required=False),
        NestedField(11, "gene",               StringType(),  required=False),
        NestedField(12, "log2",               FloatType(),   required=False),
        NestedField(13, "baf",                FloatType(),  required=False),
        NestedField(14, "cn",                 FloatType(),  required=False),
        NestedField(15, "copy",               IntegerType(),    required=False),
        NestedField(16, "call",               StringType(),  required=False),
    )

    partition_spec = PartitionSpec(
        PartitionField(source_id=8,  field_id=1000, transform=IdentityTransform(),        name="contig_part"),
        PartitionField(source_id=9,  field_id=1001, transform=TruncateTransform(5000000), name="pos_trunc"),
        PartitionField(source_id=2,  field_id=1002, transform=BucketTransform(16),        name="sample_bucket"),
    )

    sort_order = SortOrder(
        SortField(source_id=9,  transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # pos_start
        SortField(source_id=10,  transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # pos_end
        SortField(source_id=11, transform=IdentityTransform(), null_order=NullOrder.NULLS_LAST),  # gene
    )

    catalog.create_table(
        identifier="variants.cnv",
        schema=schema,
        partition_spec=partition_spec,
        sort_order=sort_order,
        properties={
            "write.target-file-size-bytes": "268435456",
            "write.parquet.bloom-filter-enabled.column.gene": "true",
            "write.parquet.bloom-filter-enabled.column.sampleid": "true",
            "write.distribution-mode": "hash",
            "write.parquet.compression-codec": "zstd",
        },
    )
    print("Created table: variants.cnv")


def drop_table(identifier):
    catalog.drop_table(identifier, purge_requested=True)
    print(f"Dropped table: {identifier}")


def verify_sort_order(identifier):
    print(catalog.load_table(identifier).sort_order())


# drop_table("variants.variants")
# drop_table("variants.annotations")
# drop_table("variants.variants_by_sample")
# drop_table("variants.annotations_dedup")

# create_variants_table()
# create_annotations_table()
# create_annotations_table(tbl_name="variants.annotations_dedup")
# create_tpm_table()
# create_cnv_table()
# create_variants_by_sample_table()

verify_sort_order("variants.variants")
verify_sort_order("variants.annotations")
