import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
# spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkIcebergSQL") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog","s3tables") \
    .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3tables.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.s3tables.glue.id", "**account-id**:s3tablescatalog/**table-bucket-name**") \
    .config("spark.sql.catalog.s3tables.warehouse", "s3://**table-bucket-name**/warehouse/") \
    .getOrCreate()

spark.sql("""
        INSERT INTO variants.annotations_dedup
        SELECT chrom, pos, ref, alt, gene, symbol, consequence, impact, gnomad_af, gnomad_af_popmax, pathogenecity
        FROM (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY chrom, pos, ref, alt ORDER BY gene) AS rn
          FROM
            variants.annotations
        )
        WHERE rn = 1
"""
)

job.commit()
