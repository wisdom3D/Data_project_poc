from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Sante-Raw-to-Iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.type", "hadoop")
    .config("spark.sql.catalog.demo.warehouse", "s3a://clean/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# Lecture RAW
df = (
    spark.read
    .option("header", "true")
    .csv("s3a://raw/sante/")
)

# Nettoyage minimal (exemple)
df_clean = (
    df
    .dropDuplicates()
    .filter(col("location").isNotNull())
)

# Écriture Iceberg
df_clean.writeTo("demo.sante_covid").using("iceberg").createOrReplace()

spark.stop()
