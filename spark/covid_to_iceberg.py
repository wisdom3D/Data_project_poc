from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (
    SparkSession.builder
    .appName("Covid-Raw-to-Iceberg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
    .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog")
    .config("spark.sql.catalog.demo.jdbc.user", "metadata")
    .config("spark.sql.catalog.demo.jdbc.password", "metadata")
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
    .csv("s3a://raw/covid/")
)

# Nettoyage minimal (exemple)
df_clean = (
    df
    .dropDuplicates()
    .filter(col("location").isNotNull())
    .withColumn("last_updated_date", to_date(col("last_updated_date")))
    .withColumn("total_cases", col("total_cases").cast("float"))
    .withColumn("new_cases", col("new_cases").cast("float"))
    .withColumn("new_cases_smoothed", col("new_cases_smoothed").cast("float"))
    .withColumn("total_deaths", col("total_deaths").cast("float"))
    .withColumn("total_cases_per_million", col("total_cases_per_million").cast("float"))
    .withColumn("new_cases_per_million", col("new_cases_per_million").cast("float"))
    .withColumn("new_cases_smoothed", col("new_cases_smoothed").cast("float"))
    .withColumn("new_cases_smoothed_per_million", col("new_cases_smoothed_per_million").cast("float"))
    .withColumn("total_deaths_per_million", col("total_deaths_per_million").cast("float"))
    .withColumn("new_deaths_per_million", col("new_deaths_per_million").cast("float"))
    .withColumn("new_deaths_smoothed_per_million", col("new_deaths_smoothed_per_million").cast("float"))
)

# Écriture Iceberg
df_clean.writeTo("demo.public.covid").using("iceberg").createOrReplace()

spark.stop()
