from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = (
    SparkSession.builder
    .appName("Malaria-Clean-Pipeline")
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

# 1. Lecture RAW
df = spark.read.option("header", "true").csv("s3a://raw/malaria/")

# 2. Définition des colonnes complexes à transformer (Ancien Nom -> Nouveau Nom)
mapping_cols = {
    "rural population (% of total population)": "pct_pop_rurale",
    "rural population growth (annual %)": "croissance_pop_rurale_annuelle",
    "urban population (% of total population)": "pct_pop_urbaine",
    "urban population growth (annual %)": "croissance_pop_urbaine_annuelle",
    "people using at least basic drinking water services (% of population)": "pct_eau_potable_total",
    "people using at least basic drinking water services, rural (% of rural population)": "pct_eau_potable_rurale",
    "people using at least basic drinking water services, urban (% of urban population)": "pct_eau_potable_urbaine",
    "people using at least basic sanitation services (% of population)": "pct_sanitaire_total",
    "people using at least basic sanitation services, rural (% of rural population)": "pct_sanitaire_rurale",
    "people using at least basic sanitation services, urban  (% of urban population)": "pct_sanitaire_urbaine",
    "incidence of malaria (per 1,000 population at risk)": "incidence_malaria_pour_1000",
    "malaria cases reported": "cas_malaria_rapportes",
    "latitude": "latitude",
    "longitude": "longitude"
}

# 3. Application des transformations
df_clean = df.dropDuplicates()

for old_name, new_name in mapping_cols.items():
    df_clean = df_clean.withColumn(new_name, col(f"`{old_name}`").cast("float"))

df_final = df_clean.select(
    col("country name").alias("pays"),
    col("year").cast("int").alias("annee"),
    *mapping_cols.values()
)

# Écriture Iceberg
df_final.writeTo("demo.public.malaria").using("iceberg").createOrReplace()

spark.stop()
