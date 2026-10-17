# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — ingesta incremental
# MAGIC
# MAGIC - **RIPS JSON** → `bronze_rips` con Auto Loader (incremental: solo procesa archivos nuevos).
# MAGIC - **Tablas de referencia** (nota técnica, bandas PGP, prestadores, catálogos) → `bronze_ref_*`.
# MAGIC - **Anomalías sembradas** → `eval_anomalias_sembradas`. Se guardan aparte: solo sirven para
# MAGIC   medir el detector al final, nunca para construirlo.
# MAGIC
# MAGIC Bronze guarda el dato tal como llega, más metadatos de trazabilidad (archivo de origen,
# MAGIC periodo y momento de carga). Toda limpieza va en Silver.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "health_claims", "Esquema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. RIPS JSON con Auto Loader

# COMMAND ----------

rips_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")                       # cada archivo es un JSON indentado
    .option("pathGlobFilter", "*.json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/bronze_rips/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(f"{LANDING}/raw/rips/")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_periodo", F.regexp_extract(F.col("_metadata.file_path"), r"/rips/(\d{6})/", 1))
    .withColumn("_ingested_at", F.current_timestamp())
)

consulta = (
    rips_stream.writeStream
    .option("checkpointLocation", f"{CHECKPOINTS}/bronze_rips/checkpoint")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)                        # procesa lo pendiente y se detiene
    .toTable(f"{CATALOG}.{SCHEMA}.bronze_rips")
)
consulta.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tablas de referencia y etiquetas de evaluación

# COMMAND ----------

def cargar_csv(ruta: str, tabla: str) -> None:
    (
        spark.read.option("header", "true").option("inferSchema", "true").csv(ruta)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{tabla}")
    )
    print(f"{tabla}: {spark.table(f'{CATALOG}.{SCHEMA}.{tabla}').count():,} filas")

referencias = {
    "ref/nota_tecnica.csv": "bronze_ref_nota_tecnica",
    "ref/pgp_bandas.csv": "bronze_ref_pgp_bandas",
    "ref/prestadores.csv": "bronze_ref_prestadores",
    "ref/catalogo_cups.csv": "bronze_ref_cups",
    "ref/catalogo_cie10.csv": "bronze_ref_cie10",
    "ref/catalogo_medicamentos.csv": "bronze_ref_medicamentos",
    "labels/anomalias_sembradas.csv": "eval_anomalias_sembradas",
    "labels/anomalias_prestador_mes.csv": "eval_anomalias_prestador_mes",
}
for archivo, tabla in referencias.items():
    cargar_csv(f"{LANDING}/{archivo}", tabla)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación

# COMMAND ----------

display(spark.sql(f"""
    SELECT _periodo,
           COUNT(*)                         AS facturas,
           COUNT(DISTINCT numDocumentoIdObligado) AS prestadores,
           SUM(size(usuarios))              AS usuarios_en_facturas
    FROM {CATALOG}.{SCHEMA}.bronze_rips
    GROUP BY _periodo
    ORDER BY _periodo
"""))

# COMMAND ----------

spark.table(f"{CATALOG}.{SCHEMA}.bronze_rips").printSchema()
