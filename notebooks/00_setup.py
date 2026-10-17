# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — esquema, volúmenes y datos sintéticos
# MAGIC
# MAGIC Crea el esquema y los volúmenes en Unity Catalog y genera los RIPS sintéticos
# MAGIC directamente en el volumen `landing`. Se ejecuta una sola vez (o cuando quieras
# MAGIC regenerar los datos con otros parámetros).
# MAGIC
# MAGIC **Requisito:** este notebook debe estar dentro del Git folder del repo, para que
# MAGIC encuentre `../generar_rips_sinteticos.py`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "health_claims", "Esquema")
dbutils.widgets.text("meses", "12", "Meses a generar")
dbutils.widgets.text("escala", "3", "Escala de volumen")
dbutils.widgets.text("tasa_anomalia", "0.03", "Tasa de anomalías")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Esquema y volúmenes

# COMMAND ----------

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}
    COMMENT 'Auditoría de facturación en salud con datos 100 % sintéticos'
""")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing COMMENT 'Archivos fuente (RIPS JSON y CSV de referencia)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints COMMENT 'Checkpoints y esquemas de Auto Loader'")

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generar los datos sintéticos en el volumen

# COMMAND ----------

import subprocess
import sys

cmd = [
    sys.executable, "../generar_rips_sinteticos.py",
    "--salida", LANDING,
    "--meses", dbutils.widgets.get("meses"),
    "--escala", dbutils.widgets.get("escala"),
    "--tasa-anomalia", dbutils.widgets.get("tasa_anomalia"),
]
resultado = subprocess.run(cmd, capture_output=True, text=True)
print(resultado.stdout)
print(resultado.stderr)
resultado.check_returncode()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificación rápida

# COMMAND ----------

for carpeta in ["raw/rips", "ref", "labels"]:
    archivos = dbutils.fs.ls(f"{LANDING}/{carpeta}")
    print(f"{carpeta}: {len(archivos)} elementos")
