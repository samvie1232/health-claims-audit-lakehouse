# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — servicios aplanados, calidad y valorización contra la NT
# MAGIC
# MAGIC Convierte el RIPS anidado (factura → usuarios → servicios) en **una fila por servicio
# MAGIC prestado** y le aplica las reglas de auditoría:
# MAGIC
# MAGIC | Regla | Qué detecta |
# MAGIC | --- | --- |
# MAGIC | `DUPLICADO` | Mismo prestador, usuario, tecnología, fecha y valor facturado más de una vez |
# MAGIC | `NO_CONTRATADO` | Tecnología que no existe en la nota técnica del prestador (`NT_NO_CRUZA`) |
# MAGIC | `SOBRECOSTO` | Valor unitario por encima de la tarifa pactada (tolerancia configurable) |
# MAGIC | `FECHA_INVALIDA` | Atención antes del nacimiento, en el futuro o sin fecha legible |
# MAGIC | `DX_SEXO_INCOMPATIBLE` | Diagnóstico restringido a un sexo distinto al del usuario |
# MAGIC
# MAGIC Silver **no elimina** registros con hallazgos: los marca. En auditoría, el hallazgo es el producto.
# MAGIC
# MAGIC Salida: `silver_servicios`. Al final se compara contra `eval_anomalias_sembradas`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "health_claims", "Esquema")
dbutils.widgets.text("tolerancia_sobrecosto", "0.05", "Tolerancia sobrecosto (0.05 = 5 %)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
TOLERANCIA = float(dbutils.widgets.get("tolerancia_sobrecosto"))


def t(nombre: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{nombre}"

# COMMAND ----------

# La lógica de las reglas vive en src/health_audit (probada con pytest en CI); el notebook solo lee y escribe tablas.
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from pyspark.sql import functions as F

from health_audit import silver as S

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Aplanar: factura → usuario → servicio

# COMMAND ----------

servicios = S.aplanar_servicios(spark.table(t("bronze_rips")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cruzar con nota técnica, prestadores y CIE-10

# COMMAND ----------

base = S.cruzar_referencias(
    servicios,
    spark.table(t("bronze_ref_nota_tecnica")),
    spark.table(t("bronze_ref_prestadores")),
    spark.table(t("bronze_ref_cie10")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reglas de auditoría
# MAGIC
# MAGIC Definidas en `src/health_audit/silver.py` (`aplicar_reglas`). El orden de `S.REGLAS` es la prioridad
# MAGIC del hallazgo principal.

# COMMAND ----------

auditado = S.aplicar_reglas(base, TOLERANCIA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Valorización
# MAGIC
# MAGIC - `VALORIZADO_SEGUN_NT`: la tecnología cruza con la NT; se reconoce `cantidad × min(valor facturado, tarifa NT)`.
# MAGIC - `NT_NO_CRUZA`: no está contratada; no se reconoce valor.
# MAGIC - Los duplicados no se reconocen.
# MAGIC - `glosa_potencial` = lo facturado que no se reconoce.

# COMMAND ----------

silver = S.valorizar(auditado)

(
    silver.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(t("silver_servicios"))
)
print(f"silver_servicios: {spark.table(t('silver_servicios')).count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumen de auditoría

# COMMAND ----------

display(
    spark.table(t("silver_servicios"))
    .groupBy(F.coalesce("hallazgo_principal", F.lit("SIN_HALLAZGO")).alias("hallazgo_principal"))
    .agg(
        F.count("*").alias("servicios"),
        F.sum("vr_servicio").alias("valor_facturado"),
        F.sum("glosa_potencial").alias("glosa_potencial"),
    )
    .orderBy(F.desc("servicios"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Evaluación contra las anomalías sembradas
# MAGIC
# MAGIC Las etiquetas se usan **solo aquí**, para medir las reglas. Precisión = de lo que marcamos,
# MAGIC cuánto era anomalía real; recall = de las anomalías reales, cuántas encontramos.

# COMMAND ----------

evaluacion = S.evaluar_servicios(spark.table(t("silver_servicios")), spark.table(t("eval_anomalias_sembradas")))

m = S.metricas(evaluacion)
print(f"Verdaderos positivos: {m['vp']:,} | falsos positivos: {m['fp']:,} | falsos negativos: {m['fn']:,}")
print(f"Precisión: {m['precision']:.1%} | Recall: {m['recall']:.1%}")

# COMMAND ----------

# Recall por tipo de anomalía: ¿la regla correcta encontró cada anomalía sembrada?
display(S.recall_por_tipo(evaluacion))
