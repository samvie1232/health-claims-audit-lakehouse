# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — riesgo por prestador y mes
# MAGIC
# MAGIC Silver responde "¿este servicio está bien facturado?". Gold responde lo que pregunta una gerencia:
# MAGIC **¿qué prestador, en qué mes, me está costando más de lo pactado y por qué?**
# MAGIC
# MAGIC | Tabla | Contenido |
# MAGIC | --- | --- |
# MAGIC | `gold_prestador_mes` | Facturado, reconocido, glosa y hallazgos por prestador-mes |
# MAGIC | `gold_ejecucion_pgp` | Ejecución mensual de los contratos PGP frente a sus bandas |
# MAGIC | `gold_alertas` | Alertas que una regla fila por fila no ve: sobreejecución PGP y cambios de patrón (upcoding) |
# MAGIC
# MAGIC Al final, las alertas se evalúan contra `eval_anomalias_prestador_mes`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catálogo")
dbutils.widgets.text("schema", "health_claims", "Esquema")
dbutils.widgets.text("umbral_patron", "1.20", "Alerta si el valor por consulta supera su mediana histórica × este factor")
dbutils.widgets.text("meses_historia", "6", "Meses de historia para la línea base")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
UMBRAL_PATRON = float(dbutils.widgets.get("umbral_patron"))
MESES_HISTORIA = int(dbutils.widgets.get("meses_historia"))


def t(nombre: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{nombre}"

# COMMAND ----------

# La lógica de las reglas vive en src/health_audit (probada con pytest en CI); el notebook solo lee y escribe tablas.
import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

from pyspark.sql import functions as F

from health_audit import gold as G

silver = spark.table(t("silver_servicios"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `gold_prestador_mes`

# COMMAND ----------

prestador_mes = G.prestador_mes(silver)

prestador_mes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_prestador_mes"))
display(spark.table(t("gold_prestador_mes")).orderBy("periodo", "nit_prestador"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `gold_ejecucion_pgp`
# MAGIC
# MAGIC En un contrato PGP la EPS paga un valor mensual pactado con una banda de tolerancia (±10 %).
# MAGIC La ejecución se mide al valor reconocido (tarifa NT, sin duplicados ni servicios no contratados).

# COMMAND ----------

ejecucion_pgp = G.ejecucion_pgp(spark.table(t("gold_prestador_mes")), spark.table(t("bronze_ref_pgp_bandas")))

ejecucion_pgp.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_ejecucion_pgp"))
display(spark.table(t("gold_ejecucion_pgp")).orderBy("nit_prestador", "periodo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `gold_alertas`
# MAGIC
# MAGIC - **SOBREEJECUCION_PGP**: el mes se sale por encima de la banda. Valor en riesgo = exceso sobre el límite.
# MAGIC - **CAMBIO_PATRON_CONSULTAS**: el valor medio por consulta supera `umbral_patron` × la mediana de los
# MAGIC   `meses_historia` meses anteriores del mismo prestador (mínimo 3 meses de historia). Detecta upcoding:
# MAGIC   cada consulta cobra su tarifa correcta, pero el prestador empezó a facturar las más caras.
# MAGIC   Valor en riesgo = (valor medio actual − línea base) × consultas.

# COMMAND ----------

alertas = G.alertas(
    spark.table(t("gold_prestador_mes")),
    spark.table(t("gold_ejecucion_pgp")),
    umbral_patron=UMBRAL_PATRON,
    meses_historia=MESES_HISTORIA,
)
alertas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_alertas"))
display(spark.table(t("gold_alertas")).orderBy(F.desc("valor_en_riesgo")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Evaluación contra las anomalías prestador-mes sembradas

# COMMAND ----------

display(G.evaluar_alertas(spark.table(t("gold_alertas")), spark.table(t("eval_anomalias_prestador_mes"))))
