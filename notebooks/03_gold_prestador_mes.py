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

from pyspark.sql import Window
from pyspark.sql import functions as F

DINERO = "decimal(18,2)"
silver = spark.table(t("silver_servicios"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `gold_prestador_mes`

# COMMAND ----------

conteos_hallazgos = [
    F.sum(F.col(c).cast("int")).alias(f"n_{c.removeprefix('dq_')}")
    for c in ["dq_duplicado", "dq_no_contratado", "dq_sobrecosto", "dq_fecha_invalida", "dq_dx_sexo_incompatible"]
]

prestador_mes = (
    silver
    .groupBy("periodo", "nit_prestador", "nombre_prestador", "modalidad")
    .agg(
        F.countDistinct("num_factura").alias("facturas"),
        F.countDistinct("num_documento").alias("usuarios_atendidos"),
        F.count("*").alias("servicios"),
        F.sum("vr_servicio").cast(DINERO).alias("valor_facturado"),
        F.sum("valor_reconocido").cast(DINERO).alias("valor_reconocido"),
        F.sum("glosa_potencial").cast(DINERO).alias("glosa_potencial"),
        F.sum(F.col("tiene_hallazgo").cast("int")).alias("servicios_con_hallazgo"),
        *conteos_hallazgos,
        # valor medio por consulta, solo servicios limpios: base para detectar cambios de patrón
        F.avg(F.when((F.col("tipo_servicio") == "consultas") & ~F.col("tiene_hallazgo"), F.col("vr_unitario")))
         .cast(DINERO).alias("valor_medio_consulta"),
        F.sum(F.when((F.col("tipo_servicio") == "consultas") & ~F.col("tiene_hallazgo"), 1).otherwise(0))
         .alias("consultas_limpias"),
    )
    .withColumn("pct_glosa", F.round(F.col("glosa_potencial") / F.col("valor_facturado"), 4))
    .withColumn("pct_servicios_con_hallazgo", F.round(F.col("servicios_con_hallazgo") / F.col("servicios"), 4))
)

prestador_mes.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_prestador_mes"))
display(spark.table(t("gold_prestador_mes")).orderBy("periodo", "nit_prestador"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `gold_ejecucion_pgp`
# MAGIC
# MAGIC En un contrato PGP la EPS paga un valor mensual pactado con una banda de tolerancia (±10 %).
# MAGIC La ejecución se mide al valor reconocido (tarifa NT, sin duplicados ni servicios no contratados).

# COMMAND ----------

bandas = spark.table(t("bronze_ref_pgp_bandas")).select(
    F.col("nit_prestador").cast("string").alias("nit_prestador"),
    F.col("valor_mensual_pgp").cast(DINERO).alias("valor_mensual_pgp"),
    F.col("limite_inferior").cast(DINERO).alias("limite_inferior"),
    F.col("limite_superior").cast(DINERO).alias("limite_superior"),
)

ejecucion_pgp = (
    spark.table(t("gold_prestador_mes"))
    .where(F.col("modalidad") == "PGP")
    .join(bandas, "nit_prestador")
    .select(
        "periodo", "nit_prestador", "nombre_prestador", "usuarios_atendidos",
        F.col("valor_reconocido").alias("valor_ejecutado"),
        "valor_mensual_pgp", "limite_inferior", "limite_superior",
    )
    .withColumn("pct_ejecucion", F.round(F.col("valor_ejecutado") / F.col("valor_mensual_pgp"), 4))
    .withColumn(
        "estado_banda",
        F.when(F.col("valor_ejecutado") > F.col("limite_superior"), "SOBRE_LIMITE_SUPERIOR")
         .when(F.col("valor_ejecutado") < F.col("limite_inferior"), "BAJO_LIMITE_INFERIOR")
         .otherwise("DENTRO_DE_BANDA"),
    )
    .withColumn(
        "exceso_sobre_limite",
        F.greatest(F.col("valor_ejecutado") - F.col("limite_superior"), F.lit(0)).cast(DINERO),
    )
)

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

historia = (
    Window.partitionBy("nit_prestador").orderBy("periodo").rowsBetween(-MESES_HISTORIA, -1)
)

patron = (
    spark.table(t("gold_prestador_mes"))
    .withColumn("linea_base_consulta", F.percentile_approx("valor_medio_consulta", 0.5).over(historia))
    .withColumn("meses_en_historia", F.count("valor_medio_consulta").over(historia))
    .withColumn("razon_vs_linea_base", F.round(F.col("valor_medio_consulta") / F.col("linea_base_consulta"), 4))
)

alertas_patron = (
    patron
    .where((F.col("meses_en_historia") >= 3) & (F.col("razon_vs_linea_base") > UMBRAL_PATRON))
    .select(
        "periodo", "nit_prestador", "nombre_prestador", "modalidad",
        F.lit("CAMBIO_PATRON_CONSULTAS").alias("tipo_alerta"),
        F.concat(
            F.lit("Valor medio por consulta "), F.format_number("valor_medio_consulta", 0),
            F.lit(" vs línea base "), F.format_number("linea_base_consulta", 0),
            F.lit(" (×"), F.format_number("razon_vs_linea_base", 2), F.lit(")"),
        ).alias("detalle"),
        ((F.col("valor_medio_consulta") - F.col("linea_base_consulta")) * F.col("consultas_limpias"))
         .cast(DINERO).alias("valor_en_riesgo"),
    )
)

alertas_pgp = (
    spark.table(t("gold_ejecucion_pgp"))
    .where(F.col("estado_banda") == "SOBRE_LIMITE_SUPERIOR")
    .select(
        "periodo", "nit_prestador", "nombre_prestador", F.lit("PGP").alias("modalidad"),
        F.lit("SOBREEJECUCION_PGP").alias("tipo_alerta"),
        F.concat(
            F.lit("Ejecución al "), F.format_number(F.col("pct_ejecucion") * 100, 1),
            F.lit(" % del valor pactado (límite 110 %)"),
        ).alias("detalle"),
        F.col("exceso_sobre_limite").alias("valor_en_riesgo"),
    )
)

alertas = alertas_pgp.unionByName(alertas_patron)
alertas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_alertas"))
display(spark.table(t("gold_alertas")).orderBy(F.desc("valor_en_riesgo")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Evaluación contra las anomalías prestador-mes sembradas

# COMMAND ----------

EQUIVALENCIA = {"SOBREEJECUCION_PGP": "SOBREEJECUCION_PGP", "UPCODING_PATRON": "CAMBIO_PATRON_CONSULTAS"}
mapa = F.create_map(*[F.lit(x) for par in EQUIVALENCIA.items() for x in par])

etiquetas = (
    spark.table(t("eval_anomalias_prestador_mes"))
    .select(
        F.col("nit_prestador").cast("string").alias("nit_prestador"),
        F.col("periodo").cast("string").alias("periodo"),
        mapa[F.col("tipo_anomalia")].alias("tipo_alerta"),
        F.lit(True).alias("sembrada"),
    )
)
detectadas = spark.table(t("gold_alertas")).select("nit_prestador", "periodo", "tipo_alerta", F.lit(True).alias("alertada"))

evaluacion = etiquetas.join(detectadas, ["nit_prestador", "periodo", "tipo_alerta"], "full_outer")

display(
    evaluacion
    .groupBy("tipo_alerta")
    .agg(
        F.sum(F.col("sembrada").isNotNull().cast("int")).alias("sembradas"),
        F.sum(F.col("alertada").isNotNull().cast("int")).alias("alertas"),
        F.sum((F.col("sembrada").isNotNull() & F.col("alertada").isNotNull()).cast("int")).alias("aciertos"),
    )
    .withColumn("precision", F.round(F.col("aciertos") / F.col("alertas"), 4))
    .withColumn("recall", F.round(F.col("aciertos") / F.col("sembradas"), 4))
    .orderBy("tipo_alerta")
)
