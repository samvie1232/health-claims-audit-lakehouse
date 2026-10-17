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

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DINERO = "decimal(18,2)"


def a_timestamp(col: str):
    """Fecha-hora RIPS ('AAAA-MM-DD HH:MM'); NULL si no se puede leer (seguro con ANSI)."""
    texto = f"cast({col} as string)"
    return F.coalesce(
        F.expr(f"try_to_timestamp({texto}, 'yyyy-MM-dd HH:mm')"),
        F.expr(f"try_to_timestamp({texto}, 'yyyy-MM-dd HH:mm:ss')"),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Aplanar: factura → usuario → servicio

# COMMAND ----------

usuarios = (
    spark.table(t("bronze_rips"))
    .select(
        F.col("numDocumentoIdObligado").cast("string").alias("nit_prestador"),
        F.col("numFactura").alias("num_factura"),
        F.col("_periodo").alias("periodo"),
        F.col("_source_file").alias("archivo_origen"),
        F.explode("usuarios").alias("u"),
    )
)

# tipo de servicio -> (campo del código, campo de la fecha, cantidad, valor unitario)
CAMPOS = {
    "consultas":      ("s.codConsulta",        "s.fechaInicioAtencion", "1",                      "s.vrServicio"),
    "procedimientos": ("s.codProcedimiento",   "s.fechaInicioAtencion", "1",                      "s.vrServicio"),
    "medicamentos":   ("s.codTecnologiaSalud", "s.fechaDispensAdmon",   "s.cantidadMedicamento",  "s.vrUnitMedicamento"),
}


def servicios_de(tipo: str) -> DataFrame:
    codigo, fecha, cantidad, vr_unit = CAMPOS[tipo]
    return (
        usuarios
        .select("*", F.explode(F.col(f"u.servicios.{tipo}")).alias("s"))
        .select(
            "nit_prestador", "num_factura", "periodo", "archivo_origen",
            F.col("u.consecutivo").cast("int").alias("usuario_consecutivo"),
            F.col("u.tipoDocumentoIdentificacion").alias("tipo_documento"),
            F.col("u.numDocumentoIdentificacion").cast("string").alias("num_documento"),
            F.col("u.tipoUsuario").cast("string").alias("tipo_usuario"),
            F.expr("try_to_date(cast(u.fechaNacimiento as string), 'yyyy-MM-dd')").alias("fecha_nacimiento"),
            F.col("u.codSexo").alias("sexo"),
            F.col("u.codMunicipioResidencia").cast("string").alias("cod_municipio"),
            F.lit(tipo).alias("tipo_servicio"),
            F.col("s.consecutivo").cast("int").alias("consecutivo"),
            F.expr(codigo).cast("string").alias("codigo"),
            F.col("s.codDiagnosticoPrincipal").alias("cod_dx"),
            a_timestamp(fecha).alias("fecha_atencion"),
            F.col("s.numAutorizacion").cast("string").alias("num_autorizacion"),
            F.expr(cantidad).cast("int").alias("cantidad"),
            F.expr(vr_unit).cast(DINERO).alias("vr_unitario"),
            F.col("s.vrServicio").cast(DINERO).alias("vr_servicio"),
        )
    )


servicios = servicios_de("consultas").unionByName(servicios_de("procedimientos")).unionByName(servicios_de("medicamentos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cruzar con nota técnica, prestadores y CIE-10

# COMMAND ----------

nota_tecnica = spark.table(t("bronze_ref_nota_tecnica")).select(
    F.col("nit_prestador").cast("string").alias("nit_prestador"),
    F.col("codigo").cast("string").alias("codigo"),
    F.col("descripcion").alias("descripcion_nt"),
    F.col("valor_unitario_nt").cast(DINERO).alias("valor_unitario_nt"),
)
prestadores = spark.table(t("bronze_ref_prestadores")).select(
    F.col("nit_prestador").cast("string").alias("nit_prestador"),
    F.col("nombre").alias("nombre_prestador"),
    "modalidad",
)
cie10 = spark.table(t("bronze_ref_cie10")).select(
    F.col("codigo").cast("string").alias("cod_dx"),
    F.nullif(F.trim(F.col("sexo_restringido").cast("string")), F.lit("")).alias("dx_sexo_restringido"),
)

base = (
    servicios
    .join(F.broadcast(nota_tecnica), ["nit_prestador", "codigo"], "left")
    .join(F.broadcast(prestadores), "nit_prestador", "left")
    .join(F.broadcast(cie10), "cod_dx", "left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Reglas de auditoría

# COMMAND ----------

def bandera(condicion):
    return F.coalesce(condicion, F.lit(False))


ventana_dup = Window.partitionBy(
    "nit_prestador", "num_documento", "tipo_servicio", "codigo", "fecha_atencion", "vr_unitario", "cantidad"
).orderBy("num_factura", "usuario_consecutivo", "consecutivo")

auditado = (
    base
    .withColumn("dq_duplicado", F.row_number().over(ventana_dup) > 1)
    .withColumn("dq_no_contratado", F.col("valor_unitario_nt").isNull())
    .withColumn("dq_sobrecosto", bandera(F.col("vr_unitario") > F.col("valor_unitario_nt") * F.lit(1 + TOLERANCIA)))
    .withColumn("dq_fecha_invalida", bandera(
        F.col("fecha_atencion").isNull()
        | (F.to_date("fecha_atencion") < F.col("fecha_nacimiento"))
        | (F.col("fecha_atencion") > F.current_timestamp())
    ))
    .withColumn("dq_dx_sexo_incompatible", bandera(
        F.col("dx_sexo_restringido").isNotNull() & (F.col("dx_sexo_restringido") != F.col("sexo"))
    ))
)

REGLAS = [  # orden = prioridad del hallazgo principal
    ("dq_duplicado", "DUPLICADO"),
    ("dq_no_contratado", "NO_CONTRATADO"),
    ("dq_sobrecosto", "SOBRECOSTO"),
    ("dq_fecha_invalida", "FECHA_INVALIDA"),
    ("dq_dx_sexo_incompatible", "DX_SEXO_INCOMPATIBLE"),
]

auditado = (
    auditado
    .withColumn("hallazgos", F.filter(F.array(*[F.when(F.col(c), F.lit(n)) for c, n in REGLAS]), lambda x: x.isNotNull()))
    .withColumn("hallazgo_principal", F.expr("try_element_at(hallazgos, 1)"))
    .withColumn("tiene_hallazgo", F.size("hallazgos") > 0)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Valorización
# MAGIC
# MAGIC - `VALORIZADO_SEGUN_NT`: la tecnología cruza con la NT; se reconoce `cantidad × min(valor facturado, tarifa NT)`.
# MAGIC - `NT_NO_CRUZA`: no está contratada; no se reconoce valor.
# MAGIC - Los duplicados no se reconocen.
# MAGIC - `glosa_potencial` = lo facturado que no se reconoce.

# COMMAND ----------

silver = (
    auditado
    .withColumn("estado_valorizacion", F.when(F.col("dq_no_contratado"), "NT_NO_CRUZA").otherwise("VALORIZADO_SEGUN_NT"))
    .withColumn(
        "valor_reconocido",
        F.when(F.col("dq_duplicado") | F.col("dq_no_contratado"), F.lit(0).cast(DINERO))
         .otherwise((F.col("cantidad") * F.least("vr_unitario", "valor_unitario_nt")).cast(DINERO)),
    )
    .withColumn("glosa_potencial", (F.col("vr_servicio") - F.col("valor_reconocido")).cast(DINERO))
    .withColumn("_procesado_en", F.current_timestamp())
    .select(
        "periodo", "nit_prestador", "nombre_prestador", "modalidad", "num_factura",
        "usuario_consecutivo", "tipo_documento", "num_documento", "tipo_usuario",
        "fecha_nacimiento", "sexo", "cod_municipio",
        "tipo_servicio", "consecutivo", "codigo", "descripcion_nt", "cod_dx", "fecha_atencion",
        "num_autorizacion", "cantidad", "vr_unitario", "vr_servicio", "valor_unitario_nt",
        "estado_valorizacion", "valor_reconocido", "glosa_potencial",
        "dq_duplicado", "dq_no_contratado", "dq_sobrecosto", "dq_fecha_invalida", "dq_dx_sexo_incompatible",
        "hallazgos", "hallazgo_principal", "tiene_hallazgo",
        "archivo_origen", "_procesado_en",
    )
)

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

LLAVE = ["num_factura", "usuario_consecutivo", "tipo_servicio", "consecutivo"]

etiquetas = spark.table(t("eval_anomalias_sembradas")).select(
    F.col("numFactura").alias("num_factura"),
    F.col("usuario_consecutivo").cast("int"),
    "tipo_servicio",
    F.col("consecutivo").cast("int"),
    "tipo_anomalia",
)

evaluacion = (
    spark.table(t("silver_servicios")).select(*LLAVE, "tiene_hallazgo", "hallazgo_principal", "hallazgos")
    .join(etiquetas, LLAVE, "left")
    .withColumn("es_anomalia", F.col("tipo_anomalia").isNotNull())
)

m = evaluacion.agg(
    F.sum((F.col("es_anomalia") & F.col("tiene_hallazgo")).cast("int")).alias("vp"),
    F.sum((~F.col("es_anomalia") & F.col("tiene_hallazgo")).cast("int")).alias("fp"),
    F.sum((F.col("es_anomalia") & ~F.col("tiene_hallazgo")).cast("int")).alias("fn"),
).first()
precision = m.vp / (m.vp + m.fp) if (m.vp + m.fp) else 0.0
recall = m.vp / (m.vp + m.fn) if (m.vp + m.fn) else 0.0
print(f"Verdaderos positivos: {m.vp:,} | falsos positivos: {m.fp:,} | falsos negativos: {m.fn:,}")
print(f"Precisión: {precision:.1%} | Recall: {recall:.1%}")

# COMMAND ----------

# Recall por tipo de anomalía: ¿la regla correcta encontró cada anomalía sembrada?
display(
    evaluacion.where("es_anomalia")
    .groupBy("tipo_anomalia")
    .agg(
        F.count("*").alias("sembradas"),
        F.sum(F.expr("array_contains(hallazgos, tipo_anomalia)").cast("int")).alias("detectadas_por_su_regla"),
    )
    .withColumn("recall", F.round(F.col("detectadas_por_su_regla") / F.col("sembradas"), 4))
    .orderBy("tipo_anomalia")
)
