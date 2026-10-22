"""Silver: aplanar el RIPS, cruzar con la nota técnica, aplicar reglas de auditoría y valorizar.

| Regla | Qué detecta |
| --- | --- |
| DUPLICADO | Mismo prestador, usuario, tecnología, fecha y valor facturado más de una vez |
| NO_CONTRATADO | Tecnología que no existe en la nota técnica del prestador (NT_NO_CRUZA) |
| SOBRECOSTO | Valor unitario por encima de la tarifa pactada (tolerancia configurable) |
| FECHA_INVALIDA | Atención antes del nacimiento, en el futuro o sin fecha legible |
| DX_SEXO_INCOMPATIBLE | Diagnóstico restringido a un sexo distinto al del usuario |

Silver no elimina registros con hallazgos: los marca. En auditoría, el hallazgo es el producto.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

DINERO = "decimal(18,2)"

# tipo de servicio -> (campo del código, campo de la fecha, cantidad, valor unitario)
CAMPOS = {
    "consultas": ("s.codConsulta", "s.fechaInicioAtencion", "1", "s.vrServicio"),
    "procedimientos": ("s.codProcedimiento", "s.fechaInicioAtencion", "1", "s.vrServicio"),
    "medicamentos": ("s.codTecnologiaSalud", "s.fechaDispensAdmon", "s.cantidadMedicamento", "s.vrUnitMedicamento"),
}

REGLAS = [  # orden = prioridad del hallazgo principal
    ("dq_duplicado", "DUPLICADO"),
    ("dq_no_contratado", "NO_CONTRATADO"),
    ("dq_sobrecosto", "SOBRECOSTO"),
    ("dq_fecha_invalida", "FECHA_INVALIDA"),
    ("dq_dx_sexo_incompatible", "DX_SEXO_INCOMPATIBLE"),
]

COLUMNAS_SILVER = [
    "periodo",
    "nit_prestador",
    "nombre_prestador",
    "modalidad",
    "num_factura",
    "usuario_consecutivo",
    "tipo_documento",
    "num_documento",
    "tipo_usuario",
    "fecha_nacimiento",
    "sexo",
    "cod_municipio",
    "tipo_servicio",
    "consecutivo",
    "codigo",
    "descripcion_nt",
    "cod_dx",
    "fecha_atencion",
    "num_autorizacion",
    "cantidad",
    "vr_unitario",
    "vr_servicio",
    "valor_unitario_nt",
    "estado_valorizacion",
    "valor_reconocido",
    "glosa_potencial",
    "dq_duplicado",
    "dq_no_contratado",
    "dq_sobrecosto",
    "dq_fecha_invalida",
    "dq_dx_sexo_incompatible",
    "hallazgos",
    "hallazgo_principal",
    "tiene_hallazgo",
    "archivo_origen",
    "_procesado_en",
]

LLAVE_SERVICIO = ["num_factura", "usuario_consecutivo", "tipo_servicio", "consecutivo"]


def a_timestamp(col: str) -> Column:
    """Fecha-hora RIPS ('AAAA-MM-DD HH:MM'); NULL si no se puede leer (seguro con ANSI)."""
    texto = f"cast({col} as string)"
    return F.coalesce(
        F.expr(f"try_to_timestamp({texto}, 'yyyy-MM-dd HH:mm')"),
        F.expr(f"try_to_timestamp({texto}, 'yyyy-MM-dd HH:mm:ss')"),
    )


def bandera(condicion: Column) -> Column:
    """Una regla que no se puede evaluar (NULL) no es un hallazgo."""
    return F.coalesce(condicion, F.lit(False))


# ---------------------------------------------------------------------------
# 1. Aplanar: factura -> usuario -> servicio
# ---------------------------------------------------------------------------
def aplanar_servicios(bronze_rips: DataFrame) -> DataFrame:
    """Una fila por servicio prestado (consultas, procedimientos y medicamentos)."""
    usuarios = bronze_rips.select(
        F.col("numDocumentoIdObligado").cast("string").alias("nit_prestador"),
        F.col("numFactura").alias("num_factura"),
        F.col("_periodo").alias("periodo"),
        F.col("_source_file").alias("archivo_origen"),
        F.explode("usuarios").alias("u"),
    )

    def servicios_de(tipo: str) -> DataFrame:
        codigo, fecha, cantidad, vr_unit = CAMPOS[tipo]
        return usuarios.select("*", F.explode(F.col(f"u.servicios.{tipo}")).alias("s")).select(
            "nit_prestador",
            "num_factura",
            "periodo",
            "archivo_origen",
            F.col("u.consecutivo").cast("int").alias("usuario_consecutivo"),
            F.col("u.tipoDocumentoIdentificacion").alias("tipo_documento"),
            F.col("u.numDocumentoIdentificacion").cast("string").alias("num_documento"),
            F.col("u.tipoUsuario").cast("string").alias("tipo_usuario"),
            # try_to_timestamp + cast: portable entre Databricks y Spark OSS (try_to_date no existe en OSS 4.0)
            F.expr("cast(try_to_timestamp(cast(u.fechaNacimiento as string), 'yyyy-MM-dd') as date)").alias(
                "fecha_nacimiento"
            ),
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

    return (
        servicios_de("consultas").unionByName(servicios_de("procedimientos")).unionByName(servicios_de("medicamentos"))
    )


# ---------------------------------------------------------------------------
# 2. Cruzar con nota técnica, prestadores y CIE-10
# ---------------------------------------------------------------------------
def cruzar_referencias(
    servicios: DataFrame, nota_tecnica: DataFrame, prestadores: DataFrame, cie10: DataFrame
) -> DataFrame:
    nt = nota_tecnica.select(
        F.col("nit_prestador").cast("string").alias("nit_prestador"),
        F.col("codigo").cast("string").alias("codigo"),
        F.col("descripcion").alias("descripcion_nt"),
        F.col("valor_unitario_nt").cast(DINERO).alias("valor_unitario_nt"),
    )
    prest = prestadores.select(
        F.col("nit_prestador").cast("string").alias("nit_prestador"),
        F.col("nombre").alias("nombre_prestador"),
        "modalidad",
    )
    dx = cie10.select(
        F.col("codigo").cast("string").alias("cod_dx"),
        F.nullif(F.trim(F.col("sexo_restringido").cast("string")), F.lit("")).alias("dx_sexo_restringido"),
    )
    return (
        servicios.join(F.broadcast(nt), ["nit_prestador", "codigo"], "left")
        .join(F.broadcast(prest), "nit_prestador", "left")
        .join(F.broadcast(dx), "cod_dx", "left")
    )


# ---------------------------------------------------------------------------
# 3. Reglas de auditoría
# ---------------------------------------------------------------------------
def aplicar_reglas(base: DataFrame, tolerancia_sobrecosto: float = 0.05) -> DataFrame:
    ventana_dup = Window.partitionBy(
        "nit_prestador", "num_documento", "tipo_servicio", "codigo", "fecha_atencion", "vr_unitario", "cantidad"
    ).orderBy("num_factura", "usuario_consecutivo", "consecutivo")

    auditado = (
        base.withColumn("dq_duplicado", F.row_number().over(ventana_dup) > 1)
        .withColumn("dq_no_contratado", F.col("valor_unitario_nt").isNull())
        .withColumn(
            "dq_sobrecosto",
            bandera(F.col("vr_unitario") > F.col("valor_unitario_nt") * F.lit(1 + tolerancia_sobrecosto)),
        )
        .withColumn(
            "dq_fecha_invalida",
            bandera(
                F.col("fecha_atencion").isNull()
                | (F.to_date("fecha_atencion") < F.col("fecha_nacimiento"))
                | (F.col("fecha_atencion") > F.current_timestamp())
            ),
        )
        .withColumn(
            "dq_dx_sexo_incompatible",
            bandera(F.col("dx_sexo_restringido").isNotNull() & (F.col("dx_sexo_restringido") != F.col("sexo"))),
        )
    )
    return (
        auditado.withColumn(
            "hallazgos",
            F.filter(F.array(*[F.when(F.col(c), F.lit(n)) for c, n in REGLAS]), lambda x: x.isNotNull()),
        )
        .withColumn("hallazgo_principal", F.expr("try_element_at(hallazgos, 1)"))
        .withColumn("tiene_hallazgo", F.size("hallazgos") > 0)
    )


# ---------------------------------------------------------------------------
# 4. Valorización
# ---------------------------------------------------------------------------
def valorizar(auditado: DataFrame) -> DataFrame:
    """- VALORIZADO_SEGUN_NT: se reconoce cantidad x min(valor facturado, tarifa NT).
    - NT_NO_CRUZA: no está contratada; no se reconoce valor.
    - Los duplicados no se reconocen.
    - glosa_potencial = lo facturado que no se reconoce.
    """
    return (
        auditado.withColumn(
            "estado_valorizacion",
            F.when(F.col("dq_no_contratado"), "NT_NO_CRUZA").otherwise("VALORIZADO_SEGUN_NT"),
        )
        .withColumn(
            "valor_reconocido",
            F.when(F.col("dq_duplicado") | F.col("dq_no_contratado"), F.lit(0).cast(DINERO)).otherwise(
                (F.col("cantidad") * F.least("vr_unitario", "valor_unitario_nt")).cast(DINERO)
            ),
        )
        .withColumn("glosa_potencial", (F.col("vr_servicio") - F.col("valor_reconocido")).cast(DINERO))
        .withColumn("_procesado_en", F.current_timestamp())
        .select(*COLUMNAS_SILVER)
    )


def construir_silver(
    bronze_rips: DataFrame,
    nota_tecnica: DataFrame,
    prestadores: DataFrame,
    cie10: DataFrame,
    tolerancia_sobrecosto: float = 0.05,
) -> DataFrame:
    servicios = aplanar_servicios(bronze_rips)
    base = cruzar_referencias(servicios, nota_tecnica, prestadores, cie10)
    return valorizar(aplicar_reglas(base, tolerancia_sobrecosto))


# ---------------------------------------------------------------------------
# 5. Evaluación contra las anomalías sembradas (solo para medir, nunca para construir)
# ---------------------------------------------------------------------------
def evaluar_servicios(silver: DataFrame, anomalias_sembradas: DataFrame) -> DataFrame:
    etiquetas = anomalias_sembradas.select(
        F.col("numFactura").alias("num_factura"),
        F.col("usuario_consecutivo").cast("int"),
        "tipo_servicio",
        F.col("consecutivo").cast("int"),
        "tipo_anomalia",
    )
    return (
        silver.select(*LLAVE_SERVICIO, "tiene_hallazgo", "hallazgo_principal", "hallazgos")
        .join(etiquetas, LLAVE_SERVICIO, "left")
        .withColumn("es_anomalia", F.col("tipo_anomalia").isNotNull())
    )


def metricas(evaluacion: DataFrame) -> dict:
    """Precisión = de lo marcado, cuánto era anomalía real; recall = de lo real, cuánto se encontró."""
    m = evaluacion.agg(
        F.sum((F.col("es_anomalia") & F.col("tiene_hallazgo")).cast("int")).alias("vp"),
        F.sum((~F.col("es_anomalia") & F.col("tiene_hallazgo")).cast("int")).alias("fp"),
        F.sum((F.col("es_anomalia") & ~F.col("tiene_hallazgo")).cast("int")).alias("fn"),
    ).first()
    vp, fp, fn = (m.vp or 0), (m.fp or 0), (m.fn or 0)
    return {
        "vp": vp,
        "fp": fp,
        "fn": fn,
        "precision": vp / (vp + fp) if (vp + fp) else 0.0,
        "recall": vp / (vp + fn) if (vp + fn) else 0.0,
    }


def recall_por_tipo(evaluacion: DataFrame) -> DataFrame:
    """¿La regla correcta encontró cada anomalía sembrada?"""
    return (
        evaluacion.where("es_anomalia")
        .groupBy("tipo_anomalia")
        .agg(
            F.count("*").alias("sembradas"),
            F.sum(F.expr("array_contains(hallazgos, tipo_anomalia)").cast("int")).alias("detectadas_por_su_regla"),
        )
        .withColumn("recall", F.round(F.col("detectadas_por_su_regla") / F.col("sembradas"), 4))
        .orderBy("tipo_anomalia")
    )
