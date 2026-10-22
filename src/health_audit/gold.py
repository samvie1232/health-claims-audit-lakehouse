"""Gold: riesgo por prestador y mes.

| Tabla | Contenido |
| --- | --- |
| gold_prestador_mes | Facturado, reconocido, glosa y hallazgos por prestador-mes |
| gold_ejecucion_pgp | Ejecución mensual de los contratos PGP frente a sus bandas |
| gold_alertas | Sobreejecución PGP y cambios de patrón (upcoding) |
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from health_audit.silver import DINERO

BANDERAS = ["dq_duplicado", "dq_no_contratado", "dq_sobrecosto", "dq_fecha_invalida", "dq_dx_sexo_incompatible"]

# Etiqueta sembrada -> alerta que debería dispararla
EQUIVALENCIA_ALERTAS = {"SOBREEJECUCION_PGP": "SOBREEJECUCION_PGP", "UPCODING_PATRON": "CAMBIO_PATRON_CONSULTAS"}


def prestador_mes(silver: DataFrame) -> DataFrame:
    conteos = [F.sum(F.col(c).cast("int")).alias(f"n_{c.removeprefix('dq_')}") for c in BANDERAS]
    consulta_limpia = (F.col("tipo_servicio") == "consultas") & ~F.col("tiene_hallazgo")
    return (
        silver.groupBy("periodo", "nit_prestador", "nombre_prestador", "modalidad")
        .agg(
            F.countDistinct("num_factura").alias("facturas"),
            F.countDistinct("num_documento").alias("usuarios_atendidos"),
            F.count("*").alias("servicios"),
            F.sum("vr_servicio").cast(DINERO).alias("valor_facturado"),
            F.sum("valor_reconocido").cast(DINERO).alias("valor_reconocido"),
            F.sum("glosa_potencial").cast(DINERO).alias("glosa_potencial"),
            F.sum(F.col("tiene_hallazgo").cast("int")).alias("servicios_con_hallazgo"),
            *conteos,
            # valor medio por consulta, solo servicios limpios: base para detectar cambios de patrón
            F.avg(F.when(consulta_limpia, F.col("vr_unitario"))).cast(DINERO).alias("valor_medio_consulta"),
            F.sum(F.when(consulta_limpia, 1).otherwise(0)).alias("consultas_limpias"),
        )
        .withColumn("pct_glosa", F.round(F.col("glosa_potencial") / F.col("valor_facturado"), 4))
        .withColumn("pct_servicios_con_hallazgo", F.round(F.col("servicios_con_hallazgo") / F.col("servicios"), 4))
    )


def ejecucion_pgp(prestador_mes_df: DataFrame, pgp_bandas: DataFrame) -> DataFrame:
    """Ejecución al valor reconocido (tarifa NT, sin duplicados ni servicios no contratados)."""
    bandas = pgp_bandas.select(
        F.col("nit_prestador").cast("string").alias("nit_prestador"),
        F.col("valor_mensual_pgp").cast(DINERO).alias("valor_mensual_pgp"),
        F.col("limite_inferior").cast(DINERO).alias("limite_inferior"),
        F.col("limite_superior").cast(DINERO).alias("limite_superior"),
    )
    return (
        prestador_mes_df.where(F.col("modalidad") == "PGP")
        .join(bandas, "nit_prestador")
        .select(
            "periodo",
            "nit_prestador",
            "nombre_prestador",
            "usuarios_atendidos",
            F.col("valor_reconocido").alias("valor_ejecutado"),
            "valor_mensual_pgp",
            "limite_inferior",
            "limite_superior",
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


def alertas(
    prestador_mes_df: DataFrame,
    ejecucion_pgp_df: DataFrame,
    umbral_patron: float = 1.20,
    meses_historia: int = 6,
) -> DataFrame:
    """- SOBREEJECUCION_PGP: el mes supera el límite superior de la banda. Riesgo = exceso.
    - CAMBIO_PATRON_CONSULTAS: valor medio por consulta > umbral x mediana de los meses anteriores
      (mínimo 3 meses de historia). Riesgo = (valor medio - línea base) x consultas.
    """
    historia = Window.partitionBy("nit_prestador").orderBy("periodo").rowsBetween(-meses_historia, -1)

    patron = (
        prestador_mes_df.withColumn(
            "linea_base_consulta", F.percentile_approx("valor_medio_consulta", 0.5).over(historia)
        )
        .withColumn("meses_en_historia", F.count("valor_medio_consulta").over(historia))
        .withColumn("razon_vs_linea_base", F.round(F.col("valor_medio_consulta") / F.col("linea_base_consulta"), 4))
    )
    alertas_patron = patron.where(
        (F.col("meses_en_historia") >= 3) & (F.col("razon_vs_linea_base") > umbral_patron)
    ).select(
        "periodo",
        "nit_prestador",
        "nombre_prestador",
        "modalidad",
        F.lit("CAMBIO_PATRON_CONSULTAS").alias("tipo_alerta"),
        F.concat(
            F.lit("Valor medio por consulta "),
            F.format_number("valor_medio_consulta", 0),
            F.lit(" vs línea base "),
            F.format_number("linea_base_consulta", 0),
            F.lit(" (×"),
            F.format_number("razon_vs_linea_base", 2),
            F.lit(")"),
        ).alias("detalle"),
        ((F.col("valor_medio_consulta") - F.col("linea_base_consulta")) * F.col("consultas_limpias"))
        .cast(DINERO)
        .alias("valor_en_riesgo"),
    )
    alertas_pgp = ejecucion_pgp_df.where(F.col("estado_banda") == "SOBRE_LIMITE_SUPERIOR").select(
        "periodo",
        "nit_prestador",
        "nombre_prestador",
        F.lit("PGP").alias("modalidad"),
        F.lit("SOBREEJECUCION_PGP").alias("tipo_alerta"),
        F.concat(
            F.lit("Ejecución al "),
            F.format_number(F.col("pct_ejecucion") * 100, 1),
            F.lit(" % del valor pactado (límite 110 %)"),
        ).alias("detalle"),
        F.col("exceso_sobre_limite").alias("valor_en_riesgo"),
    )
    return alertas_pgp.unionByName(alertas_patron)


def evaluar_alertas(alertas_df: DataFrame, anomalias_prestador_mes: DataFrame) -> DataFrame:
    """Sembradas vs. alertadas por tipo de alerta, con precisión y recall."""
    mapa = F.create_map(*[F.lit(x) for par in EQUIVALENCIA_ALERTAS.items() for x in par])
    etiquetas = anomalias_prestador_mes.select(
        F.col("nit_prestador").cast("string").alias("nit_prestador"),
        F.col("periodo").cast("string").alias("periodo"),
        mapa[F.col("tipo_anomalia")].alias("tipo_alerta"),
        F.lit(True).alias("sembrada"),
    )
    detectadas = alertas_df.select("nit_prestador", "periodo", "tipo_alerta", F.lit(True).alias("alertada"))
    return (
        etiquetas.join(detectadas, ["nit_prestador", "periodo", "tipo_alerta"], "full_outer")
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
