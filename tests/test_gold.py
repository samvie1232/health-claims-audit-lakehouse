"""Tests unitarios de Gold: bandas PGP y alerta de cambio de patrón (upcoding)."""

from __future__ import annotations

from decimal import Decimal

from pyspark.sql import types as T

from health_audit.gold import alertas, ejecucion_pgp

D = T.DecimalType(18, 2)
ESQUEMA_PM = T.StructType(
    [
        T.StructField("periodo", T.StringType()),
        T.StructField("nit_prestador", T.StringType()),
        T.StructField("nombre_prestador", T.StringType()),
        T.StructField("modalidad", T.StringType()),
        T.StructField("usuarios_atendidos", T.LongType()),
        T.StructField("valor_reconocido", D),
        T.StructField("valor_medio_consulta", D),
        T.StructField("consultas_limpias", T.LongType()),
    ]
)
ESQUEMA_BANDAS = T.StructType(
    [
        T.StructField("nit_prestador", T.StringType()),
        T.StructField("valor_mensual_pgp", D),
        T.StructField("limite_inferior", D),
        T.StructField("limite_superior", D),
    ]
)


def pm(periodo, nit="900", modalidad="EVENTO", reconocido="0", medio=None, consultas=0):
    return (
        periodo,
        nit,
        f"IPS {nit}",
        modalidad,
        10,
        Decimal(reconocido),
        Decimal(medio) if medio else None,
        consultas,
    )


def test_ejecucion_pgp_clasifica_los_tres_estados_de_banda(spark):
    meses = spark.createDataFrame(
        [
            pm("202601", "PGP1", "PGP", "1150"),  # sobre el límite superior (1100)
            pm("202602", "PGP1", "PGP", "1000"),  # dentro de banda
            pm("202603", "PGP1", "PGP", "850"),  # bajo el límite inferior (900)
            pm("202601", "EV1", "EVENTO", "5000"),  # no es PGP: se excluye
        ],
        ESQUEMA_PM,
    )
    bandas = spark.createDataFrame([("PGP1", Decimal("1000"), Decimal("900"), Decimal("1100"))], ESQUEMA_BANDAS)

    filas = {r.periodo: r for r in ejecucion_pgp(meses, bandas).collect()}
    assert set(filas) == {"202601", "202602", "202603"}
    assert filas["202601"].estado_banda == "SOBRE_LIMITE_SUPERIOR"
    assert filas["202601"].exceso_sobre_limite == Decimal("50.00")
    assert filas["202602"].estado_banda == "DENTRO_DE_BANDA" and filas["202602"].exceso_sobre_limite == 0
    assert filas["202603"].estado_banda == "BAJO_LIMITE_INFERIOR"


def _alertas_patron(spark, medios: list[str], umbral=1.20, consultas=50):
    meses = spark.createDataFrame(
        [pm(f"2026{m + 1:02d}", "EV1", "EVENTO", "0", v, consultas) for m, v in enumerate(medios)], ESQUEMA_PM
    )
    sin_pgp = ejecucion_pgp(spark.createDataFrame([], ESQUEMA_PM), spark.createDataFrame([], ESQUEMA_BANDAS))
    return [r for r in alertas(meses, sin_pgp, umbral, 6).collect() if r.tipo_alerta == "CAMBIO_PATRON_CONSULTAS"]


def test_alerta_de_patron_cuando_el_valor_por_consulta_sube_mas_del_umbral(spark):
    (a,) = _alertas_patron(spark, ["100", "100", "100", "130"])
    assert a.periodo == "202604"
    assert a.valor_en_riesgo == Decimal("1500.00")  # (130 - 100) x 50 consultas


def test_alerta_de_patron_exige_al_menos_tres_meses_de_historia(spark):
    assert _alertas_patron(spark, ["100", "100", "200"]) == []


def test_alerta_de_patron_no_dispara_por_debajo_del_umbral(spark):
    assert _alertas_patron(spark, ["100", "100", "100", "115"]) == []
