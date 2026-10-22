"""Tests unitarios de las 5 reglas de Silver y de la valorización, con casos mínimos construidos a mano."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from health_audit.silver import aplicar_reglas, valorizar

ESQUEMA_BASE = T.StructType(
    [
        T.StructField("cod_dx", T.StringType()),
        T.StructField("nit_prestador", T.StringType()),
        T.StructField("codigo", T.StringType()),
        T.StructField("num_factura", T.StringType()),
        T.StructField("periodo", T.StringType()),
        T.StructField("archivo_origen", T.StringType()),
        T.StructField("usuario_consecutivo", T.IntegerType()),
        T.StructField("tipo_documento", T.StringType()),
        T.StructField("num_documento", T.StringType()),
        T.StructField("tipo_usuario", T.StringType()),
        T.StructField("fecha_nacimiento", T.DateType()),
        T.StructField("sexo", T.StringType()),
        T.StructField("cod_municipio", T.StringType()),
        T.StructField("tipo_servicio", T.StringType()),
        T.StructField("consecutivo", T.IntegerType()),
        T.StructField("fecha_atencion", T.TimestampType()),
        T.StructField("num_autorizacion", T.StringType()),
        T.StructField("cantidad", T.IntegerType()),
        T.StructField("vr_unitario", T.DecimalType(18, 2)),
        T.StructField("vr_servicio", T.DecimalType(18, 2)),
        T.StructField("descripcion_nt", T.StringType()),
        T.StructField("valor_unitario_nt", T.DecimalType(18, 2)),
        T.StructField("nombre_prestador", T.StringType()),
        T.StructField("modalidad", T.StringType()),
        T.StructField("dx_sexo_restringido", T.StringType()),
    ]
)

FECHA_OK = dt.datetime(2026, 3, 10, 9, 30)


def fila(consecutivo: int = 1, **cambios) -> dict:
    """Un servicio limpio: contratado, a tarifa, fecha válida, diagnóstico sin restricción."""
    base = {
        "cod_dx": "I10X",
        "nit_prestador": "900000001",
        "codigo": "890201",
        "num_factura": "F-1",
        "periodo": "202603",
        "archivo_origen": "f.json",
        "usuario_consecutivo": 1,
        "tipo_documento": "CC",
        "num_documento": "123",
        "tipo_usuario": "01",
        "fecha_nacimiento": dt.date(1980, 1, 1),
        "sexo": "H",
        "cod_municipio": "76001",
        "tipo_servicio": "consultas",
        "consecutivo": consecutivo,
        "fecha_atencion": FECHA_OK,
        "num_autorizacion": None,
        "cantidad": 1,
        "vr_unitario": Decimal("100.00"),
        "vr_servicio": Decimal("100.00"),
        "descripcion_nt": "Consulta",
        "valor_unitario_nt": Decimal("100.00"),
        "nombre_prestador": "IPS Uno",
        "modalidad": "EVENTO",
        "dx_sexo_restringido": None,
    }
    base.update(cambios)
    return base


@pytest.fixture
def auditar(spark):
    def _auditar(filas: list[dict], tolerancia: float = 0.05) -> list:
        df = spark.createDataFrame(filas, schema=ESQUEMA_BASE)
        return valorizar(aplicar_reglas(df, tolerancia)).orderBy("consecutivo").collect()

    return _auditar


def test_servicio_limpio_no_tiene_hallazgos(auditar):
    (r,) = auditar([fila()])
    assert r.hallazgos == [] and r.hallazgo_principal is None and not r.tiene_hallazgo
    assert r.estado_valorizacion == "VALORIZADO_SEGUN_NT"
    assert r.valor_reconocido == Decimal("100.00") and r.glosa_potencial == Decimal("0.00")


def test_duplicado_marca_solo_la_repeticion_y_no_se_reconoce(auditar):
    original, repetido = auditar([fila(1), fila(2)])
    assert not original.dq_duplicado
    assert repetido.dq_duplicado and repetido.hallazgo_principal == "DUPLICADO"
    assert repetido.valor_reconocido == Decimal("0.00") and repetido.glosa_potencial == Decimal("100.00")


def test_mismo_servicio_con_otro_valor_no_es_duplicado(auditar):
    a, b = auditar([fila(1), fila(2, vr_unitario=Decimal("90.00"), vr_servicio=Decimal("90.00"))])
    assert not a.dq_duplicado and not b.dq_duplicado


def test_no_contratado_no_se_reconoce(auditar):
    (r,) = auditar([fila(valor_unitario_nt=None, descripcion_nt=None)])
    assert r.dq_no_contratado and r.hallazgo_principal == "NO_CONTRATADO"
    assert r.estado_valorizacion == "NT_NO_CRUZA"
    assert r.valor_reconocido == Decimal("0.00") and r.glosa_potencial == Decimal("100.00")
    assert not r.dq_sobrecosto  # sin tarifa no hay contra qué comparar


@pytest.mark.parametrize("valor, esperado", [("105.00", False), ("105.01", True), ("150.00", True)])
def test_sobrecosto_respeta_la_tolerancia_del_5_por_ciento(auditar, valor, esperado):
    (r,) = auditar([fila(vr_unitario=Decimal(valor), vr_servicio=Decimal(valor))])
    assert r.dq_sobrecosto is esperado


def test_sobrecosto_reconoce_solo_la_tarifa_pactada(auditar):
    (r,) = auditar([fila(cantidad=3, vr_unitario=Decimal("150.00"), vr_servicio=Decimal("450.00"))])
    assert r.valor_reconocido == Decimal("300.00")  # 3 x min(150, 100)
    assert r.glosa_potencial == Decimal("150.00")


@pytest.mark.parametrize(
    "fecha_atencion",
    [
        None,  # ilegible
        dt.datetime(1979, 12, 31, 8, 0),  # antes del nacimiento
        dt.datetime(2999, 1, 1, 8, 0),  # en el futuro
    ],
)
def test_fecha_invalida(auditar, fecha_atencion):
    (r,) = auditar([fila(fecha_atencion=fecha_atencion)])
    assert r.dq_fecha_invalida and "FECHA_INVALIDA" in r.hallazgos


@pytest.mark.parametrize("restringido, sexo, esperado", [("M", "H", True), ("M", "M", False), (None, "H", False)])
def test_diagnostico_incompatible_con_el_sexo(auditar, restringido, sexo, esperado):
    (r,) = auditar([fila(cod_dx="O800", dx_sexo_restringido=restringido, sexo=sexo)])
    assert r.dq_dx_sexo_incompatible is esperado


def test_hallazgo_principal_sigue_la_prioridad_de_las_reglas(auditar):
    caro = {"vr_unitario": Decimal("200.00"), "vr_servicio": Decimal("200.00")}
    _, r = auditar([fila(1, **caro), fila(2, **caro)])
    assert r.hallazgos == ["DUPLICADO", "SOBRECOSTO"]
    assert r.hallazgo_principal == "DUPLICADO"


def test_silver_marca_pero_no_elimina_registros(auditar):
    filas = [fila(1), fila(2), fila(3, valor_unitario_nt=None), fila(4, fecha_atencion=None)]
    assert len(auditar(filas)) == len(filas)
