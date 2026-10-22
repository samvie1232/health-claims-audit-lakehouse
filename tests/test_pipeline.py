"""Pipeline completo sobre datos sintéticos: generador -> Bronze (local) -> Silver -> Gold -> evaluación.

Las etiquetas sembradas por el generador solo se usan aquí, para medir. Nunca para construir las reglas.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from conftest import generar
from health_audit import gold as G
from health_audit import silver as S


@pytest.fixture(scope="module")
def silver(datos_sinteticos):
    d = datos_sinteticos
    return S.construir_silver(d["bronze_rips"], d["nota_tecnica"], d["prestadores"], d["cie10"], 0.05).cache()


def test_silver_tiene_una_fila_por_servicio_y_no_descarta_nada(silver, datos_sinteticos):
    usuarios = datos_sinteticos["bronze_rips"].select(F.explode("usuarios").alias("u"))
    total = sum(
        usuarios.select(F.explode(f"u.servicios.{tipo}")).count()
        for tipo in ["consultas", "procedimientos", "medicamentos"]
    )
    assert total > 0
    assert silver.count() == total


def test_llave_de_servicio_es_unica(silver):
    assert silver.groupBy(*S.LLAVE_SERVICIO).count().where("count > 1").count() == 0


def test_valor_reconocido_nunca_supera_lo_facturado(silver):
    assert silver.where(F.col("valor_reconocido") > F.col("vr_servicio")).count() == 0
    assert silver.where(F.col("glosa_potencial") < 0).count() == 0


def test_reglas_de_servicio_contra_anomalias_sembradas(silver, datos_sinteticos):
    evaluacion = S.evaluar_servicios(silver, datos_sinteticos["anomalias_sembradas"])
    m = S.metricas(evaluacion)
    assert m["vp"] > 0
    assert m["precision"] >= 0.999, m
    assert m["recall"] >= 0.995, m
    # cada tipo de anomalía lo encuentra su propia regla
    for r in S.recall_por_tipo(evaluacion).collect():
        assert r.recall >= 0.99, (r.tipo_anomalia, r.recall)


def test_alertas_prestador_mes_contra_anomalias_sembradas(silver, datos_sinteticos):
    pm = G.prestador_mes(silver).cache()
    ej = G.ejecucion_pgp(pm, datos_sinteticos["pgp_bandas"])
    al = G.alertas(pm, ej, umbral_patron=1.20, meses_historia=6)
    resultado = {r.tipo_alerta: r for r in G.evaluar_alertas(al, datos_sinteticos["anomalias_prestador_mes"]).collect()}
    assert set(resultado) == {"SOBREEJECUCION_PGP", "CAMBIO_PATRON_CONSULTAS"}
    for tipo, r in resultado.items():
        assert r.sembradas > 0, tipo
        assert r.precision == 1.0 and r.recall == 1.0, (tipo, r)


def _huella(carpeta: Path) -> str:
    h = hashlib.sha256()
    for f in sorted(p for p in carpeta.rglob("*") if p.is_file()):
        h.update(str(f.relative_to(carpeta)).encode())
        h.update(f.read_bytes())
    return h.hexdigest()


def test_generador_es_reproducible_con_la_misma_semilla(tmp_path):
    a = generar(tmp_path / "a", meses=2, escala=0.5)
    b = generar(tmp_path / "b", meses=2, escala=0.5)
    c = generar(tmp_path / "c", meses=2, escala=0.5, semilla=1)
    assert _huella(a) == _huella(b)
    assert _huella(a) != _huella(c)
