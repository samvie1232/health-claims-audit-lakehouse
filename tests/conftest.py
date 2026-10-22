"""Fixtures compartidos: una SparkSession local y los datos sintéticos cargados como lo hace Bronze."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RAIZ = Path(__file__).resolve().parents[1]
GENERADOR = RAIZ / "scripts" / "generar_rips_sinteticos.py"


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    sesion = (
        SparkSession.builder.master("local[2]")
        .appName("health-audit-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "America/Bogota")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sesion.sparkContext.setLogLevel("ERROR")
    yield sesion
    sesion.stop()


def generar(salida: Path, meses: int = 12, escala: float = 1.0, tasa: float = 0.03, semilla: int = 2275) -> Path:
    cmd = [
        sys.executable,
        str(GENERADOR),
        "--salida",
        str(salida),
        "--meses",
        str(meses),
        "--escala",
        str(escala),
        "--tasa-anomalia",
        str(tasa),
        "--semilla",
        str(semilla),
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)
    return salida


def cargar_bronze(spark: SparkSession, landing: Path) -> dict[str, DataFrame]:
    """Equivalente local de 01_bronze_ingesta (sin Auto Loader): mismas columnas y metadatos."""
    rips = (
        spark.read.option("multiLine", "true")
        .json(str(landing / "raw" / "rips" / "*" / "*.json"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_periodo", F.regexp_extract(F.col("_metadata.file_path"), r"/rips/(\d{6})/", 1))
    )

    def csv(ruta: str) -> DataFrame:
        return spark.read.option("header", "true").option("inferSchema", "true").csv(str(landing / ruta))

    return {
        "bronze_rips": rips,
        "nota_tecnica": csv("ref/nota_tecnica.csv"),
        "pgp_bandas": csv("ref/pgp_bandas.csv"),
        "prestadores": csv("ref/prestadores.csv"),
        "cie10": csv("ref/catalogo_cie10.csv"),
        "anomalias_sembradas": csv("labels/anomalias_sembradas.csv"),
        "anomalias_prestador_mes": csv("labels/anomalias_prestador_mes.csv"),
    }


@pytest.fixture(scope="session")
def datos_sinteticos(spark, tmp_path_factory) -> dict[str, DataFrame]:
    """12 meses a escala 1: suficiente historia para la alerta de patrón y rápido en CI."""
    landing = generar(tmp_path_factory.mktemp("landing"))
    return cargar_bronze(spark, landing)
