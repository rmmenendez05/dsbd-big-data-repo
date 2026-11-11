"""Job 01 · Cobertura por (indicator_id, década) en RDD, DataFrame y Spark SQL.

Lee el dataset Parquet de la zona limpia (data/clean/indicators/, particionado
por indicator_id), cuenta solo las filas con value no nulo y calcula para cada
(indicator_id, década):

    n_filas      filas con dato
    n_paises     países distintos (iso3) con dato
    primer_anio  primer año con dato
    ultimo_anio  último año con dato

de tres maneras (RDD estilo MapReduce, DataFrame y Spark SQL) y las contrasta
con la misma agregación sobre data/serving/observatorio.sqlite. Si alguna no
coincide, el proceso termina con código 1.

Salidas:
    data/serving/spark/cobertura_decada/   Parquet de la versión DataFrame
    logs/spark-events/                     event log de la sesión

Uso (desde la raíz del repo, después de `python -m src.pipeline`):

    python spark/job_01.py

Requiere Java 17 y pyspark. En Windows, Hadoop necesita además winutils.exe
y hadoop.dll en %HADOOP_HOME%\\bin.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
INDICATORS = ROOT / "data" / "clean" / "indicators"
SQLITE_DB = ROOT / "data" / "serving" / "observatorio.sqlite"
OUTPUT = ROOT / "data" / "serving" / "spark" / "cobertura_decada"
EVENT_LOG_DIR = ROOT / "logs" / "spark-events"

KEY = ("indicator_id", "decada")
COLUMNS = ("indicator_id", "decada", "n_filas", "n_paises", "primer_anio", "ultimo_anio")


def build_spark() -> SparkSession:
    EVENT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    # Los workers de Python usan el mismo intérprete que lanza el job (p. ej. la .venv)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.master("local[*]")
        .appName("observatorio-job_01-cobertura_decada")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENT_LOG_DIR.as_uri())
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_indicators(spark: SparkSession) -> DataFrame:
    if not INDICATORS.is_dir():
        raise FileNotFoundError(
            f"No existe {INDICATORS}. Ejecutad antes `python -m src.pipeline`."
        )
    df = spark.read.parquet(str(INDICATORS))
    print(f"[job_01] particiones del DataFrame leído: {df.rdd.getNumPartitions()}")
    return df


def with_decade(df: DataFrame) -> DataFrame:
    """Solo filas con valor (un nulo no es una observación) y la década del año."""
    return (
        df.where(F.col("value").isNotNull() & F.col("year").isNotNull())
        .withColumn("year", F.col("year").cast("int"))
        .withColumn("decada", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .select("indicator_id", "decada", "iso3", "year")
    )


# ---------------------------------------------------------------- 1. RDD (MapReduce)


def coverage_rdd(base: DataFrame) -> dict[tuple, tuple]:
    """map → pares ((indicator_id, década), parcial); reduceByKey combina parciales."""

    def to_pair(row):
        return (row.indicator_id, row.decada), (1, {row.iso3}, row.year, row.year)

    def combine(a, b):
        return (a[0] + b[0], a[1] | b[1], min(a[2], b[2]), max(a[3], b[3]))

    reduced = (
        base.rdd.map(to_pair)
        .reduceByKey(combine)
        .mapValues(lambda v: (v[0], len(v[1]), v[2], v[3]))
    )
    return {k: tuple(v) for k, v in reduced.collect()}


# ------------------------------------------------------------------ 2. DataFrame


def coverage_dataframe(base: DataFrame) -> DataFrame:
    return base.groupBy(*KEY).agg(
        F.count(F.lit(1)).alias("n_filas"),
        F.countDistinct("iso3").alias("n_paises"),
        F.min("year").alias("primer_anio"),
        F.max("year").alias("ultimo_anio"),
    )


# ------------------------------------------------------------------ 3. Spark SQL

COVERAGE_SQL = """
SELECT indicator_id,
       CAST(FLOOR(CAST(year AS INT) / 10) * 10 AS INT) AS decada,
       COUNT(*)                                        AS n_filas,
       COUNT(DISTINCT iso3)                            AS n_paises,
       MIN(CAST(year AS INT))                          AS primer_anio,
       MAX(CAST(year AS INT))                          AS ultimo_anio
FROM indicators
WHERE value IS NOT NULL AND year IS NOT NULL
GROUP BY indicator_id, CAST(FLOOR(CAST(year AS INT) / 10) * 10 AS INT)
"""


def coverage_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("indicators")
    return spark.sql(COVERAGE_SQL)


# ------------------------------------------------------------- 4. SQLite (serving)

COVERAGE_SQLITE = """
SELECT indicator_id,
       (CAST(year AS INTEGER) / 10) * 10 AS decada,
       COUNT(*)                          AS n_filas,
       COUNT(DISTINCT iso3)              AS n_paises,
       MIN(CAST(year AS INTEGER))        AS primer_anio,
       MAX(CAST(year AS INTEGER))        AS ultimo_anio
FROM indicators
WHERE value IS NOT NULL AND year IS NOT NULL
GROUP BY indicator_id, decada
"""


def coverage_sqlite() -> dict[tuple, tuple]:
    if not SQLITE_DB.is_file():
        raise FileNotFoundError(f"No existe {SQLITE_DB}. Ejecutad antes `python -m src.pipeline`.")
    with sqlite3.connect(SQLITE_DB) as conn:
        rows = conn.execute(COVERAGE_SQLITE).fetchall()
    return {(r[0], int(r[1])): tuple(int(x) for x in r[2:]) for r in rows}


# ------------------------------------------------------------------- comparación


def to_dict(df: DataFrame) -> dict[tuple, tuple]:
    return {
        (r.indicator_id, int(r.decada)): (int(r.n_filas), int(r.n_paises),
                                          int(r.primer_anio), int(r.ultimo_anio))
        for r in df.collect()
    }


def compare(results: dict[str, dict[tuple, tuple]]) -> list[str]:
    """Devuelve las diferencias frente a la versión DataFrame (vacía si todo coincide)."""
    reference_name = "dataframe"
    reference = results[reference_name]
    problems = []
    for name, other in results.items():
        if name == reference_name:
            continue
        if other == reference:
            continue
        for key in sorted(set(reference) | set(other)):
            if reference.get(key) != other.get(key):
                problems.append(
                    f"{name} != {reference_name} en {key}: {other.get(key)} frente a {reference.get(key)}"
                )
    return problems


def main() -> int:
    spark = build_spark()
    try:
        df = read_indicators(spark)
        base = with_decade(df)

        by_df = coverage_dataframe(base)
        print("[job_01] plan físico de la versión DataFrame:")
        by_df.explain(mode="formatted")

        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        by_df.orderBy(*KEY).write.mode("overwrite").parquet(str(OUTPUT))
        print(f"[job_01] cobertura_decada → {OUTPUT}")

        results = {
            "dataframe": to_dict(by_df),
            "rdd": coverage_rdd(base),
            "spark_sql": to_dict(coverage_sql(spark, df)),
            "sqlite": coverage_sqlite(),
        }
        for name, res in results.items():
            print(f"[job_01] {name}: {len(res)} grupos (indicator_id, década)")

        by_df.orderBy(*KEY).show(50, truncate=False)

        problems = compare(results)
        if problems:
            print("[job_01] ERROR: las versiones no coinciden:", file=sys.stderr)
            for p in problems[:50]:
                print("   ", p, file=sys.stderr)
            return 1
        print("[job_01] RDD, DataFrame, Spark SQL y SQLite coinciden.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
