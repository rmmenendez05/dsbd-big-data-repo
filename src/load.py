"""Load: data/clean/ → data/serving/observatorio.sqlite (+ copia opcional en Parquet).

El SQLite se regenera desde cero en cada ejecución leyendo la zona limpia.
Las tablas limpias se leen en Parquet (countries.parquet, languages.parquet y
el dataset Hive indicators/). El CSV solo se usa si no hay Parquet, y es el
formato de join_orphans y excluded_entities.
Load no recalcula el criterio de país: copia el limpio a serving.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from .core.paths import CLEAN, SERVING, ensure_dirs

DB_PATH = SERVING / "observatorio.sqlite"
TABLES = ("countries", "languages", "indicators", "join_orphans", "excluded_entities")


def _read_clean(name: str) -> tuple[pd.DataFrame | None, str]:
    """Lee una tabla de clean/: Parquet (fichero o dataset Hive) y, si no hay, CSV."""
    for parquet in (CLEAN / f"{name}.parquet", CLEAN / name):
        if parquet.exists():
            df = pd.read_parquet(parquet)
            # La columna de partición Hive vuelve como categoría: se deja como texto
            for col in df.columns:
                if isinstance(df[col].dtype, pd.CategoricalDtype):
                    df[col] = df[col].astype(str)
            return df, parquet.name
    csv_path = CLEAN / f"{name}.csv"
    if csv_path.exists():
        return pd.read_csv(csv_path), csv_path.name
    return None, ""


def run_load(write_parquet: bool = False) -> Path:
    ensure_dirs()
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    try:
        for name in TABLES:
            df, origin = _read_clean(name)
            if df is None:
                print(f"[load] omitido (no está en clean/): {name}")
                continue
            df.to_sql(name, conn, index=False, if_exists="replace")
            print(f"[load] {name} ({origin}): {len(df)} filas → sqlite")
            if write_parquet:
                pq = SERVING / f"{name}.parquet"
                try:
                    df.to_parquet(pq, index=False)
                    print(f"[load] {name} → {pq}")
                except ImportError:
                    print(
                        "[load] Parquet omitido: instalad pyarrow "
                        "(pip install pyarrow)"
                    )
                    write_parquet = False
    finally:
        conn.close()

    print(f"[load] base → {DB_PATH}")
    return DB_PATH


if __name__ == "__main__":
    run_load()
