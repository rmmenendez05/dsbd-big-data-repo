"""Transform: data/raw/ → data/clean/ (tablas normalizadas).

  - universo de países: criterio explícito (universe.criterion en sources.yaml);
    countries solo con lo aceptado y lo descartado en excluded_entities.csv
    (source_id, source_code, name, reason). Nada se descarta en silencio.
  - clave canónica iso3; las equivalencias de código de cada fuente se
    declaran en sources.yaml (aliases), no en ifs del transform.
  - perfil aplanado → countries; explosión de idiomas → languages
  - indicadores en esquema común (iso3, indicator_id, indicator_label, year,
    value, unit, source_id), year entero y value numérico; sin dato = nulo.
  - clave única (iso3, indicator_id, year, source_id): un duplicado es error.
  - join_orphans.csv por país, fuente e indicador.
  - zona limpia en Parquet: countries.parquet, languages.parquet y
    indicators/ particionado estilo Hive (indicator_id=<id>/).

Para una URL nueva: implementad una función y registradla en TRANSFORMERS
con el mismo nombre que `transformer:` en sources.yaml.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Callable

import pandas as pd

from .core.config import Config, Source, load_config
from .core.paths import CLEAN, RAW, ensure_dirs
from .transform_module.universe import EXCLUDED_COLUMNS, Universe, build_universe

Transformer = Callable[[Source, Config], dict[str, pd.DataFrame]]

INDICATOR_COLUMNS = [
    "iso3",
    "iso2",
    "indicator_id",
    "indicator_label",
    "year",
    "value",
    "unit",
    "source_id",
]
INDICATOR_KEY = ["iso3", "indicator_id", "year", "source_id"]


def _read_json(path: Path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _universe(cfg: Config) -> Universe:
    return build_universe(cfg, RAW)


def _excluded(source: Source, code, name, reason: str) -> dict:
    return {"source_id": source.id, "source_code": code, "name": name, "reason": reason}


def _excluded_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=EXCLUDED_COLUMNS).drop_duplicates(
        subset=["source_id", "source_code"]
    )


def _indicator_row(source: Source, iso3: str, iso2, year, value, label=None) -> dict:
    meta = source.meta or {}
    return {
        "iso3": iso3,
        "iso2": iso2,
        "indicator_id": meta.get("indicator_id", source.id),
        "indicator_label": meta.get("indicator_label", label),
        "year": year,
        "value": value,
        "unit": meta.get("unit"),
        "source_id": source.id,
    }


def _typed_indicators(rows: list[dict]) -> pd.DataFrame:
    """Esquema común tipado: year entero (Int64), value numérico, nulos como nulos."""
    df = pd.DataFrame(rows, columns=INDICATOR_COLUMNS)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df[df["year"].notna()].reset_index(drop=True)


# --------------------------------------------------------------------------- perfil


def transform_countries_profile(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    payload = _read_json(RAW / f"{source.id}.json")
    universe = _universe(cfg)
    accepted = universe.iso3

    countries_rows = []
    language_rows = []
    for item in payload:
        iso3 = item.get("cca3")
        if iso3 not in accepted:
            continue  # queda anotado en universe.excluded
        name = item.get("name") or {}
        countries_rows.append(
            {
                "iso3": iso3,
                "iso2": item.get("cca2"),
                "name_common": name.get("common"),
                "name_official": name.get("official"),
                "region": item.get("region"),
                "subregion": item.get("subregion"),
            }
        )
        for code, lang_name in (item.get("languages") or {}).items():
            language_rows.append(
                {"iso3": iso3, "language_code": code, "language_name": lang_name}
            )

    return {
        "countries": pd.DataFrame(countries_rows),
        "languages": pd.DataFrame(
            language_rows, columns=["iso3", "language_code", "language_name"]
        ),
        "excluded_entities": universe.excluded,
    }


# --------------------------------------------------------------------- Banco Mundial


def transform_worldbank_indicator(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    universe = _universe(cfg)
    iso2_by_iso3 = universe.iso2_by_iso3
    rows: list[dict] = []
    excluded: list[dict] = []

    def records(payload):
        # World Bank: [meta, [records...]]
        if not isinstance(payload, list) or len(payload) < 2 or not payload[1]:
            return []
        return [rec for rec in payload[1] if rec is not None]

    pages = sorted(RAW.glob(f"{source.id}_page*.json"))
    if pages:
        # Descarga de universo (country/all, paginada)
        for path in pages:
            for rec in records(_read_json(path)):
                country = rec.get("country") or {}
                code = rec.get("countryiso3code") or country.get("id")
                iso3 = source.aliases.get(code, code)
                if iso3 not in iso2_by_iso3:
                    excluded.append(_excluded(source, code, country.get("value"),
                                              "no es país del universo (agregado o fuera del criterio)"))
                    continue
                rows.append(_indicator_row(source, iso3, iso2_by_iso3[iso3], rec.get("date"),
                                           rec.get("value"), (rec.get("indicator") or {}).get("value")))
    else:
        # Descarga por país (url_template con {iso2})
        for iso3, iso2 in iso2_by_iso3.items():
            path = RAW / f"{source.id}_{iso2}.json"
            if not path.exists():
                continue
            for rec in records(_read_json(path)):
                rows.append(_indicator_row(source, iso3, iso2, rec.get("date"), rec.get("value"),
                                           (rec.get("indicator") or {}).get("value")))

    return {"indicators": _typed_indicators(rows), "excluded_entities": _excluded_df(excluded)}


# ------------------------------------------------------------------------------ OWID


def transform_owid_life_expectancy(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    df = pd.read_csv(RAW / f"{source.id}.csv")
    meta = source.meta or {}
    universe = _universe(cfg)
    iso2_by_iso3 = universe.iso2_by_iso3

    # Columnas OWID: Entity, Code, Year, <valor>
    code_col = "Code" if "Code" in df.columns else "code"
    year_col = "Year" if "Year" in df.columns else "year"
    entity_col = "Entity" if "Entity" in df.columns else code_col
    value_col = meta.get("value_column") or (
        "Life expectancy" if "Life expectancy" in df.columns else df.columns[-1]
    )

    codes = df[code_col].fillna("").astype(str)
    iso3 = codes.map(lambda c: source.aliases.get(c, c))
    in_universe = iso3.isin(iso2_by_iso3.keys())

    excluded = [
        _excluded(source, code or None, entity,
                  "entidad sin código ISO3 (agregado OWID)" if not code
                  else "no es país del universo (agregado o fuera del criterio)")
        for code, entity in df.loc[~in_universe, [code_col, entity_col]]
        .fillna("").drop_duplicates().itertuples(index=False)
    ]

    sub = df[in_universe].assign(_iso3=iso3[in_universe])
    # Por defecto la serie va completa. Un recorte a un año solo se aplica si
    # la fuente lo declara expresamente en meta (compatibilidad con la P1).
    only_year = meta["year"] if "year" in meta else None
    if only_year is not None:
        sub = sub[sub[year_col].astype(str) == str(only_year)]

    rows = [
        _indicator_row(source, r["_iso3"], iso2_by_iso3.get(r["_iso3"]), r[year_col], r[value_col], value_col)
        for _, r in sub.iterrows()
    ]
    for row in rows:
        row["indicator_id"] = meta.get("indicator_id", "life_expectancy")
        row["unit"] = meta.get("unit", "years")
    return {"indicators": _typed_indicators(rows), "excluded_entities": _excluded_df(excluded)}


# -------------------------------------------------------------------------- Eurostat


def _jsonstat_cells(payload: dict):
    """Recorre un JSON-stat de Eurostat: devuelve ({dimensión: código}, valor)."""
    ids = payload["id"]
    sizes = payload["size"]
    dims = payload["dimension"]
    pos_to_code = {}
    for d in ids:
        index = dims[d]["category"]["index"]
        if isinstance(index, list):
            index = {code: i for i, code in enumerate(index)}
        pos_to_code[d] = {pos: code for code, pos in index.items()}
    strides = []
    acc = 1
    for size in reversed(sizes):
        strides.insert(0, acc)
        acc *= size
    for flat, value in (payload.get("value") or {}).items():
        rest = int(flat)
        coords = {}
        for d, stride in zip(ids, strides):
            coords[d] = pos_to_code[d][rest // stride]
            rest %= stride
        yield coords, value


def transform_eurostat_demo_pjan(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    meta = source.meta or {}
    universe = _universe(cfg)
    iso3_by_iso2 = universe.iso3_by_iso2
    rows: list[dict] = []
    excluded: list[dict] = []

    universe_path = RAW / f"{source.id}.json"
    if universe_path.is_file():
        # Una sola petición con todas las entidades y toda la serie
        payload = _read_json(universe_path)
        geo_labels = payload["dimension"]["geo"]["category"].get("label", {})
        for coords, value in _jsonstat_cells(payload):
            geo = coords["geo"]
            iso2 = source.aliases.get(geo, geo)
            iso3 = iso3_by_iso2.get(iso2)
            if iso3 is None:
                excluded.append(_excluded(source, geo, geo_labels.get(geo),
                                          "no es país del universo (agregado o fuera del criterio)"))
                continue
            rows.append(_indicator_row(source, iso3, iso2, coords.get("time"), value,
                                       payload.get("label")))
    else:
        # Descarga por país con una celda filtrada (formato de la P1)
        for iso2, iso3 in iso3_by_iso2.items():
            path = RAW / f"{source.id}_{iso2}.json"
            if not path.exists():
                continue
            payload = _read_json(path)
            values = payload.get("value") or {}
            if not values:
                continue  # sin dato: quedará como huérfano del join
            rows.append(_indicator_row(source, iso3, iso2, meta.get("time"),
                                       next(iter(values.values())), payload.get("label")))

    for row in rows:
        row["indicator_id"] = meta.get("indicator_id", "demo_pjan")
        row["unit"] = meta.get("unit", "persons")
    return {"indicators": _typed_indicators(rows), "excluded_entities": _excluded_df(excluded)}


# --------------------------------------------------------------------------- genérico


def transform_generic_passthrough(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    """Plantilla: lee raw y deja un DataFrame mínimo para que lo adaptéis."""
    paths = sorted(RAW.glob(f"{source.id}*"))
    rows = []
    for path in paths:
        rows.append(
            {
                "source_id": source.id,
                "filename": path.name,
                "bytes": path.stat().st_size,
                "note": "Reemplazad este transformer por uno específico",
            }
        )
    return {"custom_raw_index": pd.DataFrame(rows)}


TRANSFORMERS: dict[str, Transformer] = {
    "countries_profile": transform_countries_profile,
    "worldbank_indicator": transform_worldbank_indicator,
    "eurostat_demo_pjan": transform_eurostat_demo_pjan,
    "owid_life_expectancy": transform_owid_life_expectancy,
    "generic_passthrough": transform_generic_passthrough,
}

INDICATOR_TRANSFORMERS = {
    "worldbank_indicator",
    "eurostat_demo_pjan",
    "owid_life_expectancy",
}


# -------------------------------------------------------------------- comprobaciones


def _check_unique_key(indicators: pd.DataFrame) -> None:
    """Duplicado de (iso3, indicator_id, year, source_id) = error, no aviso."""
    if indicators.empty:
        return
    dup = indicators[indicators.duplicated(subset=INDICATOR_KEY, keep=False)]
    if not dup.empty:
        raise ValueError(
            f"indicators tiene {len(dup)} filas con clave duplicada "
            f"{INDICATOR_KEY}. Ejemplo:\n{dup.head().to_string(index=False)}"
        )


def _check_unique_countries(countries: pd.DataFrame) -> None:
    if "iso3" in countries and countries["iso3"].duplicated().any():
        raise ValueError("countries tiene iso3 duplicados: iso3 tiene que ser clave única")


def _join_orphans(cfg: Config, countries: pd.DataFrame, indicators: pd.DataFrame) -> pd.DataFrame:
    """Huérfanos por país, fuente e indicador: el país no tiene ningún valor."""
    pairs = {
        (s.id, (s.meta or {}).get("indicator_id", s.id))
        for s in cfg.sources
        if s.transformer in INDICATOR_TRANSFORMERS
    }
    have = set()
    if not indicators.empty:
        con_valor = indicators[indicators["value"].notna()]
        pairs |= set(zip(indicators["source_id"], indicators["indicator_id"]))
        have = set(zip(con_valor["iso3"], con_valor["source_id"], con_valor["indicator_id"]))
    rows = [
        {"iso3": iso3, "source_id": source_id, "indicator_id": indicator_id,
         "reason": "sin ningún valor de este indicador en la fuente"}
        for iso3 in sorted(countries["iso3"].dropna().unique())
        for source_id, indicator_id in sorted(pairs)
        if (iso3, source_id, indicator_id) not in have
    ]
    return pd.DataFrame(rows, columns=["iso3", "source_id", "indicator_id", "reason"])


def _write_clean_parquet(merged: dict[str, pd.DataFrame]) -> None:
    """Zona limpia en Parquet (siempre, no bajo un flag)."""
    countries = merged.get("countries")
    if countries is not None and len(countries.columns):
        countries.to_parquet(CLEAN / "countries.parquet", index=False)
        print(f"[transform] countries → {CLEAN / 'countries.parquet'}")
    languages = merged.get("languages")
    if languages is not None and len(languages.columns):
        languages.to_parquet(CLEAN / "languages.parquet", index=False)
        print(f"[transform] languages → {CLEAN / 'languages.parquet'}")

    # Dataset Hive: data/clean/indicators/indicator_id=<id>/*.parquet
    dataset = CLEAN / "indicators"
    if dataset.exists():
        shutil.rmtree(dataset)  # si no, pyarrow añadiría ficheros a los de la corrida anterior
    indicators = merged.get("indicators")
    if indicators is not None and not indicators.empty:
        indicators.to_parquet(dataset, partition_cols=["indicator_id"], index=False)
        print(f"[transform] indicators → {dataset}/indicator_id=…/")


def run_transform(cfg: Config | None = None) -> dict[str, pd.DataFrame]:
    cfg = cfg or load_config()
    ensure_dirs()
    buckets: dict[str, list[pd.DataFrame]] = {}

    for source in cfg.sources:
        name = source.transformer
        if name not in TRANSFORMERS:
            raise KeyError(
                f"Transformer '{name}' no registrado. "
                f"Disponibles: {sorted(TRANSFORMERS)}"
            )
        print(f"[transform] {source.id} → {name}")
        out = TRANSFORMERS[name](source, cfg)
        for table, df in out.items():
            buckets.setdefault(table, []).append(df)

    merged: dict[str, pd.DataFrame] = {}
    for table, parts in buckets.items():
        parts = [p for p in parts if not p.empty] or parts[:1]
        merged[table] = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    if "excluded_entities" in merged:
        merged["excluded_entities"] = _excluded_df(merged["excluded_entities"].to_dict("records"))
    if "indicators" in merged:
        _check_unique_key(merged["indicators"])
    if "countries" in merged:
        _check_unique_countries(merged["countries"])

    for table, df in merged.items():
        dest = CLEAN / f"{table}.csv"
        df.to_csv(dest, index=False)
        print(f"[transform] {table}: {len(df)} filas → {dest}")

    if "countries" in merged and "iso3" in merged["countries"]:
        indicators = merged.get("indicators", pd.DataFrame(columns=INDICATOR_COLUMNS))
        report = _join_orphans(cfg, merged["countries"], indicators)
        report_path = CLEAN / "join_orphans.csv"
        report.to_csv(report_path, index=False)
        merged["join_orphans"] = report
        print(f"[transform] huérfanos del join: {len(report)} → {report_path}")

    _write_clean_parquet(merged)
    return merged


if __name__ == "__main__":
    run_transform()
