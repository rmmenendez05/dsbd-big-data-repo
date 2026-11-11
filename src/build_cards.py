"""Genera fichas Markdown desde data/serving/ (nunca desde raw/)."""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

from .core.paths import CARDS, SERVING, ensure_dirs

DB_PATH = SERVING / "observatorio.sqlite"


def _read_table(conn: sqlite3.Connection, name: str) -> pd.DataFrame:
    try:
        return pd.read_sql_query(f"SELECT * FROM {name}", conn)
    except Exception:
        return pd.DataFrame()


def _latest_values(inds: pd.DataFrame) -> pd.DataFrame:
    """Último año con dato de cada indicador del país."""
    con_valor = inds[inds["value"].notna()]
    if con_valor.empty:
        return con_valor
    idx = con_valor.groupby(["source_id", "indicator_id"])["year"].idxmax()
    return con_valor.loc[idx].sort_values(["indicator_id", "source_id"])


def _comparative_lines(c: pd.Series, inds: pd.DataFrame, countries: pd.DataFrame,
                       indicators: pd.DataFrame) -> list[str]:
    """Mediana de la región, mismo indicador y mismo año, frente al valor del país.

    El año de referencia es el último en el que la región tiene datos de ese
    indicador. Si el país no tiene valor en ese año, la ficha lo dice.
    """
    region = c.get("region")
    peers = countries[countries["region"] == region]["iso3"]
    lines = []
    for (source_id, ind_id), own_rows in inds.groupby(["source_id", "indicator_id"]):
        regional = indicators[
            indicators["iso3"].isin(peers)
            & (indicators["indicator_id"] == ind_id)
            & (indicators["source_id"] == source_id)
            & indicators["value"].notna()
        ]
        if regional.empty:
            continue
        year = int(regional["year"].max())
        same_year = regional[regional["year"] == year]["value"].astype(float)
        median_v = same_year.median()
        own = own_rows[(own_rows["year"] == year) & own_rows["value"].notna()]
        label = own_rows.iloc[0]["indicator_label"]
        label = label if pd.notna(label) and label else ind_id
        head = (
            f"- **{label}** ({year}): mediana de **{region}** = {median_v:,.2f} "
            f"({len(same_year)} países)"
        )
        if own.empty:
            lines.append(f"{head}; este país no tiene dato en {year} (sin dato).")
            continue
        own_v = float(own.iloc[0]["value"])
        if own_v > median_v:
            pos = "por encima"
        elif own_v < median_v:
            pos = "por debajo"
        else:
            pos = "igual"
        lines.append(f"{head}; este país: **{own_v:,.2f}** ({pos} de la mediana).")
    return lines


def _card_markdown(iso3: str, countries: pd.DataFrame, languages: pd.DataFrame,
                   indicators: pd.DataFrame) -> str:
    c = countries[countries["iso3"] == iso3].iloc[0]
    langs = languages[languages["iso3"] == iso3] if "iso3" in languages else languages
    inds = indicators[indicators["iso3"] == iso3] if "iso3" in indicators else indicators

    lang_lines = (
        "\n".join(
            f"- `{r.language_code}`: {r.language_name}" for _, r in langs.iterrows()
        )
        or "- (sin idiomas en clean/)"
    )
    latest = _latest_values(inds) if not inds.empty else inds
    ind_lines = (
        "\n".join(
            f"- **{r.indicator_label}** ({r.year}): {r.value} {r.unit or ''} "
            f"_(fuente: {r.source_id})_"
            for _, r in latest.iterrows()
        )
        or "- (sin indicadores; posible huérfano del join)"
    )

    comparative = "_Sin indicadores para comparar._"
    if not inds.empty and "region" in countries.columns:
        lines = _comparative_lines(c, inds, countries, indicators)
        if lines:
            comparative = "\n".join(lines)

    return f"""# Ficha país — {c['name_common']} ({iso3})

- **Nombre oficial:** {c.get('name_official')}
- **ISO2 / ISO3:** {c.get('iso2')} / {iso3}
- **Región / subregión:** {c.get('region')} / {c.get('subregion')}

## Idiomas

{lang_lines}

## Indicadores (último año con dato)

{ind_lines}

## Comparativo (mediana regional, mismo año)

{comparative}

---
Generado automáticamente el {date.today().isoformat()} desde `data/serving/observatorio.sqlite`.
"""


def run_build_cards() -> list[Path]:
    ensure_dirs()
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"No existe {DB_PATH}. Ejecutad antes extract → transform → load."
        )

    conn = sqlite3.connect(DB_PATH)
    try:
        countries = _read_table(conn, "countries")
        languages = _read_table(conn, "languages")
        indicators = _read_table(conn, "indicators")
    finally:
        conn.close()

    if countries.empty:
        raise RuntimeError("Tabla countries vacía: revisad transform.")

    written: list[Path] = []
    for iso3 in countries["iso3"].tolist():
        md = _card_markdown(iso3, countries, languages, indicators)
        path = CARDS / f"{iso3}.md"
        path.write_text(md, encoding="utf-8")
        written.append(path)
        print(f"[cards] {path}")
    return written


if __name__ == "__main__":
    run_build_cards()
