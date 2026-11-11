"""Universo de países: qué entidades del perfil cuentan como país.

El criterio se declara en sources.yaml (universe.criterion). Lo que no pasa
el criterio no desaparece: vuelve como excluded (source_id, source_code,
name, reason) para escribirse en data/clean/excluded_entities.csv.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ..core.config import Config

EXCLUDED_COLUMNS = ["source_id", "source_code", "name", "reason"]

CRITERIA = {
    "all": lambda c: True,
    "independent": lambda c: c.get("independent") is True,
    "un_member": lambda c: c.get("unMember") is True,
    # Nombre antiguo del criterio anterior; se mantiene por compatibilidad
    "one_member": lambda c: c.get("unMember") is True,
}


@dataclass
class Universe:
    df: pd.DataFrame  # columnas iso3, iso2
    criterion: str
    excluded: pd.DataFrame

    @property
    def iso3(self) -> set[str]:
        return set(self.df["iso3"])

    @property
    def iso2_by_iso3(self) -> dict[str, str]:
        return dict(zip(self.df["iso3"], self.df["iso2"]))

    @property
    def iso3_by_iso2(self) -> dict[str, str]:
        return dict(zip(self.df["iso2"], self.df["iso3"]))


def profile_source_id(cfg: Config) -> str | None:
    for s in cfg.sources:
        if s.transformer == "countries_profile" or s.role == "profile":
            return s.id
    return None


def _focus_universe(cfg: Config, payload: list | None, source_id: str | None) -> Universe:
    df = pd.DataFrame(
        [{"iso3": c.iso3, "iso2": c.iso2} for c in cfg.focus_countries],
        columns=["iso3", "iso2"],
    )
    wanted = set(df["iso3"])
    excluded = [
        {
            "source_id": source_id,
            "source_code": c.get("cca3"),
            "name": (c.get("name") or {}).get("common"),
            "reason": "fuera de focus_countries (criterio focus)",
        }
        for c in (payload or [])
        if c.get("cca3") not in wanted
    ]
    return Universe(df=df, criterion="focus", excluded=pd.DataFrame(excluded, columns=EXCLUDED_COLUMNS))


def build_universe(cfg: Config, raw_dir: Path, all_countries: bool = True) -> Universe:
    """Calcula el universo leyendo el perfil ya descargado en raw_dir.

    raw_dir se pasa explícito para que transform y extract decidan de dónde leen.
    """
    source_id = profile_source_id(cfg)
    path = raw_dir / f"{source_id}.json" if source_id else None
    payload = None
    if path is not None and path.is_file():
        with path.open(encoding="utf-8") as f:
            payload = json.load(f)

    criterion = cfg.universe_criterion if all_countries else "focus"
    if criterion == "focus":
        return _focus_universe(cfg, payload, source_id)

    if criterion not in CRITERIA:
        raise ValueError(
            f"Criterio de universo '{criterion}' no válido. "
            f"Opciones: {sorted([*CRITERIA, 'focus'])}"
        )
    if payload is None:
        raise FileNotFoundError(
            f"El criterio '{criterion}' necesita el perfil en {path}. "
            "Ejecutad antes el extract."
        )

    keep = CRITERIA[criterion]
    rows, excluded = [], []
    for c in payload:
        if keep(c):
            rows.append({"iso3": c["cca3"], "iso2": c["cca2"]})
        else:
            excluded.append(
                {
                    "source_id": source_id,
                    "source_code": c.get("cca3"),
                    "name": (c.get("name") or {}).get("common"),
                    "reason": f"no cumple el criterio '{criterion}'",
                }
            )
    return Universe(
        df=pd.DataFrame(rows, columns=["iso3", "iso2"]),
        criterion=criterion,
        excluded=pd.DataFrame(excluded, columns=EXCLUDED_COLUMNS),
    )
