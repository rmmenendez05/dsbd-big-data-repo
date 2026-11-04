from __future__ import annotations
from dataclasses import dataclass
from ..core.config import Config
import pandas as pd
import json
from ..core.paths import RAW

CRITERIA = {
    "all": lambda c: True,
    "independent": lambda c: c.get("independent") is True,
    "one_member": lambda c: c.get("oneMember") is True,
}

@dataclass
class Universe:
    df: pd.DataFrame
    criterion: str

    @property
    def iso3(self) -> set[str]:
        return set(self.df["iso3"])

    @property
    def iso2_by_iso3(self) -> dict[str, str]:
        return dict(zip(self.df["iso3"], self.df["iso2"]))


def build_universe(cfg: Config, all_countries: bool = True) -> Universe:
    # Modo --focus-countries: el universo son los países del YAML
    if not all_countries:
        df = pd.DataFrame([{"iso3": c.iso3, "iso2": c.iso2} for c in cfg.focus_countries])
        return Universe(df=df, criterion="focus")

    criterion = cfg.universe_criterion
    if criterion not in CRITERIA:
        raise ValueError(f"Criterio de universo '{criterion}' no válido. Opciones: {sorted(CRITERIA)}")

    with (RAW / "countries_profile.json").open(encoding="utf-8") as f:
        payload = json.load(f)

    keep = CRITERIA[criterion]
    rows = [{"iso3": c["cca3"], "iso2": c["cca2"]} for c in payload if keep(c)]
    return Universe(df=pd.DataFrame(rows), criterion=criterion)