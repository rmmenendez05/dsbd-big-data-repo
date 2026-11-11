from pathlib import Path

from ..core.config import Config, FocusCountry
from ..transform_module.universe import build_universe


def get_all_countries(cfg: Config, raw_dir: Path) -> list[FocusCountry]:
    """Países del universo, a partir del perfil ya descargado en raw_dir.

    Sin perfil descargado (o sin fuente de perfil) se usan los focus_countries.
    """
    try:
        universe = build_universe(cfg, raw_dir)
    except FileNotFoundError:
        return list(cfg.focus_countries)
    return [
        FocusCountry(iso2=r.iso2, iso3=r.iso3)
        for r in universe.df.itertuples(index=False)
    ]
