import requests

from ..core.config import Config, FocusCountry
from ..core.constants import TIMEOUT, USER_AGENT


def get_all_countries(cfg: Config) -> list[FocusCountry]:
    """Descarga mledoze/countries y devuelve todos los países (iso2, iso3)."""
    source = next(s for s in cfg.sources if s.id == "countries_profile")
    resp = requests.get(
        source.resolve_url(),
        timeout=TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )
    resp.raise_for_status()
    return [FocusCountry(iso2=c["cca2"], iso3=c["cca3"]) for c in resp.json()]
