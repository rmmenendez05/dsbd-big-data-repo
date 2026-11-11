from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from .constants import MAX_REQUESTS_PER_SECOND, MAX_RETRIES, TIMEOUT, USER_AGENT
from .paths import SOURCES_YAML


@dataclass
class HttpConfig:
    """Cortesía con las APIs: identificación, ritmo, reintentos y timeout."""

    user_agent: str = USER_AGENT
    max_requests_per_second: float = MAX_REQUESTS_PER_SECOND
    max_retries: int = MAX_RETRIES
    timeout_seconds: float = TIMEOUT
    backoff_seconds: float = 0.5


@dataclass
class Source:
    id: str
    name: str
    format: str = "json"
    role: str = "indicator"
    transformer: str = "generic_passthrough"
    url: str | None = None
    url_template: str | None = None
    # "worldbank": recorre todas las páginas de la API y contrasta con `total`
    paginate: str | None = None
    # Equivalencias de código de esta fuente → código canónico (iso2 o iso3)
    aliases: dict[str, str] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    def resolve_url(self, iso2: str = "", iso3: str = "") -> str:
        if self.url_template:
            return self.url_template.format(iso2=iso2, iso3=iso3)
        if self.url:
            return self.url
        raise ValueError(f"Fuente {self.id}: falta url o url_template")


@dataclass
class FocusCountry:
    iso2: str
    iso3: str


@dataclass
class Config:
    sources: list[Source]
    focus_countries: list[FocusCountry]
    # Sin criterio declarado, el universo son los focus_countries
    universe_criterion: str = "focus"
    http: HttpConfig = field(default_factory=HttpConfig)


def load_config(path=SOURCES_YAML) -> Config:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    sources = [Source(**s) for s in raw.get("sources", [])]
    focus = [FocusCountry(**c) for c in raw.get("focus_countries", [])]
    criterion = (raw.get("universe") or {}).get("criterion", "focus")
    http = HttpConfig(**(raw.get("http") or {}))
    return Config(
        sources=sources,
        focus_countries=focus,
        universe_criterion=criterion,
        http=http,
    )
