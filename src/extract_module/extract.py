"""Extract: descarga intacta → data/raw/ + manifiesto.

No transforma. Añadir una URL nueva = entrada en sources.yaml.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

import requests

from ..core.config import Config, FocusCountry, Source, load_config
from ..core.constants import TIMEOUT, USER_AGENT
from ..core.paths import MANIFEST, RAW, ensure_dirs
from .countries import get_all_countries


def _raw_name(source: Source, iso2: str | None = None) -> str:
    if source.url_template and iso2:
        return f"{source.id}_{iso2}.{source.format}"
    return f"{source.id}.{source.format}"


def _download(url: str, dest: Path) -> tuple[int, int]:
    resp = requests.get(
        url,
        timeout=TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )
    dest.write_bytes(resp.content)
    return resp.status_code, len(resp.content)


def extract_source(source: Source, countries: list[FocusCountry]) -> list[dict]:
    """Descarga una fuente; si tiene plantilla, una petición por país objetivo."""
    rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    targets = countries if source.url_template else [None]

    ctr_count = 0
    for country in targets:
        iso2 = country.iso2 if country else ""
        iso3 = country.iso3 if country else ""
        url = source.resolve_url(iso2=iso2, iso3=iso3)
        filename = _raw_name(source, iso2 or None)
        dest = RAW / filename
        print(f"  {iso2 or '-'}: {url}")
        try:
            status, nbytes = _download(url, dest)
            note = ""
            ctr_count+=1
        except requests.RequestException as exc:
            status, nbytes = 0, 0
            note = str(exc)
        rows.append(
            {
                "timestamp_utc": now,
                "source_id": source.id,
                "url": url,
                "http_status": status,
                "bytes": nbytes,
                "filename": filename,
                "iso2": iso2,
                "iso3": iso3,
                "note": note,
            }
        )
    print("TOTAL COUNTRIES:", ctr_count)
    return rows


def run_extract(cfg: Config | None = None, all_countries: bool = True) -> Path:
    cfg = cfg or load_config()
    ensure_dirs()
    countries = get_all_countries(cfg) if all_countries else cfg.focus_countries
    print(f"[extract] {len(countries)} países objetivo")
    all_rows: list[dict] = []
    for source in cfg.sources:
        print(f"[extract] {source.id} — {source.name}")
        all_rows.extend(extract_source(source, countries))

    fieldnames = [
        "timestamp_utc",
        "source_id",
        "url",
        "http_status",
        "bytes",
        "filename",
        "iso2",
        "iso3",
        "note",
    ]
    with MANIFEST.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"[extract] manifiesto → {MANIFEST}")
    return MANIFEST


if __name__ == "__main__":
    run_extract()
