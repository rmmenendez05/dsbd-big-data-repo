"""Extract: descarga intacta → data/raw/ + manifiesto acumulativo.

No transforma. Añadir una URL nueva = entrada en sources.yaml.
El código vive aquí porque los tests del Hito 1 leen src/extract.py;
src/extract_module/extract.py lo reexporta.

Reglas de la descarga:
  - Cortesía: User-Agent del equipo y como mucho max_requests_per_second
    peticiones por segundo (time.sleep entre peticiones).
  - Reintentos con espera creciente (backoff) ante 429, 5xx y errores de red.
    Un 404 es definitivo: no se reintenta.
  - Solo una respuesta HTTP 200 se guarda en data/raw/; el resto se anota
    en el manifiesto.
  - Paginación (paginate: worldbank): se piden todas las páginas y la suma
    de registros tiene que coincidir con el campo total de la API.
  - Idempotencia: si el artefacto de raw ya es válido no se vuelve a pedir,
    salvo con --force.
  - El manifiesto es acumulativo: cada ejecución añade filas con su run_id.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

from .core.config import Config, FocusCountry, HttpConfig, Source, load_config
from .core.paths import MANIFEST, RAW, ensure_dirs

RETRY_STATUS = {429, 500, 502, 503, 504}
# 404 (y el resto de 4xx) son definitivos: no se reintentan.
NO_RETRY_STATUS = {400, 401, 403, 404}

MANIFEST_FIELDS = [
    "run_id",
    "timestamp_utc",
    "source_id",
    "url",
    "http_status",
    "bytes",
    "filename",
    "iso2",
    "iso3",
    "attempts",
    "note",
]

_last_request_at = 0.0


def _throttle(http: HttpConfig) -> None:
    """Respeta el ritmo máximo de peticiones por segundo."""
    global _last_request_at
    min_interval = 1.0 / http.max_requests_per_second if http.max_requests_per_second else 0.0
    wait = _last_request_at + min_interval - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _last_request_at = time.monotonic()


def _body_ok(content: bytes, fmt: str | None) -> bool:
    """Un 200 solo es dato si el cuerpo se puede leer en el formato declarado.

    Algunas APIs (Eurostat) responden 200 con una página HTML de error
    («Server temporarily unavailable»): eso no es un dato.
    """
    if not content:
        return False
    head = content[:200].lstrip().lower()
    if fmt in ("json", "csv") and (head.startswith(b"<!doctype") or head.startswith(b"<html")):
        return False
    if fmt == "json":
        try:
            json.loads(content)
        except (ValueError, UnicodeDecodeError):
            return False
    return True


def _get(url: str, http: HttpConfig, fmt: str | None = None) -> tuple[int, bytes | None, int, str]:
    """GET con reintentos. Devuelve (status, contenido si 200 válido, intentos, nota).

    Con fmt, un 200 cuyo cuerpo no es de ese formato se trata como error
    transitorio: se reintenta y, si sigue igual, no se devuelve como dato.
    """
    headers = {"User-Agent": http.user_agent}
    attempt = 0
    while True:
        attempt += 1
        _throttle(http)
        try:
            resp = requests.get(url, timeout=http.timeout_seconds, headers=headers)
        except requests.RequestException as exc:
            if attempt > http.max_retries:
                raise
            note = f"error de red: {exc}"
        else:
            status = resp.status_code
            if status == 200 and (fmt is None or _body_ok(resp.content, fmt)):
                return status, resp.content, attempt, ""
            if status == 200:
                if attempt > http.max_retries:
                    return status, None, attempt, (
                        f"HTTP 200 pero el cuerpo no es {fmt} válido tras {attempt} intentos "
                        "(p. ej. página HTML de error); no se guarda"
                    )
                note = f"HTTP 200 con cuerpo que no es {fmt}"
                print(f"    {note}")
            elif status == 404 or status in NO_RETRY_STATUS or status not in RETRY_STATUS:
                return status, None, attempt, f"HTTP {status} definitivo, no se reintenta"
            elif attempt > http.max_retries:
                return status, None, attempt, f"HTTP {status} tras {attempt} intentos"
            else:
                note = f"HTTP {status}"
        delay = http.backoff_seconds * (2 ** (attempt - 1))
        print(f"    reintento {attempt}/{http.max_retries} en {delay:.1f}s ({note})")
        time.sleep(delay)


def _download(url: str, dest: Path, http: HttpConfig | None = None) -> tuple[int, int]:
    """Descarga url en dest solo si la respuesta es 200. Devuelve (status, bytes)."""
    status, content, _attempts, _note = _get(url, http or HttpConfig())
    if status != 200 or content is None:
        return status, 0
    dest.write_bytes(content)
    return status, len(content)


def _raw_name(source: Source, iso2: str | None = None) -> str:
    if source.url_template and iso2:
        return f"{source.id}_{iso2}.{source.format}"
    return f"{source.id}.{source.format}"


def _page_name(source: Source, page: int) -> str:
    return f"{source.id}_page{page:03d}.{source.format}"


def _with_page(url: str, page: int) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query))
    query["page"] = str(page)
    return urlunsplit(parts._replace(query=urlencode(query)))


def _is_valid(path: Path, fmt: str) -> bool:
    """Un artefacto de raw es válido si existe, no está vacío y se puede leer."""
    if not path.is_file() or path.stat().st_size == 0:
        return False
    return _body_ok(path.read_bytes(), fmt)


def _drop_invalid(dest: Path, fmt: str) -> None:
    """Borra un artefacto inválido de raw (p. ej. un HTML de error de otra ejecución)."""
    if dest.is_file() and not _is_valid(dest, fmt):
        print(f"    se borra {dest.name}: no era un {fmt} válido")
        dest.unlink()


def _wb_page_meta(content: bytes) -> tuple[dict, list]:
    """Valida una página del Banco Mundial: [meta, registros]."""
    payload = json.loads(content)
    if (
        not isinstance(payload, list)
        or len(payload) < 2
        or not isinstance(payload[0], dict)
        or "pages" not in payload[0]
    ):
        raise ValueError(f"respuesta del Banco Mundial sin paginación: {payload!r:.200}")
    return payload[0], payload[1] or []


def _row(run_id: str, source: Source, url: str, **kw) -> dict:
    row = {k: "" for k in MANIFEST_FIELDS}
    row.update(
        run_id=run_id,
        timestamp_utc=datetime.now(timezone.utc).isoformat(),
        source_id=source.id,
        url=url,
    )
    row.update(kw)
    return row


class Manifest:
    """Manifiesto acumulativo: añade filas (modo append), nunca borra el historial."""

    def __init__(self, path: Path):
        self.path = path
        self._upgrade_header()

    def _upgrade_header(self) -> None:
        """Si hay un manifiesto antiguo con otras columnas, lo migra conservando sus filas."""
        if not self.path.is_file() or self.path.stat().st_size == 0:
            return
        with self.path.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames == MANIFEST_FIELDS:
                return
            old_rows = list(reader)
        with self.path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(old_rows)

    def append(self, row: dict) -> None:
        new_file = not self.path.is_file() or self.path.stat().st_size == 0
        with self.path.open("a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
            if new_file:
                writer.writeheader()
            writer.writerow(row)


def _extract_one(
    source: Source,
    url: str,
    dest: Path,
    http: HttpConfig,
    run_id: str,
    force: bool,
    iso2: str = "",
    iso3: str = "",
) -> dict:
    """Una URL → un fichero de raw (o una nota en el manifiesto)."""
    base = dict(filename=dest.name, iso2=iso2, iso3=iso3)
    if not force and _is_valid(dest, source.format):
        print(f"  {iso2 or '-'}: ya descargado, se omite ({dest.name})")
        return _row(run_id, source, url, **base, bytes=dest.stat().st_size,
                    note="omitido: artefacto ya válido en raw (usad --force para repetir)")
    print(f"  {iso2 or '-'}: {url}")
    try:
        status, content, attempts, note = _get(url, http, source.format)
    except requests.RequestException as exc:
        _drop_invalid(dest, source.format)
        return _row(run_id, source, url, **base, http_status=0, bytes=0,
                    attempts=http.max_retries + 1, note=str(exc))
    if status == 200 and content is not None:
        dest.write_bytes(content)
        return _row(run_id, source, url, **base, http_status=status,
                    bytes=len(content), attempts=attempts, note=note)
    # Lo que no es un 200 válido no se guarda como dato
    _drop_invalid(dest, source.format)
    return _row(run_id, source, url, **base, http_status=status, bytes=0,
                attempts=attempts, note=note)


def _extract_paginated(
    source: Source, http: HttpConfig, run_id: str, force: bool, manifest: Manifest
) -> None:
    """Banco Mundial: todas las páginas y contraste con el total de la API."""
    url = source.resolve_url()
    first = RAW / _page_name(source, 1)

    if not force and _is_valid(first, source.format):
        try:
            meta, _ = _wb_page_meta(first.read_bytes())
            pages = [RAW / _page_name(source, p) for p in range(1, int(meta["pages"]) + 1)]
        except ValueError:
            pages = []
        if pages and all(_is_valid(p, source.format) for p in pages):
            print(f"  ya descargado ({len(pages)} páginas), se omite")
            manifest.append(_row(run_id, source, url, filename=first.name,
                                 note=f"omitido: {len(pages)} páginas ya válidas en raw (usad --force para repetir)"))
            return

    page, pages, total, records = 1, 1, None, 0
    written: list[Path] = []
    while page <= pages:
        page_url = _with_page(url, page)
        dest = RAW / _page_name(source, page)
        print(f"  página {page}/{pages}: {page_url}")
        try:
            status, content, attempts, note = _get(page_url, http, source.format)
        except requests.RequestException as exc:
            manifest.append(_row(run_id, source, page_url, filename=dest.name, http_status=0,
                                 bytes=0, attempts=http.max_retries + 1, note=str(exc)))
            raise RuntimeError(f"{source.id}: paginación incompleta, falló la página {page}") from exc
        if status != 200 or content is None:
            manifest.append(_row(run_id, source, page_url, filename=dest.name, http_status=status,
                                 bytes=0, attempts=attempts, note=note))
            raise RuntimeError(f"{source.id}: paginación incompleta, HTTP {status} en la página {page}")
        try:
            meta, rows = _wb_page_meta(content)
        except ValueError as exc:
            manifest.append(_row(run_id, source, page_url, filename=dest.name, http_status=status,
                                 bytes=0, attempts=attempts, note=str(exc)))
            raise RuntimeError(f"{source.id}: {exc}") from exc
        dest.write_bytes(content)
        written.append(dest)
        pages = int(meta.get("pages") or 1)
        total = int(meta.get("total") or 0)
        records += len(rows)
        manifest.append(_row(run_id, source, page_url, filename=dest.name, http_status=status,
                             bytes=len(content), attempts=attempts,
                             note=f"página {page}/{pages}; total API={total}"))
        page += 1

    # Páginas sobrantes de una descarga anterior más larga
    for old in RAW.glob(f"{source.id}_page*.{source.format}"):
        if old not in written:
            old.unlink()

    if total is not None and records != total:
        raise RuntimeError(
            f"{source.id}: se descargaron {records} registros pero la API declara total={total}"
        )
    print(f"  {records} registros en {pages} páginas (total API={total})")


def extract_source(
    source: Source,
    countries: list[FocusCountry],
    http: HttpConfig,
    run_id: str,
    manifest: Manifest,
    force: bool = False,
) -> None:
    """Descarga una fuente; si tiene plantilla, una petición por país objetivo."""
    if source.paginate == "worldbank":
        _extract_paginated(source, http, run_id, force, manifest)
        return

    targets = countries if source.url_template else [None]
    for country in targets:
        iso2 = country.iso2 if country else ""
        iso3 = country.iso3 if country else ""
        url = source.resolve_url(iso2=iso2, iso3=iso3)
        dest = RAW / _raw_name(source, iso2 or None)
        manifest.append(_extract_one(source, url, dest, http, run_id, force, iso2, iso3))


def run_extract(
    cfg: Config | None = None, all_countries: bool = True, force: bool = False
) -> Path:
    # Import local: extract_module reexporta este fichero y así no hay import circular
    from .extract_module.countries import get_all_countries

    cfg = cfg or load_config()
    ensure_dirs()
    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    manifest = Manifest(MANIFEST)
    print(f"[extract] run_id={run_id} force={force}")

    countries: list[FocusCountry] | None = None
    for source in cfg.sources:
        print(f"[extract] {source.id} — {source.name}")
        if source.url_template and countries is None:
            # El perfil ya se ha descargado antes en este bucle (va primero en el YAML)
            countries = get_all_countries(cfg, RAW) if all_countries else list(cfg.focus_countries)
            print(f"[extract] {len(countries)} países objetivo para fuentes por país")
        extract_source(source, countries or [], cfg.http, run_id, manifest, force=force)

    print(f"[extract] manifiesto → {MANIFEST}")
    return MANIFEST


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Extract del observatorio país")
    parser.add_argument("--force", action="store_true",
                        help="Volver a descargar aunque el artefacto de raw ya sea válido")
    parser.add_argument("--focus-countries", action="store_true",
                        help="Usar solo focus_countries en las fuentes por país")
    args = parser.parse_args(argv)
    run_extract(all_countries=not args.focus_countries, force=args.force)


if __name__ == "__main__":
    main()
