"""Orquestación: raw → clean → serving → cards.

Uso (desde practica_01_starter/):

    python -m src.pipeline
    python -m src.pipeline --skip-extract   # reusa data/raw/
    python -m src.pipeline --parquet
    python -m src.pipeline --focus-countries  # solo focus_countries (por defecto: todos los países)
"""

from __future__ import annotations

import argparse

from .build_cards import run_build_cards
from .extract_module.extract import run_extract
from .load import run_load
from .transform import run_transform


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="ETL observatorio país (P1)")
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="No descargar; transformar desde data/raw/ existente",
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        help="Además del SQLite, escribir Parquet en data/serving/",
    )
    # Por defecto se cargan todos los países; este flag limita a focus_countries
    parser.add_argument("--focus-countries", action="store_true",
                    help="Usar solo focus_countries en vez de todos los países")

    args = parser.parse_args(argv)

    if not args.skip_extract:
        run_extract(all_countries=not args.focus_countries)
    else:
        print("[pipeline] extract omitido (--skip-extract)")

    run_transform()
    run_load(write_parquet=args.parquet)
    run_build_cards()
    print("[pipeline] listo.")


if __name__ == "__main__":
    main()
