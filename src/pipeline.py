"""Orquestación: raw → clean → serving → cards.

Uso (desde la raíz del repo):

    python -m src.pipeline
    python -m src.pipeline --force            # repite descargas aunque raw ya sea válido
    python -m src.pipeline --skip-extract     # depuración: reusa data/raw/ sin llamar al extract
    python -m src.pipeline --parquet          # además, copia Parquet en data/serving/
    python -m src.pipeline --focus-countries  # universo = focus_countries (por defecto: criterio del YAML)

El extract es idempotente por sí mismo: sin --force no vuelve a pedir lo que
ya está bien descargado.
"""

from __future__ import annotations

import argparse

from .build_cards import run_build_cards
from .core.config import load_config
from .extract_module.extract import run_extract
from .load import run_load
from .transform import run_transform


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="ETL observatorio país")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Volver a descargar aunque el artefacto de data/raw/ ya sea válido",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="No llamar al extract; transformar desde data/raw/ existente",
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        help="Además del SQLite, escribir Parquet en data/serving/",
    )
    parser.add_argument(
        "--focus-countries",
        action="store_true",
        help="Usar solo focus_countries en vez del criterio de universo del YAML",
    )

    args = parser.parse_args(argv)

    if not args.skip_extract:
        run_extract(all_countries=not args.focus_countries, force=args.force)
    else:
        print("[pipeline] extract omitido (--skip-extract)")

    if args.focus_countries:
        cfg = load_config()
        cfg.universe_criterion = "focus"
        run_transform(cfg)
    else:
        run_transform()
    run_load(write_parquet=args.parquet)
    run_build_cards()
    print("[pipeline] listo.")


if __name__ == "__main__":
    main()
