"""Acceso a las rutas como src.paths (así lo importan los tests del Hito 1).

Las rutas se definen en src/core/paths.py.
"""

from .core.paths import CARDS, CLEAN, DATA, MANIFEST, RAW, ROOT, SERVING, SOURCES_YAML  # noqa: F401


def ensure_dirs() -> None:
    # Usa las rutas de este módulo para que se puedan redirigir desde aquí
    for d in (RAW, CLEAN, SERVING, CARDS):
        d.mkdir(parents=True, exist_ok=True)
