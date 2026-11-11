"""Acceso al extract desde extract_module.

El código está en src/extract.py, que es donde lo buscan los tests del Hito 1.

    python -m src.extract_module.extract [--force] [--focus-countries]
"""

from ..extract import (  # noqa: F401
    MANIFEST_FIELDS,
    NO_RETRY_STATUS,
    RETRY_STATUS,
    Manifest,
    extract_source,
    main,
    run_extract,
)

if __name__ == "__main__":
    main()
