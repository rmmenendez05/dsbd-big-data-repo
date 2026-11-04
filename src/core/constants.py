# Constantes del proyecto

from .paths import SERVING

# Extract (HTTP)
USER_AGENT = "UCJC-DSBD-practica1/1.2 (educational ETL framework)"
TIMEOUT = 60

# Base de datos (load / build_cards)
DB_PATH = SERVING / "observatorio.sqlite"
TABLES = ("countries", "languages", "indicators", "join_orphans")
