# Constantes del proyecto

from .paths import SERVING

# Extract (HTTP). El User-Agent efectivo se declara en sources.yaml (http.user_agent).
USER_AGENT = "UCJC-DSBD-BigData/2.0 (observatorio-pais; equipo dsbd-big-data-repo)"
TIMEOUT = 60
MAX_REQUESTS_PER_SECOND = 5
MAX_RETRIES = 3

# Base de datos (load / build_cards)
DB_PATH = SERVING / "observatorio.sqlite"
TABLES = ("countries", "languages", "indicators", "join_orphans", "excluded_entities")
