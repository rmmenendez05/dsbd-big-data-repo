# Observatorio país (Prácticas 1 y 2 de Big Data)

ETL **`raw → clean → serving`** sobre el universo de países y la serie histórica
completa, con la zona limpia en Parquet particionado y un job de Spark que
calcula la cobertura por indicador y década.

- Equipo: [CONTRIBUTORS.md](CONTRIBUTORS.md)
- Entrega 2 (predicción, resultado, decisiones, teoría): [ENTREGA_02.md](ENTREGA_02.md)
- Defectos encontrados: [BUGS.md](BUGS.md)
- Event log del job: [evidencias/](evidencias/)

## Fuentes

| Fuente | Qué aporta | Petición |
| --- | --- | --- |
| mledoze/countries | Perfil, idiomas y criterio de país (`independent`) | Fichero único |
| Banco Mundial (WDI) | 6 indicadores: `NY.GDP.PCAP.CD`, `SP.POP.TOTL`, `SP.DYN.LE00.IN`, `SP.URB.TOTL.IN.ZS`, `SP.DYN.TFRT.IN`, `SL.UEM.TOTL.ZS` | `country/all`, paginada y contrastada con `total` |
| Eurostat `demo_pjan` | Población a 1 de enero | Todas las entidades y años en una petición |
| Our World in Data | Esperanza de vida | CSV completo |

Todo se declara en [sources.yaml](sources.yaml): fuentes, criterio de país
(`universe.criterion`), equivalencias de códigos (`aliases`) y cortesía HTTP
(`http`: User-Agent, 5 peticiones/s, reintentos).

## Arranque

Requiere Python 3.10+ y Java 17, 21 o 25 (para Spark).

```bash
python3 -m venv .venv
. .venv/bin/activate          # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m src.pipeline        # 1.ª vez: descarga y genera todo
python -m src.pipeline        # 2.ª vez: no vuelve a descargar lo que ya es válido
python spark/job_01.py; echo "exit=$?"
```

### Con Docker (recomendado en Windows)

En Windows, Spark necesita `winutils.exe` y `hadoop.dll`. La imagen del
[Dockerfile](Dockerfile) trae Python 3.12 y Java 17, y monta el repo, así que
las salidas quedan en el disco local:

```bash
docker compose build
docker compose run --rm etl python -m src.pipeline
docker compose run --rm etl python spark/job_01.py
docker compose run --rm etl python -m pytest tests
```

### Opciones del pipeline

| Opción | Efecto |
| --- | --- |
| `--force` | Vuelve a descargar aunque `data/raw/` ya tenga un fichero válido |
| `--skip-extract` | Depuración: no entra en el extract y transforma el `raw/` existente |
| `--focus-countries` | Universo = `focus_countries` del YAML en vez del criterio |
| `--parquet` | Además, copia las tablas en Parquet a `data/serving/` |

Pasos sueltos: `python -m src.extract [--force]` · `python -m src.transform` ·
`python -m src.load` · `python -m src.build_cards`.

## Salidas

| Zona | Ruta |
| --- | --- |
| bruto | `data/raw/*.json`, `*.csv` + `data/raw/manifest.csv` (acumulativo, con `run_id`) |
| limpio | `data/clean/countries.parquet`, `languages.parquet`, `indicators/indicator_id=<id>/*.parquet`, `excluded_entities.csv`, `join_orphans.csv` |
| servicio | `data/serving/observatorio.sqlite`, `data/serving/spark/cobertura_decada/` |
| fichas | `cards/<ISO3>.md`, una por país aceptado |
| Spark | `logs/spark-events/` (event log; lo entregado se copia a `evidencias/`) |

`data/`, `logs/`, `cards/` y `.venv/` se regeneran y no se suben (`.gitignore`).

## Estructura del código

| Módulo | Qué hace |
| --- | --- |
| [src/extract.py](src/extract.py) | Descarga intacta a `raw/`: paginación, reintentos, idempotencia, manifiesto. `src/extract_module/` lo reexporta |
| [src/transform.py](src/transform.py) | `raw/` → `clean/`: criterio de país, esquema tipado, clave única, huérfanos y Parquet |
| [src/transform_module/universe.py](src/transform_module/universe.py) | Criterio de país y entidades excluidas |
| [src/load.py](src/load.py) | Parquet de `clean/` → SQLite de `serving/` |
| [src/build_cards.py](src/build_cards.py) | Fichas: mediana de la región, mismo indicador y mismo año |
| [spark/job_01.py](spark/job_01.py) | Cobertura por (indicator_id, década) en RDD, DataFrame y Spark SQL, contrastada con SQLite |
| `src/core/` | Configuración (`sources.yaml`), rutas y constantes |

## Cómo añadir una fuente

1. Añadid un bloque en `sources.yaml` (`url`, `url_template` con `{iso2}`/`{iso3}`,
   o `paginate: worldbank`).
2. Si el formato no encaja con un transformer existente, cread una función en
   `src/transform.py` y registradla en `TRANSFORMERS`.
3. Poned `transformer: nombre_de_la_funcion` en el YAML y ejecutad el pipeline.

## Reglas del pipeline

- **Extract** no limpia ni une fuentes; solo guarda en `raw/` respuestas HTTP 200
  con un cuerpo válido.
- **Transform** no descarga; solo lee `raw/` y escribe `clean/`.
- **Load, fichas y Spark** no leen `raw/`: trabajan desde `clean/` y `serving/`.
- Nada se descarta en silencio: lo que no es país va a `excluded_entities.csv`.

## Tests

```bash
python -m pytest tests/hito1 tests/hito2
```
