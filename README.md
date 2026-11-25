# bigdata2026: repositorio docente (DSBD)

Repositorio de **demostración del profesor** para el bloque Big Data
(curso 2026/27). Contiene:

1. El **código de la primera entrega** (starter ETL `raw, clean y serving`),
   sin solución del Hito 1 / T2.
2. La batería de **tests** del Hito 1 (`tests/hito1/`) y del Hito 2
   (`tests/hito2/`), sobre el contrato de la Entrega 2.
3. Dos **workflows de GitHub Actions**: `hito1-tests.yml` y
   `hito2-tests.yml`. Cada uno ejecuta su `pytest` en cada push y pull request.

No es la solución de la práctica: es el punto de partida que ya conocéis,
más la maquinaria de comprobación y CI para que veáis cómo se monta.
El trabajo se hace en **vuestro** repositorio de equipo, no en este.

## Hoy: 23 de noviembre de 2026 (sesión 20)

**Nuevo:** criterio batch frente a streaming. No montáis un pipeline:
escribís `DECISION_S20.md` con dos casos (informe de las 08:00 → batch;
alerta de aforo → streaming). Detalle: [`ENCARGO_S20.md`](ENCARGO_S20.md)
y [`SESION_20.md`](SESION_20.md).

También se **revisa** el encargo de la sesión 19
([`ENCARGO_S19.md`](ENCARGO_S19.md)).

### Qué descargar / actualizar

```bash
git clone https://github.com/cesar-andress/bigdata2026.git
# si ya lo teníais:
cd bigdata2026 && git pull --ff-only
```

Kit del día: `make kit-hoy` → `kit_sesion_20.zip` (incluye `decision/`,
`ENCARGO_S20.md`, `tests/s20/` y lo vigente de la 19).

**Nuevo hoy:** `decision/casos.yaml`, `decision/plantilla_criterio.md`,
`tests/s20/`. Entrega: `DECISION_S20.md` en **vuestro** repo.

### Cómo lanzar los tests contra vuestro clon

```bash
pip install -r requirements.txt -r requirements-dev.txt   # pyarrow incluido
pytest -q tests/hito1 --repo=/ruta/a/vuestro/clon
pytest -q tests/hito2 --repo=/ruta/a/vuestro/clon
pytest -q tests/s19   --repo=/ruta/a/vuestro/clon
pytest -q tests/s20   --repo=/ruta/a/vuestro/clon   # nuevo hoy
# o: make test-s20 REPO=/ruta/a/vuestro/clon
```

La opción es `--repo` (registrada en `tests/conftest.py`). Un solo
fallo = el encargo de esa batería no pasa.

## Qué tenéis que hacer

El código de este repo es el de la Práctica 1. La Entrega 2 lo amplía.
Los tests de `tests/hito1/` y `tests/hito2/` leen ese código y dicen si
el contrato está cumplido. Un solo fallo y el hito no pasa.

### Hito 1: 4 de noviembre de 2026

En vuestro repo, sobre el mismo ETL (`src/`, `sources.yaml`):

1. **Extract a escala.** Al menos seis indicadores del Banco Mundial,
   con `NY.GDP.PCAP.CD` y `SP.POP.TOTL` fijos y la serie completa
   (no `mrnev=1` ni una petición por país del foco). Paginación que
   cuadre con el `total` de la API. Lo que no sea HTTP 200 no se guarda
   en `data/raw/`. Reintentos con espera ante 429, 5xx y timeouts; un
   404 no se reintenta. La segunda ejecución no vuelve a descargar lo
   que ya está bien, salvo `--force`. El manifiesto es acumulativo y
   cada fila lleva `run_id`. Como mucho cinco peticiones por segundo,
   con un `User-Agent` que identifique a vuestro equipo.
2. **Universo y transform tipado.** Criterio de país, `countries.parquet`,
   `languages.parquet` y `excluded_entities.csv`. Equivalencias de código
   en la configuración. Indicadores con el esquema del enunciado, `year`
   entero, nulo si falta el dato, y error si se repite
   `(iso3, indicator_id, year, source_id)`. Huérfanos en
   `join_orphans.csv` por fuente e indicador.
3. **Parquet Hive** en `data/clean/indicators/indicator_id=<id>/`.
   `pyarrow` (o `fastparquet`) declarado en `requirements.txt`.
4. **Fichas** regeneradas desde `clean/` a SQLite, una por país, con la
   mediana de la región en el mismo indicador y el mismo año.
5. **`CONTRIBUTORS.md`** en la raíz del repo: nombre, apellidos y correo
   institucional `@…ucjc.edu`.

`pytest -m runtime` solo comprueba que la Práctica 1 sigue pudiendo
ejecutarse. Eso no sustituye al Hito 1.

### Hito 2: 11 de noviembre de 2026, 23:59

Además de lo anterior:

1. **`spark/job_01.py`**, arrancable con `python spark/job_01.py`.
   Lee `data/clean/indicators`, calcula la cobertura por indicador y
   década tres veces (RDD al estilo MapReduce, DataFrame y Spark SQL)
   y la compara con el SQLite de `serving/`. Si no coinciden, el proceso
   sale con código distinto de 0. Escribe
   `data/serving/spark/cobertura_decada/` en Parquet, imprime el plan
   físico y el número de particiones, y deja el event log en
   `logs/spark-events/`.
2. **Pieza B.** `ENTREGA_02.md` (predicción, resultado, decisiones,
   evidencia, las tres preguntas de teoría y pendientes) y `BUGS.md`
   con al menos tres defectos (reproducción, efecto y arreglo).
3. **Pieza C.** El event log copiado en `evidencias/`.
4. **`.gitignore`** con `data/`, `logs/` y `.venv/`.
5. **Etiqueta** `entrega-02` en el repositorio (`git tag -a entrega-02`
   y `git push origin entrega-02`). La fecha de esa etiqueta en GitHub
   es la marca de la entrega.

### Cómo comprobarlo en vuestro código

Desde un clon de **este** repositorio, sin copiar `tests/` al vuestro:

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q tests/hito1 --repo=/ruta/a/vuestro/clon
pytest -q tests/hito2 --repo=/ruta/a/vuestro/clon
```

La opción es `--repo` (no `--test`). Pytest la registra en
`tests/conftest.py`, así que vale `--repo=/ruta` y también
`--repo /ruta`. Si pytest responde `unrecognized arguments: --repo`
o `no option named '--repo'`, estáis con una copia antigua de
`tests/conftest.py`: volved a copiar ese fichero. También vale
`DSBD_REPO=/ruta/a/vuestro/clon pytest -q`.

Estos tests no llaman al Banco Mundial ni regeneran `data/` de verdad.
Los de contrato leen vuestro código y `sources.yaml`. Los marcados
`runtime` ejecutan el ETL con ficheros mínimos y la red simulada, en un
directorio temporal. La prueba de aula sigue siendo la del enunciado:
clon limpio, `python -m src.pipeline` dos veces y `python spark/job_01.py`.

## Arranque local del ETL

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.pipeline
```

Pasos sueltos: `python -m src.extract`, `python -m src.transform`,
`python -m src.load`, `python -m src.build_cards`.

Reutilizar descargas ya hechas:

```bash
python -m src.pipeline --skip-extract
```

## Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q tests/hito1          # Hito 1
pytest -q tests/hito2          # Hito 2
pytest -q -m runtime           # solo lo que el código de la práctica 1 ya debe ejecutar
```

Sobre el starter original **los tests de contrato fallan a propósito**.
El detalle de qué mira cada hito, y cómo lanzar la batería contra
vuestro clon, está en «Qué tenéis que hacer».

## Encargo S20 (hoy)

[`ENCARGO_S20.md`](ENCARGO_S20.md) · checklist [`SESION_20.md`](SESION_20.md).

```bash
pytest -q tests/s20 --repo=/ruta/a/vuestro/clon
```

## Encargo S19 (plazo: antes del 23/11/2026)

Los pasos A, B y C están en [`ENCARGO_S19.md`](ENCARGO_S19.md). Datos de
DataLab: [`SESION_19.md`](SESION_19.md). Hoy se revisa en aula.

```bash
pytest -q tests/s19 --repo=/ruta/a/vuestro/clon
```

## Estado de los equipos (23 de noviembre de 2026, 18:00)

Tras `git pull --ff-only` en cada clon y medición con
`pytest tests/s19` y `pytest tests/s20` de **este** repositorio sobre la
rama por defecto de cada GitHub. **Pasa** = cero fallos.
Plazo S19: antes de la clase del **23/11/2026**. Encargo S20: publicar
`DECISION_S20.md` en el repo del equipo.

| Repositorio | Último commit | `entrega-02` | S19 (`tests/s19`) | S20 (`tests/s20`) |
| --- | --- | --- | --- | --- |
| [nasratullahjabarkhil/bigdata](https://github.com/nasratullahjabarkhil/bigdata) | `189cefd`, 21/11 | sí | **Pasa** (11) | falta `DECISION_S20.md` |
| [rmmenendez05/dsbd-big-data-repo](https://github.com/rmmenendez05/dsbd-big-data-repo) | `a3b253e`, 18/11 | sí (11/11) | **Pasa** (11) | falta `DECISION_S20.md` |
| [hracjc/dsbd_etl_1](https://github.com/hracjc/dsbd_etl_1) | `99c0bfe`, 19/11 | no | 9 fallos, 2 passed | falta `DECISION_S20.md` |
| [edurlix/BigDataProject](https://github.com/edurlix/BigDataProject) | `7e56274`, 18/11 | sí (18/11, fuera de plazo) | 2 fallos (sin `streaming/` ni acta) | falta `DECISION_S20.md` |
| [uag8076-web/BigData2026](https://github.com/uag8076-web/BigData2026) | `0200627`, 18/11 | sí (18/11, fuera de plazo) | 2 fallos (sin `streaming/` ni acta) | falta `DECISION_S20.md` |
| [marcosgale/BigData_Proyecto](https://github.com/marcosgale/BigData_Proyecto) | `6f5b171`, 23/11 | no | 2 fallos (sin `streaming/` ni acta) | falta `DECISION_S20.md` |
| [Sumitodemarmeto/Practica_02_BigData](https://github.com/Sumitodemarmeto/Practica_02_BigData) | `6c02a33`, 18/11 | no | 2 fallos (sin `streaming/` ni acta) | falta `DECISION_S20.md` |

Ningún equipo ha publicado aún `DECISION_S20.md` (medición 18:00).
Dos equipos cumplen S19. El resto: copiar `streaming/` desde este repo,
rellenar T1–T6, escribir `ACTA_S19.md` y pasar `tests/s19`. Para S20:
partid de `decision/plantilla_criterio.md` y de [`ENCARGO_S20.md`](ENCARGO_S20.md).

## Problemas y arreglos (para pasar los tests)

### [nasratullahjabarkhil/bigdata](https://github.com/nasratullahjabarkhil/bigdata): S19 cumple; S20 pendiente

Nasratullah Jabarkhil. `ACTA_S19.md` + `streaming/temp_stream.py`;
`tests/s19` en verde (21/11). Tiene `entrega-02`. Falta `DECISION_S20.md`.

### [rmmenendez05/dsbd-big-data-repo](https://github.com/rmmenendez05/dsbd-big-data-repo): S19 cumple; S20 pendiente

Rodrigo Moreno Menéndez. Hitos 1 y 2 en plazo; encargo S19 también
pasa `tests/s19`. Falta `DECISION_S20.md`.

### [hracjc/dsbd_etl_1](https://github.com/hracjc/dsbd_etl_1): S19 a medias; S20 pendiente

Hay `streaming/`, pero fallan huecos T4–T6 y falta `ACTA_S19.md`.
Sin `DECISION_S20.md`.

### [edurlix/BigDataProject](https://github.com/edurlix/BigDataProject), [uag8076-web/BigData2026](https://github.com/uag8076-web/BigData2026), [marcosgale/BigData_Proyecto](https://github.com/marcosgale/BigData_Proyecto), [Sumitodemarmeto/Practica_02_BigData](https://github.com/Sumitodemarmeto/Practica_02_BigData): sin S19; S20 pendiente

Copiad `streaming/` desde este repo, rellenad T1–T6, escribid
`ACTA_S19.md` y ejecutad `pytest -q tests/s19 --repo=...`.
Para S20: `DECISION_S20.md` + `pytest -q tests/s20 --repo=...`.

Marcos: el trabajo del 23/11 (`6f5b171`, Update Extract) aún no incluye
S19 ni S20. Sumito: el repo volvió a ser público (`6c02a33`).

### Checklist mínimo del Hito 2 (quien aún no lo tenga)

1. `spark/job_01.py` ejecutable: tres vías (RDD / DataFrame / Spark SQL)
   alineadas con SQLite; Parquet en `data/serving/spark/cobertura_decada/`;
   event log en `logs/spark-events/`.
2. `ENTREGA_02.md` + `BUGS.md` (≥ 3 defectos).
3. Copiar el event log a `evidencias/`.
4. `.gitignore` con `data/`, `logs/`, `.venv/`.
5. `git tag -a entrega-02` y `git push origin entrega-02`.

## Nota histórica

El 4/11 ningún equipo pasaba ningún hito. El 18/11 a las 19:00 se midió
otra vez (tabla antigua en el historial Git). La tabla canónica es la
del **23 de noviembre, 18:00** de arriba (S19 + S20 tras `git pull`).

## Cómo funcionan las tareas automatizadas

Hay dos workflows de **GitHub Actions**, uno por hito:

- `.github/workflows/hito1-tests.yml` ejecuta `pytest -q tests/hito1`.
- `.github/workflows/hito2-tests.yml` ejecuta `pytest -q tests/hito2`.

No hace falta lanzarlos a mano: GitHub los lee en cuanto entran en el
repositorio. Los dos se disparan en los mismos casos, y solo en las
ramas `main` y `master`:

- un **push** (incluido el que acabáis de hacer);
- un **pull request** hacia esas ramas.

Cada push abre **dos máquinas** Ubuntu, independientes. Si el Hito 1
falla, el Hito 2 se ejecuta igual. En cada máquina los pasos son:

1. **Checkout.** Clona el commit que ha disparado el workflow.
2. **Python 3.12.** Instala ese intérprete y guarda la caché de pip
   para no reinstalar desde cero en cada ejecución.
3. **Dependencias.** `pip install -r requirements.txt -r requirements-dev.txt`
   (`requests`, `pandas`, `PyYAML`, `pyarrow` y `pytest`).
4. **El pytest de ese hito.** El del Hito 1 no lanza el del Hito 2, ni al revés.

El resultado queda en la pestaña **Actions** de este repositorio:
https://github.com/cesar-andress/bigdata2026/actions
Cada ejecución enseña el workflow, el commit, la hora y el registro de
`pytest`. Una cruz roja significa que algún test de ese hito ha fallado.
En este repo docente la cruz es esperable: el starter no cumple el
contrato. Verde solo cuando el `pytest` de ese workflow termina sin fallos.

Esos workflows comprueban **el código de este repositorio**, no el vuestro.
Copiar los dos YAML, `tests/`, `pytest.ini` y `requirements-dev.txt` a
vuestro repo hace que las mismas máquinas se disparen en cada push
vuestro. `--repo` se declara dentro de `tests/conftest.py`; no hace
falta el `conftest.py` de la raíz. Mientras no lo copiéis, la forma de
ver vuestro estado es la de la sección anterior (`--repo`) o la tabla
de más arriba, que actualiza el profesor.

## Estructura

| Ruta | Contenido |
| --- | --- |
| `SESION_20.md` | Checklist de aula (23/11, criterio batch vs streaming) |
| `ENCARGO_S20.md` | Entrega de hoy (`DECISION_S20.md` + tests/s20) |
| `decision/` | Casos YAML + plantilla del criterio |
| `tests/s20/` | Comprobación estática de `DECISION_S20.md` |
| `SESION_19.md` | Checklist (18/11, Structured Streaming) |
| `ENCARGO_S19.md` | Entrega S19 (T1 a T6 + tests/s19 + acta) |
| `streaming/` | Starter `temp_stream.py` y scripts de publicación (S19) |
| `tests/s19/` | Comprobación estática del query y del acta (S19) |
| `Makefile` | `deps`, `test-hito*`/`test-s19`/`test-s20`, `kit-hoy` |
| `src/` | Extract / transform / load / fichas |
| `sources.yaml` | Fuentes y países foco |
| `data/` | `raw`, `clean` y `serving` (artefactos locales, no versionados) |
| `tests/hito1/` | Comprobaciones del Hito 1, más la ejecución del ETL actual |
| `tests/hito2/` | Comprobaciones del Hito 2 (Spark y cierre) |
| `tests/conftest.py` | Registra `--repo` / `DSBD_REPO` |
| `.github/workflows/hito1-tests.yml` | CI del Hito 1 |
| `.github/workflows/hito2-tests.yml` | CI del Hito 2 |

## Reglas del framework (no romper)

- **Extract** no limpia ni une fuentes.
- **Transform** no descarga; solo lee `data/raw/`.
- **Load / fichas** no leen JSON bruto; solo de `clean/` a `serving/`.
