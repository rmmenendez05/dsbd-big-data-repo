# BUGS

Defectos encontrados en el ETL de la Práctica 1 (tag `v1.0.0`) y corregidos
en el Hito 1 (tag `hito-1`). El 7 apareció al ejecutar el universo completo en el Hito 2. Para cada uno: cómo se reproduce, qué efecto
tenía en los datos o en las fichas y cómo se arregló.

## 1. El extract guardaba en `data/raw/` respuestas de error

- **Reproducción:** pedir una URL que devuelva HTTP 500 o 404 (por ejemplo,
  un código de indicador mal escrito en `sources.yaml`). `_download` escribía
  `resp.content` en `data/raw/` sin mirar `status_code`.
- **Efecto:** el cuerpo del error quedaba en `raw/` con el nombre de un dato
  válido. El transform lo leía como si fuera la respuesta de la API: el país
  desaparecía del resultado sin aviso y, al relanzar, el fichero parecía ya
  descargado.
- **Arreglo:** `_get` reintenta los 429/5xx y los errores de red con espera
  creciente y no reintenta el 404. `_download` solo escribe si el status es
  200; el resto queda anotado en el manifiesto (`http_status`, `attempts`,
  `note`). Test: `test_respuesta_no_200_no_escribe_raw`.

## 2. El manifiesto se sobrescribía en cada ejecución

- **Reproducción:** ejecutar `python -m src.pipeline` dos veces y abrir
  `data/raw/manifest.csv`. Solo quedaban las filas de la última ejecución,
  porque se abría en modo `"w"`.
- **Efecto:** se perdía el historial de descargas. No había forma de saber
  qué se pidió en una ejecución anterior ni qué falló.
- **Arreglo:** la clase `Manifest` abre en modo append, cada fila lleva un
  `run_id` por ejecución y un manifiesto antiguo con otras columnas se migra
  conservando sus filas.

## 3. El criterio de universo `one_member` daba un universo vacío

- **Reproducción:** poner `universe.criterion: one_member` en `sources.yaml`
  y ejecutar el transform.
- **Efecto:** el filtro preguntaba por el campo `oneMember`, que no existe en
  mledoze/countries (el campo es `unMember`). Ningún país pasaba, `countries`
  quedaba vacía y `build_cards` abortaba con «Tabla countries vacía».
- **Arreglo:** el criterio se llama ahora `un_member` y lee `unMember`
  (`one_member` se mantiene como alias). Además, lo que no pasa el criterio
  se escribe en `data/clean/excluded_entities.csv` con su motivo, en vez de
  descartarse en silencio.

## 4. El año del Banco Mundial se guardaba como texto

- **Reproducción:** leer `data/clean/indicators.csv` de la versión anterior
  con tipos: `year` venía del campo `date` del Banco Mundial (`"2020"`),
  OWID lo dejaba como `str(...)` y Eurostat lo tomaba de `meta.time`.
- **Efecto:** la columna mezclaba texto y números. Ordenar por año, comparar
  años o calcular la década no era fiable, y el Parquet heredaba un tipo
  string.
- **Arreglo:** `_typed_indicators` convierte `year` a entero (`Int64`) y
  `value` a numérico en todas las fuentes; un valor ausente queda nulo,
  nunca 0.

## 5. El comparativo de las fichas mezclaba años distintos

- **Reproducción:** generar la ficha de un país con un indicador de varios
  años. El bloque comparativo calculaba la media de la región con todos los
  años de todos los países, frente al primer valor del país.
- **Efecto:** se comparaba, por ejemplo, el PIB de un país en 2023 con una
  media regional que incluía 1960. La conclusión «por encima / por debajo»
  podía ser falsa. Además usaba la media, sensible a valores extremos.
- **Arreglo:** el comparativo usa la mediana de la región para el mismo
  indicador y el mismo año. Si el país no tiene dato ese año, la ficha lo
  dice («sin dato») en vez de saltarse la comparación.

## 6. Los huérfanos del join se calculaban solo por país

- **Reproducción:** un país con PIB pero sin población no aparecía en
  `join_orphans.csv`, porque bastaba con que tuviera cualquier indicador.
- **Efecto:** la cobertura real de cada fuente no se veía. Faltas de datos de
  una fuente concreta pasaban inadvertidas.
- **Arreglo:** `join_orphans.csv` tiene una fila por
  (`iso3`, `source_id`, `indicator_id`) sin ningún valor.

## 7. Un HTTP 200 con una página HTML de error se guardaba como dato

Encontrado en la primera ejecución completa del Hito 2 (11/11/2026).

- **Reproducción:** `python -m src.pipeline` con Eurostat caído. La API
  responde **HTTP 200** con una página HTML («Server temporarily
  unavailable») en vez de JSON. Ver las primeras líneas de
  `data/raw/eurostat_pop.json`: empezaba por `<!DOCTYPE html>`.
- **Efecto:** el extract solo miraba el código HTTP y guardó el HTML como si
  fuera el JSON de Eurostat. El transform se cayó con
  `JSONDecodeError: Expecting value: line 1 column 1` y no se generó nada en
  `clean/`, así que el job de Spark tampoco tenía Parquet que leer. En la
  segunda ejecución la idempotencia veía el fichero como inválido, lo volvía
  a pedir y recibía el mismo HTML.
- **Arreglo:** `_body_ok` comprueba que el cuerpo de un 200 se pueda leer en
  el formato declarado (`format: json` o `csv`; rechaza HTML). Si no, se
  trata como error transitorio: reintentos con espera creciente y, si sigue
  igual, no se guarda y se anota en el manifiesto. `_drop_invalid` borra el
  fichero inválido que hubiera quedado en `raw/` de otra ejecución. Con
  Eurostat caído, el pipeline termina con `exit=0` y sus países aparecen como
  huérfanos de esa fuente.
