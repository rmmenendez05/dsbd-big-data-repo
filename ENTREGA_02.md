# Entrega 02 · Del ejemplo al universo

Rodrigo Moreno Menéndez · rodrigo.morenom@alumno.ucjc.edu

## 1. Predicción (antes de la primera ejecución completa)

Escrita y commiteada **antes** de ejecutar por primera vez el pipeline completo
sobre el universo. Hay dos cifras distintas.

**A. Bruto por indicador (lo que declara la API y comprueba el extract).**
El Banco Mundial devuelve en `country/all` 266 entidades (unas 217 no
agregadas y unas 49 regiones o grupos) por 66 años (1960–2025):
266 × 66 = **17 556 registros por indicador**, que coincide con el `total` del
enunciado. Espero ese mismo total en los seis indicadores, porque la API
devuelve una fila por entidad y año aunque el valor sea nulo. Con
`per_page=10000` serán **2 páginas por indicador**, 12 peticiones en total.
En bruto: 6 × 17 556 = 105 336 registros. Esa cifra **no** es la del limpio.

**B. Filas en `data/clean/indicators` (tras el criterio de país).**
El criterio es `independent` de mledoze: unos 195 estados. Quito los que el
Banco Mundial no publica (Taiwán, Ciudad del Vaticano y algún otro). Quedan
unos **192 países × 66 años ≈ 12 700 filas por indicador** del Banco
Mundial. El transform conserva la fila con `value` nulo si la API la trae
(nulo explícito, nunca 0).

| Indicador | Filas esperadas en clean | Filas con valor no nulo (estimación) |
| --- | --- | --- |
| NY.GDP.PCAP.CD | ≈ 12 700 | ≈ 10 000 (muchos huecos antes de 1990) |
| SP.POP.TOTL | ≈ 12 700 | ≈ 12 600 (serie casi completa) |
| SP.DYN.LE00.IN | ≈ 12 700 | ≈ 12 200 (los últimos años aún sin dato) |
| SP.URB.TOTL.IN.ZS | ≈ 12 700 | ≈ 12 600 |
| SP.DYN.TFRT.IN | ≈ 12 700 | ≈ 12 200 |
| SL.UEM.TOTL.ZS | ≈ 12 700 | ≈ 6 300 (la serie modelada de la OIT empieza en 1991) |

Total del Banco Mundial en clean: **≈ 76 000 filas**, de las que unas
**65 000** tienen valor. A eso se suman Eurostat (solo países europeos del
universo, unos 40 × 60 años) y OWID (serie larga, con años desde el siglo
XVIII en algunos países).

**Excluidos que espero ver:** unos 55 del perfil por no ser independientes
(territorios y dependencias), unos 49 agregados del Banco Mundial por cada
indicador (`WLD`, `EUU`, `AFE`…), agregados de Eurostat (`EU27_2020`, `EA20`…)
y de OWID (`World`, continentes sin código ISO3).

## 2. Resultado y diferencia con la predicción

Primera ejecución completa el 11/11/2026 (`run_id` `20261111T090002Z-88b77a00`),
en Docker (`compose.yaml`), después del commit de la predicción.

**A. Bruto.** Los seis indicadores declaran `total=17556` y se guardaron
17 556 registros de cada uno en 2 páginas: **acierto exacto** (105 336 en
total). Eurostat respondió HTTP 200 con una página HTML de «Server temporarily
unavailable» en todas las ejecuciones del día. El extract lo reintenta y no lo
guarda (véase BUGS.md, defecto 7), así que esta entrega sale sin Eurostat.

**B. Limpio.** 194 países aceptados (predicción: unos 195) y 193 con datos
del Banco Mundial; el que falta es la Ciudad del Vaticano (`VAT`), que el
Banco Mundial no publica.

| Indicador | Filas en clean (pred.) | Con valor (pred.) |
| --- | --- | --- |
| NY.GDP.PCAP.CD | 12 738 (≈ 12 700) | 10 962 (≈ 10 000) |
| SP.POP.TOTL | 12 738 (≈ 12 700) | 12 738 (≈ 12 600) |
| SP.DYN.LE00.IN | 12 738 (≈ 12 700) | 12 541 (≈ 12 200) |
| SP.URB.TOTL.IN.ZS | 12 738 (≈ 12 700) | 12 738 (≈ 12 600) |
| SP.DYN.TFRT.IN | 12 738 (≈ 12 700) | 12 543 (≈ 12 200) |
| SL.UEM.TOTL.ZS | 12 738 (≈ 12 700) | 6 219 (≈ 6 300) |
| life_expectancy (OWID) | 16 344 | 16 344 |

Banco Mundial: **76 428 filas** (predicción ≈ 76 000), 67 741 con valor
(predicción ≈ 65 000). Total en `data/clean/indicators`: **92 772 filas**,
84 085 con valor. 193 × 66 años = 12 738 explica exactamente las filas por
indicador.

**Diferencias con la predicción.**

- *Excluidos del Banco Mundial:* 73 por indicador, no los 49 previstos. Mi
  predicción solo contaba los agregados, pero el criterio `independent`
  también deja fuera entidades no agregadas que no son estados, como
  `PRI` (Puerto Rico) y `HKG` (Hong Kong): 266 − 193 = 73.
- *Excluidos del perfil:* 56 (predicción: unos 55). Con OWID, 548 en total.
- *Valores no nulos:* los subestimé. Población y población urbana no tienen
  ni un nulo en 1960–2025; PIB per cápita tiene más datos antiguos de los que
  esperaba.
- *Huérfanos:* 216. 194 son de Eurostat (fuente caída); de los demás, 16 son
  de desempleo (microestados sin estimación de la OIT) y 2 de PIB.

## 3. Decisiones

**Criterio de país.** Se acepta como país una entrada de mledoze/countries
con `independent: true` (`universe.criterion` en `sources.yaml`). Todo lo
demás va a `data/clean/excluded_entities.csv` con `source_id`, `source_code`,
`name` y `reason`: territorios del perfil, agregados del Banco Mundial,
Eurostat y OWID, y entidades sin código ISO3. Así un agregado como la
«European Union» no entra en la mediana regional de las fichas.

**Equivalencias de códigos.** Van en `sources.yaml` (`aliases` por fuente),
no en el código: Eurostat usa `EL` para Grecia y `UK` para Reino Unido
(→ `GR`, `GB`). Banco Mundial y OWID usan `XKX` y `OWID_KOS` para Kosovo
(→ `UNK`, el código de mledoze).

**Indicadores del Banco Mundial (seis).** Los dos fijos y cuatro elegidos:

- `NY.GDP.PCAP.CD` (PIB per cápita): fijo.
- `SP.POP.TOTL` (población total): fijo.
- `SP.DYN.LE00.IN` (esperanza de vida): resume salud; se contrasta con la de OWID.
- `SP.URB.TOTL.IN.ZS` (población urbana, %): mide el desarrollo con una serie casi completa desde 1960.
- `SP.DYN.TFRT.IN` (fecundidad): explica la evolución futura de la población.
- `SL.UEM.TOTL.ZS` (desempleo): indicador del mercado laboral; su serie empieza en 1991 y obliga a tratar bien los nulos.

**Particionado.** `data/clean/indicators/` se particiona por
`indicator_id`: una carpeta por indicador (7 en esta ejecución, 8 cuando
Eurostat responde). Es la columna del filtro habitual («este
indicador») y tiene pocos valores distintos. Particionar por `iso3` daría unas
190 carpetas diminutas y obligaría a abrir casi todas para leer un indicador.

**Extract.** Una URL `country/all` paginada por indicador (no una por país),
reintentos con espera creciente (0,5 s, 1 s, 2 s) ante 429/5xx/timeout, 404
definitivo, como mucho 5 peticiones/s, `User-Agent` del equipo, idempotencia
por defecto (`--force` para repetir) y manifiesto acumulativo con `run_id`.

## 4. Evidencia

Event log del job: `evidencias/eventlog_v2_local-1794387660065/` (copia
entera del subdirectorio de `logs/spark-events/`).

Segunda ejecución del pipeline (`run_id` `20261111T090030Z-973712bf`): no
descarga nada que ya estuviera bien; solo reintenta Eurostat, que no tiene
ningún fichero válido en `raw/`. El manifiesto acumula las filas de ambas.

```text
[extract] run_id=20261111T090030Z-973712bf force=False
[extract] worldbank_gdp — World Bank — PIB per cápita (NY.GDP.PCAP.CD)
  ya descargado (2 páginas), se omite
  …(igual en los seis indicadores, el perfil y OWID)
[extract] eurostat_pop — Eurostat — población a 1 de enero (demo_pjan)
    reintento 3/3 en 2.0s (HTTP 200 con cuerpo que no es json)
[transform] countries: 194 filas
[transform] excluded_entities: 548 filas
[transform] indicators: 92772 filas
[transform] huérfanos del join: 216
[pipeline] listo.
```

`python spark/job_01.py`:

```text
[job_01] particiones del DataFrame leído: 7
[job_01] cobertura_decada → data/serving/spark/cobertura_decada
[job_01] dataframe: 88 grupos (indicator_id, década)
[job_01] rdd: 88 grupos (indicator_id, década)
[job_01] spark_sql: 88 grupos (indicator_id, década)
[job_01] sqlite: 88 grupos (indicator_id, década)
[job_01] RDD, DataFrame, Spark SQL y SQLite coinciden.
exit=0
```

Ejemplo de la cobertura (PIB per cápita): 120 países con dato en los 60,
190 en los 90 y 192 en los 2000. La década de 2020 tiene 1 118 filas porque
2025 aún está casi vacío.

## 5. Preguntas de teoría (con nuestros datos)

1. **Event log: ¿qué registra Spark antes de la primera acción del job?**
   Antes del primer `JobStart` hay seis eventos de arranque: `LogStart`,
   `ResourceProfileAdded`, `ExecutorAdded`, `BlockManagerAdded`,
   `EnvironmentUpdate` (con `spark.master=local[*]`) y `ApplicationStart`.
   Después, el log registra 16 jobs y 17 stages (229 eventos). El **job 0**
   (1 tarea, sin ejecución SQL asociada) aparece antes de nuestra primera
   acción: lo lanza `spark.read.parquet` para leer el esquema de los
   ficheros, así que la lectura no es del todo «perezosa». La primera acción
   del job (la escritura de `cobertura_decada`, ejecución SQL 0) empieza en
   el job 1.
2. **Particiones y shuffle.** El DataFrame leído tiene **7 particiones** con
   `local[*]` (12 núcleos): una por fichero, porque cada carpeta
   `indicator_id=…` tiene un solo Parquet de unos 100 KB, muy por debajo de
   `maxPartitionBytes` (128 MB). Lo comprobamos con nuestros datos: con
   `local[1]` sale 1 partición y con `local[4]`, 4. Con ficheros tan
   pequeños, Spark agrupa ficheros por núcleo y el número de particiones al
   leer sí depende del paralelismo; con ficheros grandes dependería sobre todo
   de `maxPartitionBytes`. En el plan físico de la versión DataFrame hay
   **dos `Exchange`** (shuffle, `hashpartitioning(..., 200)`). El primero
   reparte por `(indicator_id, decada, iso3)` y el segundo por
   `(indicator_id, decada)`: `countDistinct(iso3)` obliga a agregar en dos
   niveles. `AdaptiveSparkPlan` (AQE) agrupa después esas 200 particiones de
   shuffle.
3. **CSV frente a Parquet.** `data/clean/indicators.csv` ocupa
   **9 306 861 bytes (8,9 MB)**; el dataset Parquet `data/clean/indicators/`
   ocupa **681 719 bytes (0,68 MB)**: unas **13,7 veces menos**. Influyen la
   compresión por columnas, que `year` y `value` son binarios y no texto, y
   que `indicator_id` no se repite en cada fila: está en el nombre de la
   carpeta Hive.

## 6. Pendientes

- Eurostat estuvo caído todo el día (HTTP 200 con HTML). El código ya lo
  trata bien; falta una ejecución con Eurostat disponible para tener su
  población (`demo_pjan`) en clean y comprobar los alias `EL`/`UK`.
- Kosovo: los alias `XKX`/`OWID_KOS` → `UNK` están declarados, pero con el
  criterio `independent` Kosovo queda fuera; habría que decidir si se acepta.
- Las fichas usan el último año con dato de la región. Podría añadirse una
  comparación con una serie de varios años.
