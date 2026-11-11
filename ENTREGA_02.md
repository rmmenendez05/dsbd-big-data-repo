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

_Pendiente: cifras reales tras `python -m src.pipeline` (total de la API por
indicador, filas en clean por indicador, número de países aceptados y
excluidos) y explicación de cada diferencia con la predicción._

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
`indicator_id` (6 + 2 carpetas). Es la columna del filtro habitual («este
indicador») y tiene pocos valores distintos. Particionar por `iso3` daría unas
190 carpetas diminutas y obligaría a abrir casi todas para leer un indicador.

**Extract.** Una URL `country/all` paginada por indicador (no una por país),
reintentos con espera creciente (0,5 s, 1 s, 2 s) ante 429/5xx/timeout, 404
definitivo, como mucho 5 peticiones/s, `User-Agent` del equipo, idempotencia
por defecto (`--force` para repetir) y manifiesto acumulativo con `run_id`.

## 4. Evidencia

_Pendiente: salidas de terminal de la primera y la segunda ejecución del
pipeline (la segunda sin re-descargas), salida de `python spark/job_01.py` con
`exit=0` y nombre del subdirectorio del event log copiado en `evidencias/`._

## 5. Preguntas de teoría (con nuestros datos)

1. **Event log: ¿qué registra Spark antes de la primera acción del job?**
   _Pendiente: lo que se ve en el event log de `evidencias/` antes del primer
   job._
2. **Particiones y shuffle: ¿cuántas particiones tiene el DataFrame leído y
   dónde está el shuffle en el plan físico?** _Pendiente:
   `getNumPartitions()` con `local[1]` y `local[*]`, y el operador `Exchange`
   del plan físico de la versión DataFrame._
3. **CSV frente a Parquet: ¿cuánto ocupa `indicators` en cada formato?**
   _Pendiente: tamaño de `data/clean/indicators.csv` frente al dataset
   Parquet `data/clean/indicators/`._

## 6. Pendientes

_Pendiente: lo que quede sin resolver tras la entrega._
