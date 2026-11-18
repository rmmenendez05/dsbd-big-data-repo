# Acta S19 · Query continuo `temp-stream`

Rodrigo Moreno Menéndez · rodrigo.morenom@alumno.ucjc.edu

Query de Spark Structured Streaming ([streaming/temp_stream.py](streaming/temp_stream.py))
que lee `sensores.temp` de Kafka, cuenta eventos por ventana de 1 minuto y
sensor con una watermark de 10 minutos, y los muestra en consola cada 5 s.
Un segundo query, `temp-nulos`, enseña los eventos cuyo `valor` llega nulo.

## Dónde se ha ensayado

| Entorno | Spark | Kafka | Estado |
| --- | --- | --- | --- |
| Local (Docker) y CI (`.github/workflows/s19-streaming.yaml`) | 3.5.9, Java 17 | `apache/kafka:3.9.0`, topic `sensores.temp` con 1 partición | hecho, 0 comprobaciones fallidas |
| DataLab (`admin@datalab`) | 3.5.9 | `127.0.0.1:9092` | **pendiente: completar la tabla de abajo** |

Las cifras de este acta salen del ensayo local del 18/11/2026 (hora CET). El CI
repite el mismo ensayo en cada push y deja los logs y los checkpoints en el
artefacto `ensayo-s19`.

## Ensayo de parar y arrancar

| Paso | Qué se hace | Qué se observa |
| --- | --- | --- |
| 1 | Se publican 3 eventos de `s01` y se arranca por primera vez | `temp-stream` id `1dcfb1b7…`, `temp-nulos` id `4e377878…`. Batch 0: ventana 19:24–19:25, `s01`, count 3 |
| 2 | Ctrl+C | Los dos queries se paran con `stop()`. Quedan `offsets/`, `commits/` y `state/` en `~/checkpoints/temp-stream` |
| 3 | Con el query **parado**, `publicar_control.sh s02 2` | Kafka guarda los 2 eventos; nadie los lee todavía |
| 4 | Se rearranca con el mismo comando y el mismo checkpoint | **Mismo id** en los dos queries. La numeración de batches sigue (no vuelve a 0). Solo sale `s02` (ventana 19:25–19:26, count 2); `s01` no se vuelve a emitir |
| 5 | `publicar_trampas.sh` con el query en marcha | `temp-nulos` muestra `s09` con `valor` NULL. `temp-stream` cuenta una sola ventana de `s09` (la actual) |
| 6 | Se borra el checkpoint y se arranca otra vez | **Id nuevo** (`880af9e9…`). Vuelve a leer desde `earliest` y emite otra vez `s01` con count 3 |

Datos de DataLab (rellenar al hacer el ensayo allí):

| Arranque | id `temp-stream` | id `temp-nulos` | ¿Reprocesa `s01`? |
| --- | --- | --- | --- |
| Primero | | | — |
| Rearranque | | | |

## Respuestas

**1. ¿En qué se diferencia `temp-stream` de `job_01`?**
`spark/job_01.py` es un job batch: lee un Parquet que ya está completo, calcula
la cobertura por década y termina con `exit=0`. `temp-stream` no termina
nunca: cada 5 segundos mira si hay offsets nuevos en Kafka, procesa solo esos
eventos (un micro-batch) y actualiza el resultado. Por eso `job_01` no
necesita checkpoint (si falla, se vuelve a ejecutar entero) y `temp-stream`
sí, porque tiene que saber por dónde iba y qué llevaba contado.

**2. ¿Qué guarda el checkpoint?**
Dos cosas distintas:
- **Progreso**: `offsets/N` es lo que el batch N va a leer
  (`{"sensores.temp":{"0":5}}` en el batch 2) y `commits/N` confirma que lo
  terminó. Con eso, al rearrancar, Spark sigue en el offset 5 y no vuelve
  a 0. `offsets/` guarda también la watermark (`batchWatermarkMs`).
- **Estado**: `state/` contiene los conteos parciales de cada
  (ventana, sensor) abierta. Es lo que permite que un evento nuevo de una
  ventana ya vista sume al conteo anterior en lugar de empezar de 1.

`metadata` guarda el id del query: por eso el id es el mismo tras rearrancar.
`temp-nulos` tiene `offsets/` y `commits/`, pero no `state/`, porque solo
filtra filas y no agrega nada.

**3. ¿Qué pasa con `startingOffsets` al rearrancar?**
Solo cuenta la primera vez. En el paso 4 el query arranca con
`startingOffsets=earliest`, pero no relee `s01`: manda el progreso del
checkpoint. En el paso 6, sin checkpoint, sí vuelve a leer desde el
principio. Borrar el checkpoint convierte una reanudación en un primer día.

**4. ¿Qué pasa con los eventos que llegan con el query parado?**
No se pierden: se quedan en Kafka. Al rearrancar, el primer batch lee desde
el último offset confirmado hasta el final (los 2 eventos de `s02`). Si la
parada es larga, el límite es la retención del topic, no Spark.

**5. Reto: eventos trampa y `temp-nulos`.**
- **Evento tardío** (`ts` de hace 20 minutos, 19:05:44). La watermark era de
  unos 19:15, es decir, el `ts` máximo visto menos 10 minutos. La ventana
  19:05–19:06 ya estaba cerrada, así que Spark descarta el evento: no aparece
  en `temp-stream` ni crea estado nuevo.
- **Esquema roto** (`temp` en vez de `valor`). `from_json` no falla: deja
  `valor` a nulo y conserva `sensor` y `ts`. Por eso aparece en `temp-nulos`
  y **también** en `temp-stream` (ventana 19:25–19:26, `s09`, count 1),
  porque `count()` cuenta filas, no valores.
- `temp-nulos` tiene su propio checkpoint (`~/checkpoints/temp-nulos`). Si
  compartiera el de `temp-stream`, los dos queries pisarían el mismo
  `offsets/` y `metadata`, y ninguno podría reanudar bien.

## Incidencia corregida

Con el manejador de Ctrl+C que instala PySpark, la parada lanzaba un
`Py4JError` en lugar de `KeyboardInterrupt`, y `stop()` fallaba con trazas.
El checkpoint se conservaba igual, pero la parada no era limpia. Se restaura
el manejador de Python (`signal.default_int_handler`) justo después de crear
la sesión, y ahora Ctrl+C para los dos queries sin errores.
