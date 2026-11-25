# Encargo de la sesión 19 (18/11/2026)

Se entrega en **vuestro** repositorio **antes** de la clase del
**23/11/2026**. El 23/11 se revisa en aula (`pytest tests/s19`).

## A. Bajad lo nuevo

```bash
cd bigdata2026 && git pull --ff-only
mkdir -p ../vuestro-repo/streaming
cp streaming/*.py streaming/*.sh ../vuestro-repo/streaming/
```

## B. Rellenad `streaming/temp_stream.py` y probadlo en DataLab

El fichero tiene seis huecos marcados `TODO`. Cada uno explica qué poner.

| Hueco | Qué hace |
| --- | --- |
| T1 | Leer de Kafka (`127.0.0.1:9092`, topic `sensores.temp`) |
| T2 | Convertir el mensaje JSON en columnas `sensor`, `valor`, `ts` |
| T3 | Contar eventos por sensor y por minuto |
| T4 | Mostrar en consola y guardar el checkpoint en `~/checkpoints/temp-stream` |
| T5 | Quedarse esperando y parar limpio con Ctrl+C |
| T6 | Reto: un segundo query, `temp-nulos`, con su propio checkpoint |

Lanzadlo en DataLab:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
  streaming/temp_stream.py
```

Y haced la prueba de parar y arrancar:

1. Arrancad y mirad la consola.
2. Ctrl+C.
3. Con el programa parado: `bash streaming/publicar_control.sh`
4. Arrancad otra vez con el mismo comando. Si salen los eventos del
   paso 3 sin repetir todo lo anterior, el checkpoint funciona.

## C. Escribid `ACTA_S19.md` y pasad el test

`ACTA_S19.md`, una pantalla, cinco respuestas cortas:

1. ¿Qué hace vuestro `spark/job_01.py` y por qué termina?
2. ¿Qué hace `temp-stream` y por qué no termina?
3. ¿Qué guarda el checkpoint? (progreso y estado)
4. ¿Qué pasó en la prueba de parar y arrancar?
5. Reto: ¿qué pasó con los dos eventos trampa? ¿Por qué `temp-nulos`
   necesita su propio checkpoint?

Test, desde el clon de este repo:

```bash
pytest -q tests/s19 --repo=../vuestro-repo
```

Pegad la salida al final del acta. Si no pudisteis usar DataLab, decidlo
en el acta; no inventéis resultados.

Quien no tenga aún el Hito 1 o el Hito 2: siguen pendientes
(`pytest -q tests/hito1 --repo=...` y `tests/hito2`). Vuestro arreglo
está en el `README.md`, sección «Problemas y arreglos».
