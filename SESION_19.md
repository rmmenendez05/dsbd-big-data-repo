# Sesión 19: 18 de noviembre de 2026

Lo que hay que entregar: [`ENCARGO_S19.md`](ENCARGO_S19.md)
(plazo: antes del 23/11/2026; ese día se revisa).

## La idea de hoy

El `spark/job_01.py` del Hito 2 lee, calcula y **termina**.
Hoy escribís un programa Spark que **no termina**: lee de Kafka sin parar
y cuenta eventos de sensores por minuto.

Si lo paráis y lo volvéis a arrancar, ¿sabe por dónde iba? Solo si
guarda un **checkpoint** (un marcapáginas) en una carpeta que no cambia:
`~/checkpoints/temp-stream`.

## Lo que hacéis hoy

**A.** `git pull` de este repo y copiad `streaming/` a vuestro repo.

**B.** Rellenad los seis huecos de `streaming/temp_stream.py` y probad
en DataLab a pararlo y arrancarlo.

**C.** Escribid `ACTA_S19.md` y pasad `pytest -q tests/s19 --repo=...`.

El detalle de cada paso está en [`ENCARGO_S19.md`](ENCARGO_S19.md).

## Datos de DataLab

| Qué | Valor |
| --- | --- |
| Máquina | `admin@datalab` |
| Kafka | `127.0.0.1:9092`, topic `sensores.temp` |
| Checkpoint | `~/checkpoints/temp-stream` |
| Spark UI | `127.0.0.1:4040` (mientras el programa está vivo) |
| Mensaje de ejemplo | `{"sensor":"s01","valor":22.5,"ts":"2026-11-18T12:00:00Z"}` |

## Si aún no tenéis el Hito 1 o el Hito 2

Siguen pendientes. Desde el clon de este repo:

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest -q tests/hito1 --repo=../vuestro-repo
pytest -q tests/hito2 --repo=../vuestro-repo
```

Un solo fallo quiere decir que el hito no está cumplido. Qué le falta a
cada equipo: `README.md`, sección «Problemas y arreglos».
