# Encargo de la sesión 20 (23/11/2026)

Se entrega en **vuestro** repositorio: `DECISION_S20.md` en la raíz.
No hay que montar un pipeline nuevo.

## A. Bajad lo nuevo

```bash
cd bigdata2026 && git pull --ff-only
```

Hechos: `decision/casos.yaml`.
Forma del documento: `decision/plantilla_criterio.md`.

## B. Escribid `DECISION_S20.md`

Dos casos, mismas aulas:

| Caso | Qué pide el consumidor | Conclusión que hay que justificar |
| --- | --- | --- |
| A · Informe de las ocho | Consulta diaria a las 08:00; frescura de horas | **batch** |
| B · Alerta de aforo | Reacción en menos de 2 minutos | **streaming** |

En cada caso, al menos estos campos:

- `latencia_util`
- `frescura_admitida`
- `decision`
- `consumidor_continuo`
- `conclusion_arquitectura` (`batch` o `streaming`)
- `por_que_no_la_otra` (una frase)

Y las tres frases del consumidor: quién/cuándo; qué pasa si llega
tarde; qué haríais si llega antes (o «ninguna»).

No vale argumentar solo con «moderno», «tiempo real» o «ya hay Kafka».

Cada campo tiene que tener valor en los dos casos. Vale `campo: valor` o
`- **campo**: valor`. Las tres frases, con vuestras palabras: copiar los
títulos de la plantilla no cuenta.

## C. Pasad el test

Desde el clon de este repo:

```bash
pytest -q tests/s20 --repo=/ruta/a/vuestro/clon
```

Un fallo = el documento no cumple el contrato.

## Si la sesión 19 sigue abierta

[`ENCARGO_S19.md`](ENCARGO_S19.md) no se cancela. Hoy se revisa aparte.
