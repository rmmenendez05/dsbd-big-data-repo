# DECISION_S20 · ¿Batch o streaming para las aulas?

Hechos de partida: `decision/casos.yaml`. Los dos casos usan las mismas
12 aulas y los mismos eventos de ocupación (~180 eventos/hora en día
lectivo). Lo que cambia es **quién consume el resultado y cuándo**, no los
datos. En la sesión 19 vimos el coste de un query continuo: un proceso que
no termina, un checkpoint que hay que cuidar y un broker que tiene que
estar levantado. Ese coste solo compensa si alguien aprovecha la latencia.

## Caso A · Informe de ocupación de las 08:00

- **latencia_util**: horas; el informe se lee una vez al día a las 08:00, así que cualquier dato listo antes de esa hora vale lo mismo.
- **frescura_admitida**: horas; se aceptan datos hasta el cierre de la noche anterior.
- **decision**: job Spark batch programado (cron) en la ventana 02:00–06:00 que procesa los eventos del día anterior y deja el informe en Parquet/serving, como el pipeline del Hito 2.
- **consumidor_continuo**: no; hay una única consulta diaria a las 08:00 y nadie mira el resultado entre medias.
- **conclusion_arquitectura**: batch
- **por_que_no_la_otra**: un query Kafka + Structured Streaming 24/7 tendría recursos, broker y checkpoint vivos las 24 horas para producir un resultado que solo se lee a las 08:00, sin que nadie gane nada por tenerlo a las 03:00.

**Frases del consumidor**

1. Quién/cuándo: el equipo de gestión de espacios abre el informe a las 08:00 para repartir aulas de la mañana.
2. Si llega tarde: si el job no ha terminado a las 08:00, la gestión reparte a ciegas o con el informe de anteayer; el daño es un día de planificación peor, no un incidente. Con ~180 eventos/hora el volumen diario cabe de sobra en la ventana de 4 horas, y si falla hay margen para relanzarlo antes de las 08:00.
3. Si llega antes: ninguna acción; un informe listo a las 04:00 se usa igualmente a las 08:00, así que adelantarlo no cambia ninguna decisión.

## Caso B · Alerta de aforo

- **latencia_util**: menos de 2 minutos desde que el aula supera el aforo hasta que salta el aviso.
- **frescura_admitida**: minutos; un dato de hace una hora ya no sirve para echar a nadie de un aula llena.
- **decision**: query de Spark Structured Streaming leyendo de Kafka (como `temp-stream` de la S19), ventanas de 1 minuto por aula, trigger de segundos y checkpoint propio, encendido mientras el edificio está abierto.
- **consumidor_continuo**: sí; conserjería/seguridad tiene que recibir el aviso en cualquier momento del horario de apertura.
- **conclusion_arquitectura**: streaming
- **por_que_no_la_otra**: un batch nocturno (o incluso cada hora) avisaría de un aforo superado horas o decenas de minutos después, cuando la situación de riesgo ya ha pasado y el aviso no sirve de nada.

**Frases del consumidor**

1. Quién/cuándo: el personal de conserjería/seguridad, en el momento en que un aula supera el aforo durante el horario lectivo.
2. Si llega tarde: si el aviso llega pasados los 2 minutos, el aula sigue llena sin que nadie actúe; incumplimos el aforo, con riesgo para la evacuación y responsabilidad para el centro.
3. Si llega antes: ninguna acción adicional; antes de los 2 minutos simplemente se actúa antes, no hace falta bajar a segundos ni montar nada más.

Nota: no hace falta 24/7. Fuera del horario del edificio (de noche) no hay
consumidor, así que el query puede pararse y volver a arrancar por la mañana
con el mismo checkpoint, que es exactamente el ensayo de parar y arrancar
de la sesión 19.

## Resumen

Los mismos eventos piden dos arquitecturas distintas porque cambia el
consumidor: el informe se lee una vez a las 08:00 y tolera horas de retraso
(batch), mientras que la alerta solo sirve si llega en menos de 2 minutos
a alguien que está mirando (streaming durante el horario de apertura).
La propuesta de Kafka + Streaming 24/7 para el informe se descarta por
coste sin beneficio, no por la tecnología en sí.
