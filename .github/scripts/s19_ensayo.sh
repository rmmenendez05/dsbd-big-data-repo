#!/usr/bin/env bash
# Sesión 19 · ensayo de parar y arrancar temp-stream en CI.
#
# Reproduce lo que se hace a mano en DataLab, contra un Kafka de pruebas:
#   1. eventos de s01 → primer arranque → Ctrl+C
#   2. con el query parado: eventos de s02 (publicar_control.sh)
#   3. segundo arranque con el mismo checkpoint: mismo id, solo lee s02
#      → eventos trampa (publicar_trampas.sh) → Ctrl+C
#   4. se borra el checkpoint → tercer arranque: id nuevo y vuelve a leerlo todo
#
# Necesita spark-submit y kafka-console-producer.sh en el PATH.
# Variables: PACKAGES (conector Kafka), OUT (carpeta de logs).
set -euo pipefail

: "${PACKAGES:?Falta PACKAGES (p. ej. org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9)}"
OUT="${OUT:-ensayo}"
CK_STREAM="$HOME/checkpoints/temp-stream"
CK_NULOS="$HOME/checkpoints/temp-nulos"
export PYTHONUNBUFFERED=1   # que los print() del driver lleguen al log al momento

mkdir -p "$OUT"
PID=""
FALLOS=0

fallo() { echo "::error::$*"; FALLOS=$((FALLOS + 1)); }
ok()    { echo "✔ $*"; }

arrancar() {  # $1 = nombre del log
  LOG="$OUT/$1.log"
  echo "── arrancando temp_stream.py → $LOG"
  # setsid: el proceso tiene su propio grupo, como en una terminal
  setsid spark-submit --packages "$PACKAGES" streaming/temp_stream.py >"$LOG" 2>&1 &
  PID=$!
  esperar "query=temp-nulos id=" 300
}

esperar() {  # $1 = patrón (grep -E) en $LOG, $2 = segundos
  local patron=$1 segundos=$2
  for _ in $(seq "$segundos"); do
    grep -Eq -- "$patron" "$LOG" && return 0
    if ! kill -0 "$PID" 2>/dev/null; then
      echo "::error::spark-submit terminó antes de ver /$patron/"
      tail -80 "$LOG"
      exit 1
    fi
    sleep 1
  done
  echo "::error::Timeout ($segundos s) esperando /$patron/ en $LOG"
  tail -80 "$LOG"
  exit 1
}

parar() {  # Ctrl+C al driver de Python, como en la terminal de DataLab
  local driver=""
  for p in $(pgrep -f 'streaming/temp_stream\.py' || true); do
    case "$(ps -o comm= -p "$p")" in python*) driver=$p ;; esac
  done
  echo "── Ctrl+C (SIGINT) al driver ${driver:-<no encontrado, al grupo>}"
  if [[ -n $driver ]]; then kill -INT "$driver"; else kill -INT -- "-$PID"; fi
  if ! timeout 120 tail --pid="$PID" -f /dev/null; then
    echo "::warning::No paró en 120 s; se mata el grupo"
    kill -KILL -- "-$PID" 2>/dev/null || true
  fi
  wait "$PID" 2>/dev/null || true
}

id_de() {  # $1 = query, $2 = log
  grep -Eo "query=$1 id=[0-9a-f-]+" "$2" | head -1 | cut -d= -f3
}

# Filas de la consola: temp-nulos = |sensor|valor|ts|, temp-stream = |window|sensor|count|
FILA_NULO_S09='\|s09 *\|null *\|'
FILA_CONTEO() { echo "\\}\\|$1 *\\|[0-9]+ *\\|"; }

# ── 1. Primer día ─────────────────────────────────────────────────────────
bash streaming/publicar_control.sh s01 3
arrancar 1_primer_arranque
esperar "$(FILA_CONTEO s01)" 180
sleep 6   # que el batch quede confirmado en commits/
parar
ID_STREAM_1=$(id_de temp-stream "$OUT/1_primer_arranque.log")
ID_NULOS_1=$(id_de temp-nulos "$OUT/1_primer_arranque.log")

for d in offsets commits state; do
  [[ -d $CK_STREAM/$d ]] && ok "temp-stream tiene $d/" || fallo "temp-stream sin $d/ tras parar"
done
for d in offsets commits; do
  [[ -d $CK_NULOS/$d ]] && ok "temp-nulos tiene $d/" || fallo "temp-nulos sin $d/ tras parar"
done

# ── 2. Eventos mientras está parado ───────────────────────────────────────
bash streaming/publicar_control.sh s02 2

# ── 3. Rearranque con el mismo checkpoint ─────────────────────────────────
arrancar 2_rearranque
LOG2="$OUT/2_rearranque.log"
esperar "$(FILA_CONTEO s02)" 180
ID_STREAM_2=$(id_de temp-stream "$LOG2")
ID_NULOS_2=$(id_de temp-nulos "$LOG2")

[[ $ID_STREAM_1 == "$ID_STREAM_2" ]] && ok "temp-stream conserva su id ($ID_STREAM_1)" \
  || fallo "temp-stream cambió de id: $ID_STREAM_1 → $ID_STREAM_2"
[[ $ID_NULOS_1 == "$ID_NULOS_2" ]] && ok "temp-nulos conserva su id ($ID_NULOS_1)" \
  || fallo "temp-nulos cambió de id: $ID_NULOS_1 → $ID_NULOS_2"

# Trampas: una tardía (hace 20 min) y una con esquema roto ("temp" en vez de "valor")
bash streaming/publicar_trampas.sh
esperar "$FILA_NULO_S09" 120
sleep 12  # dos triggers más, por si el conteo sale en otro batch
parar

grep -Eq "$(FILA_CONTEO s01)" "$LOG2" \
  && fallo "el rearranque volvió a emitir s01: ha releído el topic" \
  || ok "el rearranque no reprocesa s01 (progreso conservado)"
ok "temp-nulos muestra el evento con esquema roto (s09, valor null)"
N_S09=$(grep -Ec "$(FILA_CONTEO s09)" "$LOG2" || true)
[[ $N_S09 -eq 1 ]] && ok "el evento tardío de s09 queda fuera de la watermark (1 ventana de s09, no 2)" \
  || fallo "se esperaba 1 ventana de s09 en temp-stream y hay $N_S09 (¿la tardía entró?)"

cp -r "$HOME/checkpoints" "$OUT/checkpoints_tras_rearranque"

# ── 4. Contraste: sin checkpoint, cada arranque es un primer día ──────────
rm -rf "$CK_STREAM" "$CK_NULOS"
arrancar 3_sin_checkpoint
esperar "$(FILA_CONTEO s01)" 180
parar
ID_STREAM_3=$(id_de temp-stream "$OUT/3_sin_checkpoint.log")
[[ $ID_STREAM_3 != "$ID_STREAM_1" ]] && ok "sin checkpoint, temp-stream tiene id nuevo ($ID_STREAM_3)" \
  || fallo "sin checkpoint el id debería cambiar y sigue siendo $ID_STREAM_1"
ok "sin checkpoint vuelve a leer desde earliest (s01 reaparece)"

# ── Resumen ───────────────────────────────────────────────────────────────
{
  echo "### S19 · ensayo de parar y arrancar"
  echo
  echo "| Arranque | id temp-stream | id temp-nulos |"
  echo "| --- | --- | --- |"
  echo "| 1. primer arranque | \`$ID_STREAM_1\` | \`$ID_NULOS_1\` |"
  echo "| 2. rearranque (mismo checkpoint) | \`$ID_STREAM_2\` | \`$ID_NULOS_2\` |"
  echo "| 3. checkpoint borrado | \`$ID_STREAM_3\` | \`$(id_de temp-nulos "$OUT/3_sin_checkpoint.log")\` |"
  echo
  echo "Comprobaciones fallidas: **$FALLOS**. Los logs y el checkpoint están en el artefacto \`ensayo-s19\`."
} | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"

exit $((FALLOS > 0))
