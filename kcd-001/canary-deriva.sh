#!/usr/bin/env bash
# Canary de deriva de contrato — KCD.
#
# Demuestra, en un solo comando y en menos de 30 segundos, que una deriva del
# contrato Avro NO llega a runtime: rompe la compilacion.
#
# Es reversible por construccion: un trap restaura el contrato pase lo que pase,
# incluso si se interrumpe con Ctrl-C.
#
#   ./canary-deriva.sh                 # variante por renombrado de campo
#   ./canary-deriva.sh --variante tipo # variante por cambio de tipo
#
# Deliberadamente SIN `set -e`: el fallo de compilacion del paso 2 es el
# resultado esperado, no un accidente.
set -uo pipefail

MODULO="kcd-001"
CONTRATO_REL="src/main/resources/model/order_event.avsc"

AQUI="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAIZ="$(cd "$AQUI/.." && pwd)"
CONTRATO="$AQUI/$CONTRATO_REL"
RESPALDO="$(mktemp)"

VARIANTE="renombrado"
[ "${1:-}" = "--variante" ] && VARIANTE="${2:-renombrado}"

rojo()  { printf '\033[31m%s\033[0m\n' "$*"; }
verde() { printf '\033[32m%s\033[0m\n' "$*"; }
gris()  { printf '\033[90m%s\033[0m\n' "$*"; }
titulo(){ printf '\n\033[1m%s\033[0m\n' "$*"; }

cp "$CONTRATO" "$RESPALDO"
restaurar() {
  cp "$RESPALDO" "$CONTRATO"
  rm -f "$RESPALDO"
}
trap restaurar EXIT INT TERM

compilar() { ( cd "$RAIZ" && ./mvnw -q -pl "$MODULO" compile 2>&1 ); }

SEGUNDOS_INICIO=$SECONDS

titulo "1 · Contrato integro"
if compilar >/dev/null; then
  verde "   compila — el job y el contrato estan de acuerdo"
else
  rojo "   la compilacion ya falla antes de empezar; el canary no puede demostrar nada"
  exit 1
fi

titulo "2 · Se introduce la deriva ($VARIANTE)"
if [ "$VARIANTE" = "tipo" ]; then
  gris "   totalPrice: double  ->  string"
  python3 - "$CONTRATO" <<'PY'
import sys, pathlib
p = pathlib.Path(sys.argv[1])
p.write_text(p.read_text().replace('"name": "totalPrice", "type": "double"',
                                   '"name": "totalPrice", "type": "string"'))
PY
else
  gris "   totalPrice  ->  totalPriceAmount   (un productor renombro el campo)"
  python3 - "$CONTRATO" <<'PY'
import sys, pathlib
p = pathlib.Path(sys.argv[1])
p.write_text(p.read_text().replace('"name": "totalPrice"', '"name": "totalPriceAmount"'))
PY
fi

titulo "3 · Se compila con el contrato derivado"
SALIDA="$(compilar)"
ESTADO=$?

if [ $ESTADO -eq 0 ]; then
  rojo "   LA COMPILACION PASO — el canary ha FALLADO como control."
  rojo "   Si esto ocurre, revisa que el job use los accesores generados"
  rojo "   (getTotalPrice) y no acceso dinamico por nombre."
  exit 1
fi

verde "   la compilacion FALLA — la deriva quedo atrapada en el build"
echo
echo "$SALIDA" | grep -E "ERROR.*\.java|symbol:|location:|incompatible types" | head -6 | sed 's/^/   /'
echo
gris "   No se produjo ningun artefacto ejecutable: el pipeline no llega a arrancar."

titulo "4 · Se restaura el contrato"
restaurar
trap - EXIT INT TERM
if compilar >/dev/null; then
  verde "   compila de nuevo — el repositorio queda como estaba"
else
  rojo "   la restauracion no dejo el build verde; revisa $CONTRATO_REL"
  exit 1
fi

TOTAL=$(( SECONDS - SEGUNDOS_INICIO ))
titulo "Canary completo en ${TOTAL}s"
gris "Umbral declarado en SC-004: 30s"
if [ "$TOTAL" -le 30 ]; then
  verde "dentro del umbral"
else
  rojo "por encima del umbral declarado"
  exit 1
fi
