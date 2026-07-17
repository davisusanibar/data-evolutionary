#!/usr/bin/env python3
"""Fase 1: crea la secuencia de 43 slides duplicando los layouts del template KCD.

El template trae 8 slides, cada uno con su imagen de fondo:
  1 sponsors(fija) · 2 quote+onda · 3 titulo · 4 lienzo · 5 divisor · 6 bullets
  7 statement · 8 gracias(fija)

Duplicar un slide preserva su fondo y su layout. Aqui solo se construye la
estructura; el contenido lo llena build_contenido.py.
"""
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

SKILL = Path("/Users/dsusanibara/Library/Application Support/Claude/local-agent-mode-sessions/"
             "skills-plugin/5beefcaf-33e2-4a35-8a88-e35e333635de/"
             "41a08ec1-3f91-4ed8-a805-2ad9ca44f6a3/skills/pptx")
ADD_SLIDE = SKILL / "scripts/add_slide.py"
BASE = Path(__file__).parent

# (fuente_en_template, etiqueta) en el orden final del deck.
# 1=sponsors 2=quote 3=titulo 4=lienzo 5=divisor 6=bullets 7=statement 8=gracias
PLAN = [
    (1, "portada-sponsors"),
    (3, "titulo"),
    (6, "quienes-somos"),
    (7, "stmt-velocidad-sin-contrato"),
    (6, "patologia-pipeline"),
    (4, "dia-pipeline-con-sin-gobierno"),
    (6, "ia-deuda-a-escala"),
    (5, "div-que-es-edaios"),
    (6, "knowledge-first"),
    (7, "stmt-ia-consume"),
    (4, "dia-foundation-core-consumer"),
    (6, "siete-articulos"),
    (4, "dia-constitucion-canary"),
    (6, "nueve-invariantes"),
    (7, "stmt-fail-closed"),
    (4, "dia-evidence-vs-approval"),
    (6, "claim-boundary-const"),
    (5, "div-ciclo-sdd"),
    (4, "dia-ocho-fases"),
    (6, "specify-fr-sc"),
    (6, "plan-constitution-check"),
    (4, "dia-cadena-trazable"),
    (6, "quince-gates"),
    (5, "div-demo"),
    (6, "el-caso-kcd2026"),
    (4, "dia-arquitectura-demo"),
    (6, "spec-avro-contrato"),
    (4, "log-canary-contrato"),
    (6, "tres-bugs"),
    (4, "log-e2e-pipeline"),
    (5, "div-lo-que-descubrimos"),
    (4, "log-gate-rojo-adr"),
    (6, "frontera-claims-charla"),
    (7, "stmt-gate-verifica-humano-acepta"),
    # Anexos (spec 002, T015): divisor + 3 covers. Insertados ANTES de gracias
    # para que gracias siga siendo la ultima; solo la desplazan a la posicion
    # 39. Los contenido_* indexan por posicion y ninguno referencia gracias.
    (5, "div-anexos"),
    (4, "anexo-a1-os-dayzero"),
    (4, "anexo-a2-os-flink"),
    (4, "anexo-a3-use-case"),
    # Anexos A4/A5 (spec 002, T016): 4 capturas del owner sobre el layout
    # lienzo. Son evidencia visual aportada (deck/anexos/), jamas alterada.
    (4, "anexo-a4-flink-job"),
    (4, "anexo-a4-flink-overview"),
    (4, "anexo-a5-engram-obs"),
    (4, "anexo-a5-engram-detalle"),
    (8, "gracias"),
]


def main() -> int:
    work = BASE / "build"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir()
    unpacked = work / "unpacked"
    with zipfile.ZipFile(BASE / "template.pptx") as z:
        z.extractall(unpacked)

    # Los 8 originales quedan como banco de plantillas; se duplican y al final
    # se borran los que no entran en el plan.
    creados = []
    for origen, etiqueta in PLAN:
        salida = subprocess.run(
            [sys.executable, str(ADD_SLIDE), str(unpacked), f"slide{origen}.xml"],
            capture_output=True, text=True,
        )
        if salida.returncode != 0:
            print(salida.stderr, file=sys.stderr)
            return 1
        nuevo = re.search(r"Created ppt/slides/(slide\d+)\.xml", salida.stdout)
        creados.append((nuevo.group(1), etiqueta, origen))

    # Reordenar sldIdLst: solo los creados, en el orden del plan.
    pres = unpacked / "ppt/presentation.xml"
    texto = pres.read_text(encoding="utf-8")
    rels = (unpacked / "ppt/_rels/presentation.xml.rels").read_text(encoding="utf-8")

    rid_de = {}
    for m in re.finditer(r'Id="(rId\d+)"[^>]*Target="slides/(slide\d+)\.xml"', rels):
        rid_de[m.group(2)] = m.group(1)

    entradas = []
    for i, (slide, _, _) in enumerate(creados):
        entradas.append(f'<p:sldId id="{300+i}" r:id="{rid_de[slide]}"/>')
    nuevo_lst = "<p:sldIdLst>" + "".join(entradas) + "</p:sldIdLst>"
    texto = re.sub(r"<p:sldIdLst>.*?</p:sldIdLst>", nuevo_lst, texto, flags=re.S)
    pres.write_text(texto, encoding="utf-8")

    (work / "mapa.txt").write_text(
        "\n".join(f"{i+1:02d}  {s:12} {e:34} (de slide{o})"
                  for i, (s, e, o) in enumerate(creados)) + "\n", encoding="utf-8")

    # Limpiar los 8 originales, ya huerfanos del sldIdLst.
    subprocess.run([sys.executable, str(SKILL / "scripts/clean.py"), str(unpacked)],
                   capture_output=True, text=True)

    salida = work / "kcd2026.pptx"
    subprocess.run(["zip", "-Xqr", str(salida.resolve()), "."], cwd=unpacked, check=True)
    print(f"estructura: {len(creados)} slides -> {salida}")
    print((work / "mapa.txt").read_text())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
