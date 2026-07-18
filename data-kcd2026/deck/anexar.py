#!/usr/bin/env python3
"""Anexos del deck KCD 2026: paginacion por Chrome headless.

Un .pptx no puede contener HTML vivo, y rasterizar paginas enteras en slides
las vuelve ilegibles. La arquitectura es: en el PPTX, un slide-cover compacto
por anexo con el preview de la primera pagina; el HTML COMPLETO paginado por
Chrome (texto seleccionable) vive en cada `aN.pdf` como anexo independiente.
El pptx presenta; cada aN.pdf documenta.

Orden del pipeline (build_estructura borra build/, por eso los anexos se
paginan despues de la estructura y antes de llenar):

    python3 build_estructura.py      # 1. estructura de 43 slides
    python3 anexar.py --paginas      # 2. HTML -> aN.pdf + preview-aN.png
    python3 llenar.py                # 3. contenido (los covers leen los previews)
    python3 render.py                # 4. pptx -> kcd2026.pdf + PNGs

Falla cerrado: fuente ausente, Chrome ausente o pieza faltante producen exit 1
con la accion correctiva; nunca un entregable parcial.
"""
import argparse
import subprocess
import sys
from pathlib import Path

CHROME = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
BASE = Path(__file__).resolve().parent
ANEXOS_DIR = BASE / "build" / "anexos"

# (clave, ruta HTML fuente, titulo humano) en el orden final.
ANEXOS = [
    ("a1",
     BASE.parents[2] / "edaiosv/docs/demos/edaios-operating-system.html",
     "EDAIOS: el sistema operativo completo (vista day-zero)"),
    ("a2",
     BASE.parent / "docs/edaios-operating-system-flink.html",
     "EDAIOS aplicado: la demo Flink hereda el gobierno de Core"),
    ("a3",
     BASE.parent / "docs/edaios-operating-system-flink-use-case.html",
     "El caso Flink completo: contrato, canary, evidencia"),
]


def fallar(mensaje: str) -> "None":
    print(f"[anexar] FAIL: {mensaje}", file=sys.stderr)
    raise SystemExit(1)


def paginas() -> int:
    """HTML -> PDF paginado por Chrome + PNG de la pagina 1 para los covers."""
    if not CHROME.is_file():
        fallar(f"Chrome no esta en {CHROME}; los anexos se paginan con Chrome "
               "headless y no hay sustituto declarado")
    faltantes = [str(html) for _, html, _ in ANEXOS if not html.is_file()]
    if faltantes:
        fallar("fuente(s) HTML inexistente(s):\n  - " + "\n  - ".join(faltantes)
               + "\n  Los anexos son derivados de esas vistas; sin fuente no hay anexo.")

    import fitz

    ANEXOS_DIR.mkdir(parents=True, exist_ok=True)
    for clave, html, titulo in ANEXOS:
        pdf = ANEXOS_DIR / f"{clave}.pdf"
        resultado = subprocess.run(
            [str(CHROME), "--headless", "--disable-gpu", "--no-sandbox",
             f"--print-to-pdf={pdf}", "--no-pdf-header-footer",
             html.resolve().as_uri()],
            capture_output=True, text=True,
        )
        if resultado.returncode != 0 or not pdf.is_file():
            fallar(f"Chrome no produjo {pdf} desde {html}\n{resultado.stderr.strip()}")
        doc = fitz.open(pdf)
        if doc.page_count == 0:
            fallar(f"{pdf} quedo sin paginas; la fuente {html} no rindio contenido")
        preview = ANEXOS_DIR / f"preview-{clave}.png"
        doc[0].get_pixmap(dpi=150).save(preview)
        print(f"[anexar] {clave}: {doc.page_count:3d} paginas  {pdf.name} + {preview.name}"
              f"  <- {html}")
        doc.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paginas", action="store_true",
                        help="pagina cada HTML con Chrome y exporta el preview de portada")
    parser.parse_args()
    return paginas()


if __name__ == "__main__":
    raise SystemExit(main())
