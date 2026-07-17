#!/usr/bin/env python3
"""Renderiza build/kcd2026.pptx a PDF y PNGs por slide (build/render/s-NN.png)."""
import os, pathlib, shutil, subprocess
BASE = pathlib.Path(__file__).parent
os.environ["PATH"] = "/Applications/LibreOffice.app/Contents/MacOS:" + os.environ["PATH"]
build = BASE / "build"; out = build / "render"
shutil.rmtree(out, ignore_errors=True); out.mkdir(parents=True)
subprocess.run(["soffice", "--headless", "--convert-to", "pdf", "--outdir",
                str(build), str(build / "kcd2026.pptx")], check=True, capture_output=True)
import fitz
doc = fitz.open(build / "kcd2026.pdf")
for i, page in enumerate(doc, 1):
    page.get_pixmap(dpi=85).save(out / f"s-{i:02d}.png")
print(f"render: {len(doc)} slides -> {out}")
