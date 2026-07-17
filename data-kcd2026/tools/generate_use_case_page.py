#!/usr/bin/env python3
"""Genera la página del caso de uso Flink de EDAIOS como proyección determinista.

El JSON conserva la narrativa y los logs reales; el HTML es derivado
determinista y no se edita a mano. Solo biblioteca estándar.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import sys
from pathlib import Path
from typing import Any, Iterable

CONFIG = Path("docs/edaios-operating-system-flink-use-case.config.json")
OUTPUT = Path("docs/edaios-operating-system-flink-use-case.html")


class UseCaseContractError(ValueError):
    """La fuente no satisface el contrato de la página."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise UseCaseContractError(message)


def text(value: Any, label: str) -> str:
    require(isinstance(value, str) and value.strip(), f"{label} debe ser texto no vacío")
    return value.strip()


def mapping(value: Any, label: str) -> dict[str, Any]:
    require(isinstance(value, dict), f"{label} debe ser un objeto")
    return value


def sequence(value: Any, label: str, minimum: int = 1) -> list[Any]:
    require(isinstance(value, list) and len(value) >= minimum,
            f"{label} debe contener al menos {minimum} elemento(s)")
    return value


def lines(value: Any, label: str, minimum: int = 1) -> list[str]:
    raw = sequence(value, label, minimum)
    require(all(isinstance(item, str) for item in raw),
            f"{label} debe contener solo cadenas")
    return raw


def load_config(path: Path) -> tuple[dict[str, Any], str]:
    try:
        raw = path.read_bytes()
    except FileNotFoundError as exc:
        raise UseCaseContractError(f"fuente inexistente: {path}") from exc
    try:
        data = mapping(json.loads(raw.decode("utf-8")), str(path))
    except json.JSONDecodeError as exc:
        raise UseCaseContractError(f"JSON inválido en {path}: {exc}") from exc
    return data, hashlib.sha256(raw).hexdigest()


def validate(data: dict[str, Any]) -> None:
    require(data.get("schema") == "kcd2026.flink-use-case-page/v1",
            "schema de la página inválido")
    meta = mapping(data.get("meta"), "meta")
    for key in ("id", "lang", "eyebrow", "title", "subtitle", "event",
                "deck_note", "status"):
        text(meta.get(key), f"meta.{key}")
    require(meta["lang"] == "es", "meta.lang debe ser es")
    lines(meta.get("speakers"), "meta.speakers", 2)

    tesis = mapping(data.get("tesis"), "tesis")
    for key in ("eyebrow", "title", "lead", "hipotesis_valor", "paralelo_core"):
        text(tesis.get(key), f"tesis.{key}")

    patologia = mapping(data.get("patologia"), "patologia")
    for item in sequence(patologia.get("items"), "patologia.items", 3):
        entry = mapping(item, "patologia.items[]")
        text(entry.get("titulo"), "patologia.items[].titulo")
        text(entry.get("detalle"), "patologia.items[].detalle")

    arq = mapping(data.get("arquitectura"), "arquitectura")
    lines(arq.get("tree"), "arquitectura.tree", 5)
    for row in sequence(arq.get("contrato"), "arquitectura.contrato", 5):
        entry = mapping(row, "arquitectura.contrato[]")
        text(entry.get("clave"), "arquitectura.contrato[].clave")
        text(entry.get("valor"), "arquitectura.contrato[].valor")
    fr = sequence(arq.get("fr"), "arquitectura.fr", 7)
    require(len(fr) == 7, "arquitectura.fr debe declarar FR-001..FR-007")
    sc = sequence(arq.get("sc"), "arquitectura.sc", 6)
    require(len(sc) == 6, "arquitectura.sc debe declarar SC-001..SC-006")
    for index, item in enumerate(fr, start=1):
        entry = mapping(item, f"arquitectura.fr[{index}]")
        require(entry.get("id") == f"FR-{index:03d}", f"arquitectura.fr[{index}].id fuera de orden")
        text(entry.get("texto"), f"arquitectura.fr[{index}].texto")
    for index, item in enumerate(sc, start=1):
        entry = mapping(item, f"arquitectura.sc[{index}]")
        require(entry.get("id") == f"SC-{index:03d}", f"arquitectura.sc[{index}].id fuera de orden")
        text(entry.get("texto"), f"arquitectura.sc[{index}].texto")
        text(entry.get("verificado_por"), f"arquitectura.sc[{index}].verificado_por")
    lines(arq.get("clarifications"), "arquitectura.clarifications", 4)
    text(arq.get("estado_tests"), "arquitectura.estado_tests")

    canary = mapping(data.get("canary"), "canary")
    text(canary.get("log_label"), "canary.log_label")
    lines(canary.get("log_lines"), "canary.log_lines", 5)
    evidencia = mapping(canary.get("evidencia"), "canary.evidencia")
    for key in ("titulo", "tarea", "comando", "deriva_mutacion", "veredicto",
                "claim_boundary"):
        text(evidencia.get(key), f"canary.evidencia.{key}")
    require(evidencia.get("estado_sano_exit") == 0, "canary: el estado sano debe salir 0")
    require(evidencia.get("deriva_exit") == 1, "canary: la deriva debe salir 1")
    for artefacto in sequence(evidencia.get("artefactos"), "canary.evidencia.artefactos", 3):
        entry = mapping(artefacto, "canary.evidencia.artefactos[]")
        text(entry.get("nombre"), "canary.evidencia.artefactos[].nombre")
        digest = text(entry.get("digest"), "canary.evidencia.artefactos[].digest")
        require(digest.startswith("sha256:"), "canary: digest sin prefijo sha256:")

    bugs = mapping(data.get("bugs"), "bugs")
    text(bugs.get("moraleja"), "bugs.moraleja")
    items = sequence(bugs.get("items"), "bugs.items", 3)
    require(len(items) == 3, "bugs.items debe declarar T014, T015 y T016")
    for index, item in enumerate(items, start=14):
        entry = mapping(item, f"bugs.items[T{index:03d}]")
        require(entry.get("tarea") == f"T{index:03d}", "bugs: tareas fuera de orden")
        for key in ("fr", "defecto", "sintoma", "correccion"):
            text(entry.get(key), f"bugs.items[T{index:03d}].{key}")

    e2e = mapping(data.get("e2e"), "e2e")
    for key in ("eyebrow", "title", "lead", "contrato_label", "salida_label",
                "claim_boundary"):
        text(e2e.get(key), f"e2e.{key}")
    lines(e2e.get("contrato_log"), "e2e.contrato_log", 7)
    lines(e2e.get("salida_lines"), "e2e.salida_lines", 2)
    for row in sequence(e2e.get("fixtures"), "e2e.fixtures", 4):
        entry = mapping(row, "e2e.fixtures[]")
        for key in ("o_orderkey", "o_custkey", "o_totalprice"):
            text(entry.get(key), f"e2e.fixtures[].{key}")
        require(isinstance(entry.get("nota"), str), "e2e.fixtures[].nota debe ser cadena")
    for lectura in sequence(e2e.get("lecturas"), "e2e.lecturas", 3):
        entry = mapping(lectura, "e2e.lecturas[]")
        text(entry.get("id"), "e2e.lecturas[].id")
        text(entry.get("texto"), "e2e.lecturas[].texto")

    adr = mapping(data.get("adr0015"), "adr0015")
    for key in ("eyebrow", "title", "lead", "decision", "gate_intro",
                "log_label", "frontera"):
        text(adr.get(key), f"adr0015.{key}")
    lines(adr.get("log_lines"), "adr0015.log_lines", 5)

    frontera = mapping(data.get("frontera"), "frontera")
    for key in ("eyebrow", "title", "nota"):
        text(frontera.get(key), f"frontera.{key}")
    lines(frontera.get("demuestra"), "frontera.demuestra", 4)
    lines(frontera.get("no_demuestra"), "frontera.no_demuestra", 5)

    footer = mapping(data.get("footer"), "footer")
    linea = text(footer.get("linea"), "footer.linea")
    require("Vista regenerable" in linea and "no se edita a mano" in linea,
            "footer.linea debe declarar la vista regenerable y la prohibición de edición manual")


def esc(value: Any) -> str:
    return html.escape(str(value), quote=True)


def log_panel(label: str, log_lines: Iterable[str]) -> str:
    body = esc("\n".join(log_lines))
    return (f'<figure class="log"><figcaption>{esc(label)}</figcaption>'
            f'<pre>{body}</pre></figure>')


def table(headers: Iterable[str], rows: Iterable[Iterable[str]]) -> str:
    head = "".join(f"<th>{esc(header)}</th>" for header in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{esc(cell)}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return (f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead>'
            f"<tbody>{body}</tbody></table></div>")


def bullets(values: Iterable[str], class_name: str = "") -> str:
    css = f' class="{esc(class_name)}"' if class_name else ""
    return f"<ul{css}>" + "".join(f"<li>{esc(value)}</li>" for value in values) + "</ul>"


CSS = """\
:root{--kcd-blue:#0F87FF;--kcd-gray:#EFEFEF;--ink:#262626;--muted:#5c6470;
--line:#d7dbe2;--paper:#ffffff;--panel:#1b2129;--panel-ink:#e8eef6;
--blue-soft:#e5f1ff;--amber:#c07600;--amber-soft:#fff4df;--red:#b3362e;
--green:#0a7a5c;--green-soft:#e7f6f0}
*{box-sizing:border-box}
body{margin:0;background:var(--kcd-gray);color:var(--ink);
font-family:system-ui,-apple-system,"Segoe UI",Arial,sans-serif;line-height:1.55}
.shell{width:min(1080px,calc(100% - 40px));margin:0 auto}
.hero{background:var(--kcd-blue);color:#fff;padding:56px 0 44px}
.hero .eyebrow{color:#dcecff}
.hero h1{margin:.25em 0;font-size:clamp(1.8rem,4.4vw,3.1rem);line-height:1.08;
letter-spacing:-.02em}
.hero .lead{color:#eaf4ff;font-size:clamp(1rem,2vw,1.2rem);max-width:62ch;margin:0}
.hero-meta{display:flex;gap:10px;flex-wrap:wrap;margin-top:22px}
.hero-meta span{background:rgba(255,255,255,.16);border:1px solid rgba(255,255,255,.35);
border-radius:999px;padding:6px 12px;font-size:.82rem;font-weight:700}
.eyebrow{margin:0 0 .35em;font-size:.78rem;font-weight:800;letter-spacing:.13em;
text-transform:uppercase;color:var(--kcd-blue)}
h2{margin:.1em 0 .4em;font-size:clamp(1.4rem,3vw,2.1rem);line-height:1.12;
letter-spacing:-.02em}
h3{margin:.2em 0 .4em}
section{padding:44px 0 8px}
section>.shell>p.lead{color:var(--muted);max-width:78ch;margin:0 0 18px}
.card{background:var(--paper);border:1px solid var(--line);border-radius:14px;
padding:20px 22px;box-shadow:0 10px 26px rgba(38,38,38,.06)}
.grid-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
.grid-2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
.log{margin:18px 0}
.log figcaption{font-size:.76rem;font-weight:800;letter-spacing:.09em;
text-transform:uppercase;color:var(--panel-ink);background:var(--panel);
border-radius:12px 12px 0 0;padding:10px 16px;border-bottom:1px solid #313a46}
.log pre{margin:0;background:var(--panel);color:var(--panel-ink);
border-radius:0 0 12px 12px;padding:16px 18px;overflow-x:auto;
font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
font-size:.85rem;line-height:1.5}
pre.tree{background:var(--panel);color:var(--panel-ink);border-radius:12px;
padding:16px 18px;overflow-x:auto;font-family:ui-monospace,SFMono-Regular,Menlo,
Consolas,monospace;font-size:.85rem;line-height:1.5}
.table-wrap{overflow-x:auto;background:var(--paper);border:1px solid var(--line);
border-radius:12px;margin:14px 0}
table{border-collapse:collapse;width:100%;min-width:560px}
th,td{text-align:left;vertical-align:top;border-bottom:1px solid var(--line);
padding:10px 14px}
th{background:var(--blue-soft);color:#0a5cb8;font-size:.74rem;letter-spacing:.07em;
text-transform:uppercase}
tr:last-child td{border-bottom:0}
code{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
background:#e4e8ee;border-radius:5px;padding:1px 5px;font-size:.9em}
.callout{border-left:5px solid var(--kcd-blue);background:var(--blue-soft);
border-radius:0 10px 10px 0;padding:14px 16px;margin:16px 0}
.boundary{border-left:5px solid var(--amber);background:var(--amber-soft);
border-radius:0 10px 10px 0;padding:14px 16px;margin:16px 0}
.pill{display:inline-block;background:var(--blue-soft);color:#0a5cb8;
border-radius:999px;padding:4px 10px;font-size:.75rem;font-weight:800}
.pill.ok{background:var(--green-soft);color:var(--green)}
.pill.fail{background:#fdeceb;color:var(--red)}
.digests li{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
font-size:.8rem;overflow-wrap:anywhere}
.claims{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
.claims .card.no h3{color:var(--red)}
.claims .card.si h3{color:var(--green)}
.footer{border-top:4px solid var(--kcd-blue);margin-top:44px;padding:26px 0 40px;
color:var(--muted);font-size:.86rem;background:var(--paper)}
.footer code{overflow-wrap:anywhere}
@media(max-width:860px){.grid-3,.grid-2,.claims{grid-template-columns:1fr}
table{min-width:480px}}
"""


def render(data: dict[str, Any], config_digest: str) -> str:
    validate(data)
    meta = data["meta"]
    tesis = data["tesis"]
    patologia = data["patologia"]
    arq = data["arquitectura"]
    canary = data["canary"]
    evidencia = canary["evidencia"]
    bugs = data["bugs"]
    e2e = data["e2e"]
    adr = data["adr0015"]
    frontera = data["frontera"]

    hero_meta = "".join(
        f"<span>{esc(item)}</span>"
        for item in ([meta["event"]] + list(meta["speakers"]) + [meta["status"]])
    )
    patologia_cards = "".join(
        f'<article class="card"><h3>{esc(item["titulo"])}</h3>'
        f'<p>{esc(item["detalle"])}</p></article>'
        for item in patologia["items"]
    )
    fr_table = table(("ID", "Requisito funcional"),
                     [(item["id"], item["texto"]) for item in arq["fr"]])
    sc_table = table(("ID", "Criterio de éxito", "Verificado por"),
                     [(item["id"], item["texto"], item["verificado_por"])
                      for item in arq["sc"]])
    contrato_table = table(("", ""),
                           [(row["clave"], row["valor"]) for row in arq["contrato"]])
    digests = bullets(
        [f'{item["nombre"]} · {item["digest"]}' for item in evidencia["artefactos"]],
        "digests",
    )
    bug_rows = table(
        ("Tarea", "FR", "Defecto", "Síntoma", "Corrección"),
        [(item["tarea"], item["fr"], item["defecto"], item["sintoma"],
          item["correccion"]) for item in bugs["items"]],
    )
    fixtures_table = table(
        ("o_orderkey", "o_custkey", "o_totalprice", "Nota"),
        [(row["o_orderkey"], row["o_custkey"], row["o_totalprice"], row["nota"])
         for row in e2e["fixtures"]],
    )
    lecturas = "".join(
        f'<article class="card"><span class="pill ok">{esc(item["id"])}</span>'
        f'<p>{esc(item["texto"])}</p></article>'
        for item in e2e["lecturas"]
    )

    return f"""<!DOCTYPE html>
<html lang="{esc(meta['lang'])}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(meta['title'])}</title>
<meta name="description" content="{esc(meta['subtitle'])}">
<style>
{CSS}</style>
</head>
<body>
<header class="hero"><div class="shell">
<p class="eyebrow">{esc(meta['eyebrow'])}</p>
<h1>{esc(meta['title'])}</h1>
<p class="lead">{esc(meta['subtitle'])}</p>
<div class="hero-meta">{hero_meta}</div>
</div></header>
<main>
<section id="tesis"><div class="shell">
<p class="eyebrow">{esc(tesis['eyebrow'])}</p>
<h2>{esc(tesis['title'])}</h2>
<p class="lead">{esc(tesis['lead'])}</p>
<div class="callout"><b>Hipótesis de valor.</b> {esc(tesis['hipotesis_valor'])}</div>
<p>{esc(tesis['paralelo_core'])}</p>
<p>{esc(meta['deck_note'])}</p>
</div></section>
<section id="patologia"><div class="shell">
<p class="eyebrow">{esc(patologia['eyebrow'])}</p>
<h2>{esc(patologia['title'])}</h2>
<div class="grid-3">{patologia_cards}</div>
</div></section>
<section id="arquitectura"><div class="shell">
<p class="eyebrow">{esc(arq['eyebrow'])}</p>
<h2>{esc(arq['title'])}</h2>
<p class="lead">{esc(arq['lead'])}</p>
<pre class="tree">{esc(chr(10).join(arq['tree']))}</pre>
<h3>Contrato</h3>
{contrato_table}
<h3>Requisitos funcionales (FR)</h3>
{fr_table}
<h3>Criterios de éxito (SC)</h3>
{sc_table}
<h3>Clarifications</h3>
{bullets(arq['clarifications'])}
<div class="callout"><b>Estado.</b> {esc(arq['estado_tests'])}</div>
</div></section>
<section id="canary"><div class="shell">
<p class="eyebrow">{esc(canary['eyebrow'])}</p>
<h2>{esc(canary['title'])}</h2>
<p class="lead">{esc(canary['lead'])}</p>
{log_panel(canary['log_label'], canary['log_lines'])}
<article class="card">
<h3>{esc(evidencia['titulo'])}</h3>
<p><span class="pill">{esc(evidencia['tarea'])}</span>
<span class="pill">{esc(' · '.join(evidencia['criterios']))}</span>
<span class="pill ok">estado sano · exit {evidencia['estado_sano_exit']}</span>
<span class="pill fail">deriva inducida · exit {evidencia['deriva_exit']}</span>
<span class="pill ok">veredicto · {esc(evidencia['veredicto'])}</span></p>
<p><b>Comando:</b> <code>{esc(evidencia['comando'])}</code></p>
<p><b>Mutación inducida:</b> {esc(evidencia['deriva_mutacion'])}.</p>
{digests}
<div class="boundary"><b>Límite del claim.</b> {esc(evidencia['claim_boundary'])}</div>
</article>
</div></section>
<section id="bugs"><div class="shell">
<p class="eyebrow">{esc(bugs['eyebrow'])}</p>
<h2>{esc(bugs['title'])}</h2>
<p class="lead">{esc(bugs['lead'])}</p>
{bug_rows}
<div class="callout"><b>Moraleja.</b> {esc(bugs['moraleja'])}</div>
</div></section>
<section id="e2e"><div class="shell">
<p class="eyebrow">{esc(e2e['eyebrow'])}</p>
<h2>{esc(e2e['title'])}</h2>
<p class="lead">{esc(e2e['lead'])}</p>
{log_panel(e2e['contrato_label'], e2e['contrato_log'])}
<h3>Fixtures producidos a <code>orders</code></h3>
{fixtures_table}
{log_panel(e2e['salida_label'], e2e['salida_lines'])}
<div class="grid-3">{lecturas}</div>
<div class="boundary"><b>Límite del claim.</b> {esc(e2e['claim_boundary'])}</div>
</div></section>
<section id="adr-0015"><div class="shell">
<p class="eyebrow">{esc(adr['eyebrow'])}</p>
<h2>{esc(adr['title'])}</h2>
<p class="lead">{esc(adr['lead'])}</p>
<div class="callout"><b>La decisión.</b> {esc(adr['decision'])}</div>
<p>{esc(adr['gate_intro'])}</p>
{log_panel(adr['log_label'], adr['log_lines'])}
<div class="boundary"><b>Frontera.</b> {esc(adr['frontera'])}</div>
</div></section>
<section id="frontera"><div class="shell">
<p class="eyebrow">{esc(frontera['eyebrow'])}</p>
<h2>{esc(frontera['title'])}</h2>
<div class="claims">
<article class="card si"><h3>Demuestra (compose local)</h3>{bullets(frontera['demuestra'])}</article>
<article class="card no"><h3>No demuestra</h3>{bullets(frontera['no_demuestra'])}</article>
</div>
<div class="boundary"><b>Nota.</b> {esc(frontera['nota'])}</div>
</div></section>
</main>
<footer class="footer"><div class="shell">
<p><strong>{esc(data['footer']['linea'])}</strong></p>
<p>Config <code>sha256:{esc(config_digest)}</code></p>
</div></footer>
</body>
</html>
"""


def write(root: Path, check: bool) -> int:
    data, config_digest = load_config(root / CONFIG)
    content = render(data, config_digest)
    destination = root / OUTPUT
    if check:
        if not destination.exists() or destination.read_text(encoding="utf-8") != content:
            print(f"ERROR: derivado fuera de sincronía: {OUTPUT}", file=sys.stderr)
            return 1
        print("flink use case: 1/1 sincronizada; proyección determinista verificada")
        return 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8", newline="\n")
    print(f"generated {OUTPUT}")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", default=None,
                        help="raíz del módulo data-kcd2026 (por defecto, el padre de tools/)")
    parser.add_argument("--check", action="store_true",
                        help="falla si el HTML comprometido difiere de la recompilación")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = (Path(args.root).resolve() if args.root
            else Path(__file__).resolve().parent.parent)
    try:
        return write(root, args.check)
    except UseCaseContractError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
