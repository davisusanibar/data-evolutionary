#!/usr/bin/env python3
"""Genera la vista reducida del sistema operativo EDAIOS aplicada al caso Flink.

Es el "edaios-operating-system en miniatura" visto desde el consumer: la
constitución y los gates de Core verificados contra el pipeline data-kcd2026.
Vive aquí y no en Core porque ADR-0015 ubica las proyecciones renderizadas en
el consumer. El JSON conserva la narrativa; el HTML es derivado determinista y
no se edita a mano. Solo biblioteca estándar.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import sys
from pathlib import Path
from typing import Any, Iterable

CONFIG = Path("docs/edaios-operating-system-flink.config.json")
OUTPUT = Path("docs/edaios-operating-system-flink.html")

ROMANOS = ("I", "II", "III", "IV", "V", "VI", "VII")


class OsPageContractError(ValueError):
    """La fuente no satisface el contrato de la página."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise OsPageContractError(message)


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
        raise OsPageContractError(f"fuente inexistente: {path}") from exc
    try:
        data = mapping(json.loads(raw.decode("utf-8")), str(path))
    except json.JSONDecodeError as exc:
        raise OsPageContractError(f"JSON inválido en {path}: {exc}") from exc
    return data, hashlib.sha256(raw).hexdigest()


def validate(data: dict[str, Any]) -> None:
    require(data.get("schema") == "kcd2026.flink-os-page/v1",
            "schema de la página inválido")
    meta = mapping(data.get("meta"), "meta")
    for key in ("id", "lang", "eyebrow", "title", "subtitle", "status"):
        text(meta.get(key), f"meta.{key}")
    require(meta["lang"] == "es", "meta.lang debe ser es")
    lines(meta.get("badges"), "meta.badges", 3)

    jerarquia = mapping(data.get("jerarquia"), "jerarquia")
    for key in ("eyebrow", "title", "lead", "flecha"):
        text(jerarquia.get(key), f"jerarquia.{key}")
    lines(jerarquia.get("tree"), "jerarquia.tree", 5)

    constitucion = mapping(data.get("constitucion"), "constitucion")
    for key in ("eyebrow", "title", "lead", "nota_pin", "fuente"):
        text(constitucion.get(key), f"constitucion.{key}")
    articulos = sequence(constitucion.get("articulos"), "constitucion.articulos", 7)
    require(len(articulos) == 7, "constitucion.articulos debe declarar los 7 artículos")
    for index, item in enumerate(articulos):
        entry = mapping(item, f"constitucion.articulos[{index}]")
        require(entry.get("num") == ROMANOS[index],
                f"constitucion.articulos[{index}].num fuera de orden")
        for key in ("titulo", "regla", "evidencia"):
            text(entry.get(key), f"constitucion.articulos[{index}].{key}")
        require(entry.get("veredicto") in ("PASS", "N/A"),
                f"constitucion.articulos[{index}].veredicto debe ser PASS o N/A")

    controles = mapping(data.get("controles"), "controles")
    for key in ("eyebrow", "title", "lead", "core_titulo", "consumer_titulo",
                "mensaje", "fuente"):
        text(controles.get(key), f"controles.{key}")
    core_gates = lines(controles.get("core_gates"), "controles.core_gates", 15)
    require(len(core_gates) == 15, "controles.core_gates debe declarar los 15 gates de Core")
    consumer_gates = sequence(controles.get("consumer_gates"), "controles.consumer_gates", 2)
    require(len(consumer_gates) == 2, "controles.consumer_gates debe declarar los 2 gates")
    for item in consumer_gates:
        entry = mapping(item, "controles.consumer_gates[]")
        text(entry.get("id"), "controles.consumer_gates[].id")
        text(entry.get("descripcion"), "controles.consumer_gates[].descripcion")

    ciclo = mapping(data.get("ciclo"), "ciclo")
    for key in ("eyebrow", "title", "lead", "fuente"):
        text(ciclo.get(key), f"ciclo.{key}")
    specs = sequence(ciclo.get("specs"), "ciclo.specs", 2)
    require(len(specs) == 2, "ciclo.specs debe declarar las specs 001 y 002")
    for item in specs:
        entry = mapping(item, "ciclo.specs[]")
        for key in ("carpeta", "id", "estado", "fase", "tramo", "hipotesis"):
            text(entry.get(key), f"ciclo.specs[].{key}")
        lines(entry.get("produjo"), "ciclo.specs[].produjo", 2)

    adr = mapping(data.get("adr0015"), "adr0015")
    for key in ("eyebrow", "title", "estado", "cita", "detalle", "fuente"):
        text(adr.get(key), f"adr0015.{key}")

    evidencia = mapping(data.get("evidencia"), "evidencia")
    for key in ("eyebrow", "title", "lead", "canary_label", "e2e_label",
                "link_href", "link_texto", "fuente"):
        text(evidencia.get(key), f"evidencia.{key}")
    require(evidencia["link_href"] == "edaios-operating-system-flink-use-case.html",
            "evidencia.link_href debe enlazar la página larga del caso")
    lines(evidencia.get("canary_lines"), "evidencia.canary_lines", 3)
    lines(evidencia.get("e2e_lines"), "evidencia.e2e_lines", 2)

    frontera = mapping(data.get("frontera"), "frontera")
    for key in ("eyebrow", "title", "nota", "fuente"):
        text(frontera.get(key), f"frontera.{key}")
    lines(frontera.get("demuestra"), "frontera.demuestra", 4)
    lines(frontera.get("no_demuestra"), "frontera.no_demuestra", 4)

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


def bullets(values: Iterable[str], class_name: str = "") -> str:
    css = f' class="{esc(class_name)}"' if class_name else ""
    return f"<ul{css}>" + "".join(f"<li>{esc(value)}</li>" for value in values) + "</ul>"


def fuente(value: str) -> str:
    return f'<p class="fuente">{esc(value)}</p>'


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
.pill.na{background:#e4e8ee;color:var(--muted)}
.gates{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0 0;padding:0;list-style:none}
.gates li{background:var(--blue-soft);color:#0a5cb8;border-radius:999px;
padding:4px 10px;font-size:.75rem;font-weight:800;
font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
.fuente{color:var(--muted);font-size:.8rem;margin:6px 0 0}
.claims{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
.claims .card.no h3{color:var(--red)}
.claims .card.si h3{color:var(--green)}
.link-card{display:block;margin:18px 0 0;font-weight:700}
.footer{border-top:4px solid var(--kcd-blue);margin-top:44px;padding:26px 0 40px;
color:var(--muted);font-size:.86rem;background:var(--paper)}
.footer code{overflow-wrap:anywhere}
@media(max-width:860px){.grid-2,.claims{grid-template-columns:1fr}
table{min-width:480px}}
"""


def constitucion_table(articulos: list[dict[str, Any]]) -> str:
    head = "".join(f"<th>{esc(h)}</th>"
                   for h in ("#", "Artículo", "Regla (Core)", "Veredicto",
                             "Evidencia (pipeline Flink)"))
    rows = []
    for item in articulos:
        pill_class = "pill ok" if item["veredicto"] == "PASS" else "pill na"
        rows.append(
            "<tr>"
            f"<td>{esc(item['num'])}</td>"
            f"<td><b>{esc(item['titulo'])}</b></td>"
            f"<td>{esc(item['regla'])}</td>"
            f'<td><span class="{pill_class}">{esc(item["veredicto"])}</span></td>'
            f"<td>{esc(item['evidencia'])}</td>"
            "</tr>"
        )
    return (f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead>'
            f"<tbody>{''.join(rows)}</tbody></table></div>")


def render(data: dict[str, Any], config_digest: str) -> str:
    validate(data)
    meta = data["meta"]
    jerarquia = data["jerarquia"]
    constitucion = data["constitucion"]
    controles = data["controles"]
    ciclo = data["ciclo"]
    adr = data["adr0015"]
    evidencia = data["evidencia"]
    frontera = data["frontera"]

    hero_meta = "".join(f"<span>{esc(item)}</span>" for item in meta["badges"])
    core_gate_pills = ("<ul class=\"gates\">"
                       + "".join(f"<li>{esc(g)}</li>" for g in controles["core_gates"])
                       + "</ul>")
    consumer_cards = "".join(
        f'<article class="card"><h3><code>{esc(item["id"])}</code></h3>'
        f'<p>{esc(item["descripcion"])}</p></article>'
        for item in controles["consumer_gates"]
    )
    spec_cards = "".join(
        f'<article class="card"><h3><code>{esc(item["carpeta"])}</code></h3>'
        f'<p><span class="pill">{esc(item["id"])}</span> '
        f'<span class="pill">estado · {esc(item["estado"])}</span> '
        f'<span class="pill ok">fase · {esc(item["fase"])}</span> '
        f'<span class="pill">{esc(item["tramo"])}</span></p>'
        f'<p><b>Hipótesis de valor.</b> {esc(item["hipotesis"])}</p>'
        f'<p><b>Produjo:</b></p>{bullets(item["produjo"])}</article>'
        for item in ciclo["specs"]
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
<section id="jerarquia"><div class="shell">
<p class="eyebrow">{esc(jerarquia['eyebrow'])}</p>
<h2>{esc(jerarquia['title'])}</h2>
<p class="lead">{esc(jerarquia['lead'])}</p>
<pre class="tree">{esc(chr(10).join(jerarquia['tree']))}</pre>
<div class="callout"><b>La dirección de la flecha.</b> {esc(jerarquia['flecha'])}</div>
</div></section>
<section id="constitucion"><div class="shell">
<p class="eyebrow">{esc(constitucion['eyebrow'])}</p>
<h2>{esc(constitucion['title'])}</h2>
<p class="lead">{esc(constitucion['lead'])}</p>
{constitucion_table(constitucion['articulos'])}
<div class="boundary"><b>Frontera del pin.</b> {esc(constitucion['nota_pin'])}</div>
{fuente(constitucion['fuente'])}
</div></section>
<section id="controles"><div class="shell">
<p class="eyebrow">{esc(controles['eyebrow'])}</p>
<h2>{esc(controles['title'])}</h2>
<p class="lead">{esc(controles['lead'])}</p>
<article class="card"><h3>{esc(controles['core_titulo'])}</h3>{core_gate_pills}</article>
<h3>{esc(controles['consumer_titulo'])}</h3>
<div class="grid-2">{consumer_cards}</div>
<div class="callout"><b>El mensaje.</b> {esc(controles['mensaje'])}</div>
{fuente(controles['fuente'])}
</div></section>
<section id="ciclo"><div class="shell">
<p class="eyebrow">{esc(ciclo['eyebrow'])}</p>
<h2>{esc(ciclo['title'])}</h2>
<p class="lead">{esc(ciclo['lead'])}</p>
<div class="grid-2">{spec_cards}</div>
{fuente(ciclo['fuente'])}
</div></section>
<section id="adr-0015"><div class="shell">
<p class="eyebrow">{esc(adr['eyebrow'])}</p>
<h2>{esc(adr['title'])}</h2>
<article class="card">
<p><span class="pill ok">{esc(adr['estado'])}</span></p>
<div class="callout"><b>Cita textual.</b> «{esc(adr['cita'])}»</div>
<p>{esc(adr['detalle'])}</p>
{fuente(adr['fuente'])}
</article>
</div></section>
<section id="evidencia"><div class="shell">
<p class="eyebrow">{esc(evidencia['eyebrow'])}</p>
<h2>{esc(evidencia['title'])}</h2>
<p class="lead">{esc(evidencia['lead'])}</p>
{log_panel(evidencia['canary_label'], evidencia['canary_lines'])}
{log_panel(evidencia['e2e_label'], evidencia['e2e_lines'])}
<a class="link-card" href="{esc(evidencia['link_href'])}">{esc(evidencia['link_texto'])} →</a>
{fuente(evidencia['fuente'])}
</div></section>
<section id="frontera"><div class="shell">
<p class="eyebrow">{esc(frontera['eyebrow'])}</p>
<h2>{esc(frontera['title'])}</h2>
<div class="claims">
<article class="card si"><h3>Demuestra (entorno local)</h3>{bullets(frontera['demuestra'])}</article>
<article class="card no"><h3>No demuestra</h3>{bullets(frontera['no_demuestra'])}</article>
</div>
<div class="boundary"><b>Nota.</b> {esc(frontera['nota'])}</div>
{fuente(frontera['fuente'])}
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
        print("flink os view: 1/1 sincronizada; proyección determinista verificada")
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
    except OsPageContractError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
