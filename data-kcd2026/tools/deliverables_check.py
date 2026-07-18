#!/usr/bin/env python3
"""Gate de entregables de conferencia: el deck y las paginas no derivan de su fuente.

Verifica los entregables de la spec 002 como proyecciones regenerables:

    deck/template.pptx        pinado por sha256 en feature.spec.yaml (FR-001)
    deck/build/kcd2026.pptx   proyeccion renderizada: verificacion ESTRUCTURAL
    docs/...use-case.html     proyeccion de texto: byte a byte via --check
    docs/...-flink.html       vista reducida del OS: byte a byte via --check

El .pptx NO se compara byte a byte contra un digest esperado: es un contenedor
ZIP cuyos timestamps y orden de entradas varian entre corridas (ADR-0015,
invariante 5). El sustituto honesto es estructural: cero placeholders del
template, extractos de log identicos a la evidencia registrada, slide de
frontera presente y guion del orador en el umbral declarado.

Uso:
    python3 tools/deliverables_check.py [root] [--pptx RUTA]

Salida distinta de cero = entregable derivado o no verificable. No hay modo
"warning": una ausencia de evidencia no se interpreta como aprobacion.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml
from pptx import Presentation

SPEC_YAML = Path("specs/002-conference-deck-and-use-case-page/feature.spec.yaml")
MAPA = Path("deck/build/mapa.txt")

# Cada pagina de texto declara su contrato en feature.spec.yaml (clave, generador
# esperado, etiqueta humana). El --check del generador se propaga tal cual: byte
# a byte o rojo.
PAGES = (
    ("page", Path("tools/generate_use_case_page.py"), "pagina del caso"),
    ("page_os", Path("tools/generate_os_flink_page.py"), "vista reducida del OS"),
)
# Orden completo del pipeline: los anexos se paginan tras la estructura
# (build_estructura borra build/) y el deck se renderiza al final.
REBUILD = ("cd deck && python3 build_estructura.py && python3 anexar.py --paginas "
           "&& python3 llenar.py && python3 render.py")

# Textos placeholder de los 8 slides del template oficial KCD. Un rebuild
# completo no deja ninguno; una sola ocurrencia delata contenido sin llenar.
PLACEHOLDER_SUBSTRINGS = [
    "Title left aligned",
    "Bullet one",
    "Bullet two",
    "Bullet three",
    "Bullet four",
    "Bullet five",
    "Lorem ipsum",
    "A longer quote can go here",
    "Simple statement or quote goes here",
    "César Lorca Bacian",
    "SUSE Presales Engineer",
    "Kubernetes a Escala Real",
]
# "Texto" solo delata placeholder si es el texto integro de una forma.
PLACEHOLDER_EXACT = ["Texto"]


class DeliverablesError(Exception):
    """El entregable no es verificable en el estado actual del arbol."""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_contract(root: Path) -> dict:
    path = root / SPEC_YAML
    if not path.is_file():
        raise DeliverablesError(f"contrato inexistente: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    for key in ("contract", "log_extracts"):
        if key not in data:
            raise DeliverablesError(f"{SPEC_YAML}: falta la clave obligatoria {key!r}")
    return data


def resolve(root: Path, declared: str) -> Path:
    """Las rutas del contrato llevan el prefijo del modulo (data-kcd2026/...)."""
    candidate = root.parent / declared
    if candidate.exists():
        return candidate
    return root / declared


def slide_texts(slide) -> list[str]:
    return [shape.text_frame.text for shape in slide.shapes if shape.has_text_frame]


def check_template_digest(root: Path, deck: dict) -> list[str]:
    """SC-001: el template oficial se pina por sha256; TBD-DIGEST exige fijarlo."""
    template = resolve(root, deck["template"])
    if not template.is_file():
        return [f"template declarado inexistente: {deck['template']}"]
    actual = sha256_of(template)
    declared = deck.get("template_sha256")
    if declared == "TBD-DIGEST":
        return [
            "el pin del template sigue en TBD-DIGEST; digest real calculado: "
            f"sha256:{actual} — fija ese valor en {SPEC_YAML} (template_sha256) "
            "para que el pin quede verificable"
        ]
    if declared != actual:
        return [
            "el template no coincide con su pin: declarado sha256:"
            f"{declared} vs real sha256:{actual}; si el cambio del template es "
            "deliberado, actualiza el pin con la decision registrada — si no lo "
            "es, restaura deck/template.pptx"
        ]
    return []


def load_deck(root: Path, deck: dict, override: Path | None) -> Presentation:
    path = override if override else resolve(root, deck["artifact"])
    if not path.is_file():
        raise DeliverablesError(
            f"el deck no esta construido ({path}); ejecuta el rebuild completo: {REBUILD}"
        )
    return Presentation(str(path))


def check_slide_count(pres: Presentation, deck: dict) -> list[str]:
    expected = int(deck["slides"])
    actual = len(pres.slides)
    if actual != expected:
        return [f"el deck tiene {actual} slides y el contrato declara {expected}; "
                f"regenera con: {REBUILD}"]
    return []


def check_no_placeholders(pres: Presentation) -> list[str]:
    """SC-002: cero restos de texto placeholder del template."""
    problems = []
    for index, slide in enumerate(pres.slides, start=1):
        texts = slide_texts(slide)
        joined = "\n".join(texts)
        for marker in PLACEHOLDER_SUBSTRINGS:
            if marker in joined:
                problems.append(
                    f"slide {index}: placeholder del template sin llenar: {marker!r}"
                )
        for marker in PLACEHOLDER_EXACT:
            if any(text.strip() == marker for text in texts):
                problems.append(
                    f"slide {index}: forma con el placeholder exacto {marker!r}"
                )
    return problems


def load_mapa(root: Path) -> dict[str, int]:
    """Etiqueta -> posicion (1-based) segun el mapa que emite build_estructura."""
    path = root / MAPA
    if not path.is_file():
        raise DeliverablesError(
            f"no existe {MAPA}; el mapa de slides se emite en el rebuild: {REBUILD}"
        )
    mapping: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^(\d+)\s+slide\d+\s+(\S+)", line)
        if match:
            mapping[match.group(2)] = int(match.group(1))
    if not mapping:
        raise DeliverablesError(f"{MAPA} no contiene entradas interpretables")
    return mapping


def format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def field_variants(field: str, value: Any) -> list[str]:
    rendered = format_value(value)
    return [
        f'"{field}":{rendered}',
        f'"{field}": {rendered}',
        f"'{field}': {rendered}",
        f"'{field}':{rendered}",
    ]


def evidence_contains(node: Any, field: str, value: Any) -> bool:
    if isinstance(node, dict):
        if field in node and node[field] == value:
            return True
        return any(evidence_contains(item, field, value) for item in node.values())
    if isinstance(node, list):
        return any(evidence_contains(item, field, value) for item in node)
    return False


def check_log_extracts(root: Path, data: dict, pres: Presentation) -> list[str]:
    """SC-003: los extractos clave del deck coinciden con la evidencia registrada.

    Compara en dos direcciones: el extracto debe estar en el slide declarado y
    debe estar sostenido por el JSON de evidencia. Evidencia ausente = rojo;
    la ausencia no se interpreta como aprobacion.
    """
    mapa = load_mapa(root)
    slides = list(pres.slides)
    problems: list[str] = []
    for extract in data["log_extracts"]:
        label = extract["slide_label"]
        evidence_path = resolve(root, extract["evidence"])
        position = mapa.get(label)
        if position is None:
            problems.append(f"extracto {extract['id']}: la etiqueta {label!r} no "
                            f"aparece en {MAPA}")
            continue
        if position > len(slides):
            problems.append(f"extracto {extract['id']}: el mapa apunta al slide "
                            f"{position} y el deck tiene {len(slides)}")
            continue
        slide_text = "\n".join(slide_texts(slides[position - 1]))
        if not evidence_path.is_file():
            problems.append(
                f"extracto {extract['id']}: evidencia inexistente "
                f"{extract['evidence']}; registra la evidencia antes de reclamar "
                "coincidencia (una ausencia no aprueba)"
            )
            continue
        raw = evidence_path.read_text(encoding="utf-8")
        try:
            evidence = json.loads(raw)
        except json.JSONDecodeError as exc:
            problems.append(f"extracto {extract['id']}: {extract['evidence']} no es "
                            f"JSON valido: {exc}")
            continue
        for item in extract["must_match"]:
            if isinstance(item, str):
                if item not in slide_text:
                    problems.append(
                        f"slide {position} ({label}): el extracto {item!r} no "
                        "aparece en el slide; el deck derivo de la evidencia"
                    )
                if item not in raw:
                    problems.append(
                        f"extracto {extract['id']}: {item!r} no aparece en la "
                        f"evidencia {extract['evidence']}; el contrato cita algo "
                        "que la evidencia no registra"
                    )
            else:
                field, value = item["field"], item["value"]
                if not any(v in slide_text for v in field_variants(field, value)):
                    problems.append(
                        f"slide {position} ({label}): no muestra "
                        f"{field}={format_value(value)}; el deck derivo de la "
                        "evidencia"
                    )
                if not evidence_contains(evidence, field, value):
                    problems.append(
                        f"extracto {extract['id']}: la evidencia "
                        f"{extract['evidence']} no registra "
                        f"{field}={format_value(value)}"
                    )
    return problems


def check_boundary_slide(pres: Presentation, deck: dict) -> list[str]:
    """SC-004: el deck declara su frontera con el slide 'Que NO demostramos'."""
    title = deck["boundary_slide_title"]
    for slide in pres.slides:
        if any(text.strip() == title for text in slide_texts(slide)):
            return []
    return [f"no existe ningun slide con el titulo {title!r}; el deck no declara "
            "su frontera de claims (FR-004)"]


def check_speaker_notes(pres: Presentation, deck: dict) -> list[str]:
    """SC-005: guion del orador en al menos el umbral declarado de slides."""
    threshold = int(deck["min_slides_with_notes"])
    without: list[int] = []
    for index, slide in enumerate(pres.slides, start=1):
        has_note = (slide.has_notes_slide
                    and slide.notes_slide.notes_text_frame.text.strip())
        if not has_note:
            without.append(index)
    with_notes = len(pres.slides) - len(without)
    if with_notes < threshold:
        return [
            f"solo {with_notes}/{len(pres.slides)} slides llevan notas del orador "
            f"y el contrato exige {threshold}; sin nota: "
            + ", ".join(str(i) for i in without)
        ]
    return []


def check_annexes(root: Path, contract: dict, deck: dict) -> list[str]:
    """T015 (FR-007/FR-008): los anexos HTML viajan completos como aN.pdf.

    Verifica que las 3 fuentes HTML resuelvan y que los 3 PDFs paginados por
    Chrome existan y tengan paginas (texto seleccionable). Cada anexo es un PDF
    independiente; el deck lleva un cover con el preview de su primera pagina.
    El PDF no reclama determinismo byte a byte (los PDF de Chrome llevan
    metadatos de fecha): la verificacion es estructural, misma frontera que el
    .pptx (ADR-0015, invariante 5).
    """
    import fitz

    annexes = contract.get("annexes")
    if not isinstance(annexes, dict):
        return ["contract.annexes ausente del contrato: los anexos HTML del deck "
                "declaran fuente y PDF paginado"]
    problems: list[str] = []
    pdfs: list[Path] = []
    for index, item in enumerate(annexes.get("items", []), start=1):
        source = resolve(root, item.get("source_html", ""))
        if not source.is_file():
            problems.append(
                f"anexo {index}: la fuente HTML no resuelve: {item.get('source_html')}"
            )
        pdf = resolve(root, item.get("pdf_artifact", ""))
        if not pdf.is_file():
            problems.append(
                f"anexo {index}: PDF paginado inexistente ({item.get('pdf_artifact')}); "
                f"ejecuta el pipeline en orden: {REBUILD}"
            )
        else:
            pdfs.append(pdf)
    deck_pdf = resolve(root, deck["artifact"]).with_suffix(".pdf")
    if not deck_pdf.is_file():
        problems.append(f"el PDF del deck no existe ({deck_pdf}); "
                        f"ejecuta el pipeline en orden: {REBUILD}")
    if problems:
        return problems
    for index, pdf in enumerate(pdfs, start=1):
        if fitz.open(pdf).page_count == 0:
            problems.append(
                f"anexo {index}: {pdf.name} quedo sin paginas; la fuente HTML "
                "no rindio contenido"
            )
    return problems


def check_captures(root: Path, contract: dict) -> list[str]:
    """T016 (FR-007): las capturas del owner (anexos A4/A5) estan pineadas.

    Las capturas son evidencia visual aportada por el owner, no un derivado
    del pipeline: el generador las inserta tal cual y este gate las pina por
    sha256. Un archivo ausente o con digest distinto = captura reemplazada o
    ausente = rojo; la evidencia no se sustituye en silencio.
    """
    captures = contract.get("captures")
    if not isinstance(captures, list) or not captures:
        return ["contract.captures ausente del contrato: las capturas del owner "
                "(anexos A4/A5) declaran archivo y sha256"]
    problems: list[str] = []
    for item in captures:
        declared = item.get("file", "")
        pinned = item.get("sha256", "")
        path = resolve(root, declared)
        if not path.is_file():
            problems.append(
                f"captura reemplazada o ausente: {declared} no existe; las "
                "capturas del owner se copian tal cual a deck/anexos/ y no se "
                "regeneran — restaura el archivo original"
            )
            continue
        actual = sha256_of(path)
        if actual != pinned:
            problems.append(
                f"captura reemplazada o ausente: {declared} no coincide con su "
                f"pin (declarado sha256:{pinned} vs real sha256:{actual}); las "
                "capturas del owner son evidencia y no se alteran — restaura el "
                "archivo original o registra la decision antes de re-pinear"
            )
    return problems


def check_page_contract(root: Path, contract: dict) -> list[str]:
    """FR-006: fuente, generador y derivado de cada pagina existen donde el contrato dice."""
    problems = []
    for contract_key, expected_generator, _ in PAGES:
        page = contract.get(contract_key)
        if not isinstance(page, dict):
            problems.append(f"contract.{contract_key} ausente del contrato: cada pagina "
                            "de texto declara config, generator y artifact")
            continue
        for key in ("config", "generator", "artifact"):
            declared = page.get(key)
            if not declared or not resolve(root, declared).is_file():
                problems.append(f"contract.{contract_key}.{key} no resuelve: {declared}")
        declared_generator = page.get("generator", "")
        if declared_generator and not declared_generator.endswith(expected_generator.name):
            problems.append(
                f"contract.{contract_key}.generator declara {declared_generator} y este "
                f"gate ejecuta {expected_generator}; alinea el contrato con el gate"
            )
    return problems


def check_pages(root: Path) -> int:
    """SC-006 (FR-006): cada pagina HTML es byte-identica a su recompilacion."""
    for _, generator_rel, label in PAGES:
        generator = root / generator_rel
        if not generator.is_file():
            print(f"[deliverables] FAIL: generador de la {label} inexistente: "
                  f"{generator_rel}", file=sys.stderr)
            return 1
        result = subprocess.run(
            [sys.executable, str(generator), str(root), "--check"],
            capture_output=True, text=True,
        )
        if result.stdout.strip():
            print(f"[deliverables] {label}: {result.stdout.strip()}")
        if result.returncode != 0:
            print(f"[deliverables] FAIL: la {label} no coincide byte a byte "
                  "con su recompilacion", file=sys.stderr)
            if result.stderr.strip():
                print(f"  - {result.stderr.strip()}", file=sys.stderr)
            print(f"  Regenera con: python3 {generator_rel} "
                  "(el HTML no se edita a mano)", file=sys.stderr)
            return result.returncode
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", default=None,
                        help="raiz del modulo data-kcd2026 (por defecto, el padre de tools/)")
    parser.add_argument("--pptx", default=None,
                        help="ruta alternativa del deck construido (para verificar una copia)")
    args = parser.parse_args(argv)
    root = (Path(args.root).resolve() if args.root
            else Path(__file__).resolve().parent.parent)
    override = Path(args.pptx).resolve() if args.pptx else None

    try:
        data = load_contract(root)
        deck = data["contract"]["deck"]
        problems: list[str] = []
        problems += check_template_digest(root, deck)
        problems += check_page_contract(root, data["contract"])
        pres = load_deck(root, deck, override)
        problems += check_slide_count(pres, deck)
        problems += check_no_placeholders(pres)
        problems += check_log_extracts(root, data, pres)
        problems += check_boundary_slide(pres, deck)
        problems += check_speaker_notes(pres, deck)
        problems += check_annexes(root, data["contract"], deck)
        problems += check_captures(root, data["contract"])
    except DeliverablesError as exc:
        print(f"[deliverables] FAIL: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"[deliverables] FAIL: {exc}", file=sys.stderr)
        return 1

    if problems:
        print("[deliverables] FAIL: los entregables derivaron de su contrato",
              file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(
            "\n  La fuente manda. Corrige el generador o la evidencia y "
            f"regenera ({REBUILD}); no edites build/kcd2026.pptx a mano.",
            file=sys.stderr,
        )
        return 1

    page_exit = check_pages(root)
    if page_exit != 0:
        return page_exit

    print(f"[deliverables] OK: deck {deck['slides']} slides sin placeholders, "
          f"{len(data['log_extracts'])} extractos de log identicos a la evidencia, "
          "slide de frontera presente y guion del orador sobre el umbral")
    print(f"[deliverables] OK: {len(PAGES)} paginas byte-identicas a su "
          "recompilacion (caso de uso y vista reducida del OS)")
    annexes = data["contract"].get("annexes", {})
    print(f"[deliverables] OK: {len(annexes.get('items', []))} anexos HTML "
          "paginados presentes como aN.pdf independientes "
          "(verificacion estructural: un PDF de Chrome no ofrece bytes estables)")
    captures = data["contract"].get("captures", [])
    print(f"[deliverables] OK: {len(captures)} capturas del owner (anexos A4/A5) "
          "presentes y byte-identicas a su pin sha256 — evidencia visual "
          "aportada, nunca alterada")
    print("[deliverables] frontera: la verificacion del .pptx es ESTRUCTURAL, no "
          "byte a byte — un contenedor ZIP no ofrece igualdad de bytes entre "
          "corridas y este gate no la finge (ADR-0015, invariante 5)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
