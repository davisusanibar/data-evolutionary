#!/usr/bin/env python3
"""Gate de contrato del pipeline: la especificacion manda sobre el esquema.

Verifica que tres representaciones del mismo contrato no hayan derivado entre si:

    data-contract.yaml (contrato de datos)  <-- la fuente
    spec.md            (FR-002, prosa)
    orders_revenue_window.avsc            (el esquema que ejecuta Flink)

El .avsc no es la verdad: es una representacion. Si alguien edita el esquema y no
la especificacion, este gate falla cerrado antes de compilar. Es el mismo patron
que EDAIOS Core aplica a su Constitucion, que no compila si Foundation dejo de
decir lo que la Constitucion afirma que dice.

Uso:
    python3 tools/contract_check.py [--root .]

Salida distinta de cero = contrato derivado. No hay modo "warning": una ausencia
de evidencia no se interpreta como aprobacion.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import yaml

MODULE = Path("data-kcd2026")
SPEC_DIR = MODULE / "specs/001-orders-revenue-window"
TYPED = SPEC_DIR / "data-contract.yaml"
PROSE = SPEC_DIR / "spec.md"
JOB = MODULE / "src/main/java/com/topaya/kcd2026/JobOrdersRevenueWindow.java"

# Literales que delatan un esquema Avro embebido en el codigo (SC-001).
EMBEDDED_SCHEMA = re.compile(r'"type"\s*:\s*"record"|SchemaBuilder|new\s+Schema\.Parser\(\)\.parse\(\s*"')


class ContractError(Exception):
    """El contrato derivo entre sus representaciones."""


def load_typed(root: Path) -> dict:
    data = yaml.safe_load((root / TYPED).read_text(encoding="utf-8"))
    for key in ("contract", "fields", "requirements"):
        if key not in data:
            raise ContractError(f"{TYPED}: falta la clave obligatoria {key!r}")
    return data


def load_avsc(root: Path, relative: str) -> dict:
    path = root / relative
    if not path.is_file():
        raise ContractError(f"esquema declarado inexistente: {relative}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ContractError(f"{relative}: no es JSON valido: {exc}") from exc


def avsc_fields(schema: dict, relative: str) -> list[tuple[str, str, str | None]]:
    """Normaliza los campos del .avsc a (nombre, tipo, logicalType)."""
    if schema.get("type") != "record":
        raise ContractError(f"{relative}: la raiz debe ser un record")
    rows: list[tuple[str, str, str | None]] = []
    for field in schema.get("fields", []):
        name = field.get("name")
        declared = field.get("type")
        if isinstance(declared, dict):
            rows.append((name, declared.get("type"), declared.get("logicalType")))
        elif isinstance(declared, str):
            rows.append((name, declared, None))
        else:
            raise ContractError(
                f"{relative}: campo {name!r} usa un tipo que este gate no interpreta "
                f"({declared!r}); declaralo explicitamente o extiende el gate"
            )
    return rows


def typed_fields(data: dict) -> list[tuple[str, str, str | None]]:
    return [
        (f.get("name"), f.get("type"), f.get("logicalType"))
        for f in data["fields"]
    ]


def check_schema_matches_contract(root: Path, data: dict) -> list[str]:
    """El .avsc debe coincidir campo a campo con el contrato tipado."""
    relative = data["contract"]["sink_schema"]
    schema = load_avsc(root, relative)
    actual = avsc_fields(schema, relative)
    expected = typed_fields(data)

    problems: list[str] = []
    actual_names = [n for n, _, _ in actual]
    expected_names = [n for n, _, _ in expected]

    for name in expected_names:
        if name not in actual_names:
            problems.append(f"campo declarado en data-contract.yaml y ausente del .avsc: {name!r}")
    for name in actual_names:
        if name not in expected_names:
            problems.append(f"campo presente en el .avsc y no declarado en data-contract.yaml: {name!r}")

    for name, etype, elogical in expected:
        for aname, atype, alogical in actual:
            if aname != name:
                continue
            if atype != etype:
                problems.append(
                    f"campo {name!r}: la spec declara type={etype!r} y el .avsc dice {atype!r}"
                )
            if (elogical or None) != (alogical or None):
                problems.append(
                    f"campo {name!r}: la spec declara logicalType={elogical!r} y el .avsc dice {alogical!r}"
                )
    if actual_names != expected_names and not problems:
        problems.append(
            f"orden de campos divergente: spec={expected_names} avsc={actual_names}"
        )
    return problems


def check_prose_declares_fields(root: Path, data: dict) -> list[str]:
    """FR-002 en prosa debe nombrar cada campo del contrato.

    Canary de deriva semantica: si alguien agrega un campo al contrato tipado y
    al .avsc pero no lo explica en la especificacion, el requisito dejo de
    describir lo que el pipeline hace.
    """
    text = (root / PROSE).read_text(encoding="utf-8")
    match = re.search(r"^- \*\*FR-002\*\*:(.+?)(?=^- \*\*FR-003\*\*)", text, re.S | re.M)
    if not match:
        return [f"{PROSE}: no se encuentra FR-002; la prosa no declara el contrato de salida"]
    body = match.group(1)
    return [
        f"campo {name!r} del contrato no aparece en el texto de FR-002"
        for name, _, _ in typed_fields(data)
        if name not in body
    ]


def check_no_embedded_schema(root: Path) -> list[str]:
    """SC-001: el job no puede llevar el esquema embebido en el codigo."""
    path = root / JOB
    if not path.is_file():
        return []  # el job aun no existe; otras tareas lo cubren
    hit = EMBEDDED_SCHEMA.search(path.read_text(encoding="utf-8"))
    if hit:
        return [f"{JOB}: esquema Avro embebido en el codigo ({hit.group(0)!r}); FR-001 exige el registry"]
    return []


def check_sources_resolve(root: Path, data: dict) -> list[str]:
    problems = []
    for key in ("source_schema", "sink_schema"):
        relative = data["contract"][key]
        if not (root / relative).is_file():
            problems.append(f"contract.{key} no resuelve: {relative}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".", help="raiz del repositorio")
    args = parser.parse_args(argv)
    root = Path(args.root).resolve()

    try:
        data = load_typed(root)
        problems: list[str] = []
        problems += check_sources_resolve(root, data)
        problems += check_schema_matches_contract(root, data)
        problems += check_prose_declares_fields(root, data)
        problems += check_no_embedded_schema(root)
    except ContractError as exc:
        print(f"[contract] FAIL: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"[contract] FAIL: {exc}", file=sys.stderr)
        return 1

    if problems:
        print("[contract] FAIL: el contrato derivo entre sus representaciones", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(
            "\n  La especificacion manda. Corrige la fuente y regenera; "
            "no ajustes el .avsc para que el gate calle.",
            file=sys.stderr,
        )
        return 1

    fields = ", ".join(name for name, _, _ in typed_fields(data))
    print(
        f"[contract] OK: {len(data['fields'])} campos coinciden entre "
        f"data-contract.yaml, FR-002 y {data['contract']['sink_schema']}"
    )
    print(f"[contract] contrato: {fields}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
