#!/usr/bin/env python3
"""Puerta contractual de artefactos Spec Kit para EDAIOS Core Base.

Valida sin dependencias externas la metadata y coherencia de cada feature bajo
`specs/`: spec tipada, trazas, dominio, sensibilidad, valor, plan constitucional
y cobertura requisito -> tarea. Spec Kit orquesta; esta puerta decide.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path


PHASES = {"specified": 0, "clarified": 1, "planned": 2, "tasked": 3, "implemented": 4}
# Constitution Check estricto (F3.2): los 7 principios de la constitución operativa,
# cada uno con veredicto y evidencia; VIOLA es FAIL directo (el camino es el ADR).
PRINCIPLES = ("I", "II", "III", "IV", "V", "VI", "VII")
CC_VERDICTS = {"PASS", "N/A", "VIOLA"}
CC_ROW = re.compile(
    r"^\|\s*(VII|VI|IV|V|III|II|I)\b[^|]*\|\s*([^|]+?)\s*\|\s*([^|]*?)\s*\|",
    re.MULTILINE)
# El pin es una DECLARACIÓN: solo cuenta en su línea propia (no en celdas de
# evidencia ni en comentarios), y debe ser exactamente una.
PIN_LINE = re.compile(r"^Constituci[oó]n verificada:.*?(sha256:[0-9a-f]{64})", re.MULTILINE)
CC_HEADING = re.compile(r"^## Constitution Check\s*$", re.MULTILINE)
FENCE = re.compile(r"^\s*(```|~~~)")
REQUIRED_FIELDS = {
    "id",
    "estado",
    "fase",
    "dominio",
    "tramo_sensibilidad",
    "owner",
    "tipo_cambio",
    "trazas",
    "spec_tipada",
    "fuentes",
    "value_ledger",
    "hipotesis_valor",
}
REQUIRED_GATE_IDS = {
    "FND-PROJECTION",
    "CATALOG-PROJECTION",
    "AGENT-PARITY",
    "SDD-CONTRACT",
    "KOM",
    "MONOREPO-STRUCTURE",
    "BASELINE-SURFACE",
    "CORE-CONFORMANCE",
    "CLAIM-SURFACE",
    "CORE-DISTRIBUTION",
    "CORE-BASE-DEMO",
    "CORE-RELEASE-SEAL",
    "VALIDATE",
    "TEST",
    "TRACEABILITY",
}
PUSH_SCOPED_GATE_IDS = REQUIRED_GATE_IDS - {"VALIDATE"}
REQUIRED_CLOSING_TASKS = {"GATES", "LEDGER", "INGEST", "SEAL"}
PROFILE_REGISTRY = "core/framework/core/profiles/validation-profiles.json"
FEATURE_HANDOFF_SCHEMAS = {"edaios.feature-handoff/v2", "edaios.feature-handoff/v3"}
FEATURE_HANDOFF_ROLES = ("baseline_feature", "last_closed_feature", "active_feature")


def scalar(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def parse_frontmatter(path: Path) -> tuple[dict[str, object], str]:
    text = path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        raise ValueError("frontmatter YAML ausente")
    try:
        end = next(i for i in range(1, len(lines)) if lines[i].strip() == "---")
    except StopIteration as exc:
        raise ValueError("frontmatter YAML sin cierre") from exc
    meta: dict[str, object] = {}
    active_list: str | None = None
    for raw in lines[1:end]:
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("- ") and active_list:
            current = meta.setdefault(active_list, [])
            if not isinstance(current, list):
                raise ValueError(f"{active_list} mezcla valor escalar y lista")
            current.append(scalar(stripped[2:]))
            continue
        if raw[:1].isspace():
            raise ValueError(f"frontmatter anidado no soportado: {stripped}")
        key, sep, value = raw.partition(":")
        if not sep:
            raise ValueError(f"linea de frontmatter invalida: {stripped}")
        key, value = key.strip(), value.strip()
        if not key:
            raise ValueError("clave vacia en frontmatter")
        if value:
            meta[key] = scalar(value)
            active_list = None
        else:
            meta[key] = []
            active_list = key
    return meta, "\n".join(lines[end + 1 :]) + "\n"


TASK_LINE = re.compile(r"^- \[[ xX]\] .+$", re.MULTILINE)
SIMPLE_KEY = re.compile(r"^[A-Za-z_][\w.-]*$")


def parse_simple_yaml(text: str) -> dict[str, object]:
    """Parser YAML plano (clave: escalar | lista de `- item`), sin dependencias.

    Estricto a proposito: una clave invalida o una estructura fuera de este
    subconjunto es ValueError — la spec tipada malformada debe FALLAR, no pasar
    por comparacion de subcadenas.
    """
    data: dict[str, object] = {}
    active_list: str | None = None
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("- ") and active_list:
            current = data.setdefault(active_list, [])
            if not isinstance(current, list):
                raise ValueError(f"{active_list} mezcla escalar y lista")
            current.append(scalar(stripped[2:]))
            continue
        if raw[:1].isspace():
            # Una clave indentada es estructura anidada: fuera del subconjunto
            # plano — promoverla a raiz aceptaria un documento que un parser
            # YAML real lee distinto.
            raise ValueError(f"estructura anidada no soportada: {stripped[:60]}")
        key, sep, value = raw.partition(":")
        key = key.strip()
        if not sep or not SIMPLE_KEY.fullmatch(key):
            raise ValueError(f"linea invalida: {stripped[:60]}")
        value = value.strip()
        if value:
            data[key] = scalar(value)
            active_list = None
        else:
            data[key] = []
            active_list = key
    if not data:
        raise ValueError("documento vacio")
    return data


def section_body(text: str, header: str) -> str:
    """Contenido de una seccion `## header` hasta el siguiente `## ` (o el final)."""
    match = re.search(rf"^## {re.escape(header)}\s*$(.*?)(?=^## |\Z)", text,
                      re.MULTILINE | re.DOTALL)
    return match.group(1).strip() if match else ""


def constitution_fingerprint(root: Path) -> str:
    """Huella sha256 del archivo de la constitución operativa compilada."""
    path = root / ".specify" / "memory" / "constitution.md"
    try:
        return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def strip_fences(text: str) -> str:
    """Elimina el contenido de bloques cercados (``` / ~~~).

    Un heading o una tabla dentro de un fence es un EJEMPLO, no una declaración:
    no puede satisfacer el check ni eclipsar la sección real. Un fence sin cerrar
    consume hasta el final (fail-closed: lo no declarado no existe).
    """
    kept: list[str] = []
    fenced = False
    for line in text.splitlines():
        if FENCE.match(line):
            fenced = not fenced
            continue
        if not fenced:
            kept.append(line)
    return "\n".join(kept)


def validate_constitution_check(root: Path, tag: str, plan_text: str, results: Results) -> None:
    """Constitution Check estricto (F3.2): 7 principios, veredictos y pin vigente.

    La tabla es declarativa — la máquina no puede verificar que un PASS sea
    verdad (eso lo firma el checkpoint humano) — pero sí puede exigir que el
    check sea COMPLETO (sin omitir el principio que estorba), ÚNICO (sin sección
    duplicada ni ejemplos en fences que eclipsen la real), que los veredictos
    sean del dominio, que VIOLA no pase jamás, y que el check se hizo contra la
    constitución VIGENTE (pin declarado en su línea propia; si la constitución
    cambió después del plan, el check queda obsoleto y la puerta lo dice).
    """
    flat = strip_fences(plan_text)
    headings = CC_HEADING.findall(flat)
    results.check(len(headings) == 1, f"{tag}: seccion Constitution Check unica",
                  f"{len(headings)} secciones declaradas" if len(headings) != 1 else "")
    if len(headings) != 1:
        return
    section = section_body(flat, "Constitution Check")
    results.check(section, f"{tag}: Constitution Check declarado con contenido")
    if not section:
        return
    rows: dict[str, tuple[str, str]] = {}
    duplicated: list[str] = []
    for match in CC_ROW.finditer(section):
        numeral, verdict, evidence = match.group(1), match.group(2).strip(), match.group(3).strip()
        if numeral in rows:
            duplicated.append(numeral)
        rows[numeral] = (verdict, evidence)
    missing = [p for p in PRINCIPLES if p not in rows]
    detail = ""
    if missing:
        detail = "faltan: " + ", ".join(missing)
    elif duplicated:
        detail = "duplicados: " + ", ".join(sorted(set(duplicated)))
    results.check(not missing and not duplicated,
                  f"{tag}: los 7 principios enumerados (I..VII)", detail)
    bad = sorted(p for p, (v, _e) in rows.items() if v not in CC_VERDICTS)
    results.check(not bad, f"{tag}: veredictos del dominio (PASS | N/A | VIOLA)", ", ".join(bad))
    weak = sorted(p for p, (_v, e) in rows.items() if len(e) < 8)
    results.check(not weak, f"{tag}: evidencia minima por principio", ", ".join(weak))
    violas = sorted(p for p, (v, _e) in rows.items() if v == "VIOLA")
    results.check(not violas, f"{tag}: sin principios en VIOLA",
                  (", ".join(violas) + " — VIOLA exige el camino ADR: enmienda el canon o cambia el plan")
                  if violas else "")
    pins = PIN_LINE.findall(section)
    results.check(len(pins) == 1,
                  f"{tag}: pin de la constitucion declarado (una linea 'Constitucion verificada:')",
                  f"{len(pins)} lineas de pin" if len(pins) != 1 else "")
    if len(pins) != 1:
        return
    current = constitution_fingerprint(root)
    if not current:
        results.check(False, f"{tag}: constitucion compilada legible para verificar el pin")
        return
    results.check(pins[0] == current, f"{tag}: pin vigente de la constitucion",
                  "" if pins[0] == current else
                  "obsoleto: la constitucion cambio despues del plan — re-valida el check y actualiza el pin")


def values(meta: dict[str, object], key: str) -> list[str]:
    value = meta.get(key, [])
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def within(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


class Results:
    def __init__(self) -> None:
        self.rows: list[tuple[bool, str, str]] = []

    def check(self, ok: object, name: str, detail: str = "") -> None:
        self.rows.append((bool(ok), name, detail))

    @property
    def ok(self) -> bool:
        return all(row[0] for row in self.rows)

    def print(self) -> None:
        for ok, name, detail in self.rows:
            suffix = f"  · {detail}" if detail else ""
            print(f"  [{'OK ' if ok else 'FAIL'}] {name}{suffix}")
        passed = sum(1 for ok, _name, _detail in self.rows if ok)
        print(f"-- {passed}/{len(self.rows)} checks OK --")


def load_validation_profile(root: Path, selected: str, results: Results) -> set[str]:
    """Valida el registry acumulativo y devuelve controles efectivos.

    El profile selecciona controles; nunca apaga las validaciones SDD
    intrínsecas. Los checks estructurales del monorepo de Core (gates.json de 15
    IDs, tombstones, dominio del kernel, catálogos de gobierno) sí se activan por
    control: solo `core-release` y sus hijos los declaran; `consumer-release` no.

    `consumer-release` (ADR-0016) es un perfil raíz liviano: se resuelve como
    built-in, sin exigir el registry del árbol de Core, para que un consumer no
    tenga que llevar `core/framework/core/profiles/` a su repo.
    """
    if selected == "consumer-release":
        results.check(True, "perfil consumer-release (raiz liviana, ADR-0016)",
                      "sdd-contract, claim-surface")
        return {"sdd-contract", "claim-surface"}
    path = root / PROFILE_REGISTRY
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("schema") != "edaios.validation-profile-registry/v1":
            raise ValueError("schema de profiles no soportado")
        registry_rows = data.get("profiles")
        if not isinstance(registry_rows, list) or not all(isinstance(row, dict) for row in registry_rows):
            raise ValueError("profiles debe ser una lista de objetos")
        profile_root = (root / "core/framework/core/profiles").resolve()
        by_id: dict[str, dict[str, object]] = {}
        for registry_row in registry_rows:
            profile_id = str(registry_row.get("id", ""))
            profile_path = (root / str(registry_row.get("path", ""))).resolve()
            if not within(profile_root, profile_path) or not profile_path.is_file():
                raise ValueError(f"{profile_id}: path de profile no resoluble")
            row = json.loads(profile_path.read_text(encoding="utf-8"))
            if row.get("schema") != "edaios.conformance-profile/v1" or row.get("id") != profile_id:
                raise ValueError(f"{profile_id}: contrato de profile inválido")
            if row.get("remove_controls"):
                raise ValueError(f"{profile_id}: remove_controls prohibido")
            controls = row.get("controls")
            if not isinstance(controls, list) or not controls or len(controls) != len(set(controls)):
                raise ValueError(f"{profile_id}: controls inválidos")
            by_id[profile_id] = row
        if len(by_id) != len(registry_rows) or "" in by_id:
            raise ValueError("ids de profile vacíos o duplicados")
        expected = {"core-release", "initiative-adoption", "federation"}
        if set(by_id) != expected:
            raise ValueError(f"profiles requeridos: {sorted(expected)}")
    except (OSError, ValueError, AttributeError, json.JSONDecodeError) as exc:
        results.check(False, "registry de perfiles parseable", str(exc))
        return set()

    resolving: set[str] = set()
    resolved: dict[str, set[str]] = {}

    def controls(profile_id: str) -> set[str]:
        if profile_id in resolved:
            return resolved[profile_id]
        if profile_id in resolving:
            raise ValueError(f"ciclo de herencia en {profile_id}")
        if profile_id not in by_id:
            raise ValueError(f"parent no resoluble: {profile_id}")
        resolving.add(profile_id)
        row = by_id[profile_id]
        own = row.get("controls", [])
        parent = row.get("parent")
        if not isinstance(own, list) or (parent is not None and not isinstance(parent, str)):
            raise ValueError(f"{profile_id}: controls/parent inválidos")
        effective = {str(item) for item in own}
        if parent is not None:
            parent_controls = controls(parent)
            effective.update(parent_controls)
        resolving.remove(profile_id)
        resolved[profile_id] = effective
        return effective

    try:
        if by_id["core-release"].get("parent") is not None:
            raise ValueError("core-release no puede heredar otro profile")
        if by_id["initiative-adoption"].get("parent") != "core-release":
            raise ValueError("initiative-adoption debe heredar core-release")
        if by_id["federation"].get("parent") != "initiative-adoption":
            raise ValueError("federation debe heredar initiative-adoption")
        selected_controls = controls(selected)
        for profile_id in by_id:
            controls(profile_id)
    except ValueError as exc:
        results.check(False, "perfiles acumulativos sin debilitamiento", str(exc))
        return set()
    results.check(True, "perfiles acumulativos sin debilitamiento", selected)
    # Los perfiles del árbol de Core activan los checks estructurales del monorepo;
    # consumer-release (built-in, arriba) no los declara. Ver ADR-0016.
    return selected_controls | {"core-monorepo"}


def dominios_disponibles(root: Path) -> set[str]:
    """Scope instalado; Core Base no descubre ni infiere consumers o dominios."""
    return {"core"}


def load_gate_ids(root: Path, results: Results, structural: bool = True) -> set[str]:
    path = root / ".specify" / "gates.json"
    if not structural and not path.is_file():
        # Un consumer declara sus propios gates de dominio; no está obligado a
        # portar gates.json ni los 15 gates del monorepo de Core (ADR-0016).
        results.check(True, "gates.json opcional en consumer", "ausente")
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("schema") != "edaios.sdd.gates/v1":
            raise ValueError("schema de gates no soportado")
        gates = data.get("gates", [])
        if not isinstance(gates, list) or not all(isinstance(gate, dict) for gate in gates):
            raise ValueError("gates debe ser una lista de objetos")
        ids = [str(gate.get("id", "")) for gate in gates if isinstance(gate, dict)]
        commands_ok = all(gate.get("command") for gate in gates if isinstance(gate, dict))
    except (OSError, ValueError, AttributeError) as exc:
        results.check(False, "registro de gates parseable", str(exc))
        return set()
    results.check(len(ids) == len(set(ids)) and "" not in ids, "IDs de gate unicos")
    results.check(commands_ok, "gates con comando ejecutable")
    # Los 15 gate-IDs de Core y sus scopes pre-push,ci solo aplican al árbol de
    # Core: son bookkeeping del monorepo, no salud de la feature (ADR-0016).
    if structural:
        for gate in gates:
            gate_id = str(gate.get("id", ""))
            raw_scope = gate.get("scope", "")
            if isinstance(raw_scope, str):
                scopes = {part.strip() for part in raw_scope.split(",") if part.strip()}
            elif isinstance(raw_scope, list):
                scopes = {str(part).strip() for part in raw_scope if str(part).strip()}
            else:
                scopes = set()
            if gate_id in PUSH_SCOPED_GATE_IDS:
                results.check(
                    {"pre-push", "ci"}.issubset(scopes),
                    f"{gate_id}: scopes pre-push,ci obligatorios",
                )
        missing = sorted(REQUIRED_GATE_IDS - set(ids))
        results.check(not missing, "gates minimos presentes", ", ".join(missing) if missing else "")
    return set(ids)


def reference_catalogs(root: Path) -> tuple[str, str]:
    adr = (root / "governance" / "ADR_CATALOG.md").read_text(encoding="utf-8")
    rfc = (root / "governance" / "RFC_CATALOG.md").read_text(encoding="utf-8")
    return adr, rfc


def validate_feature(root: Path, feature: Path, results: Results, structural: bool = True) -> None:
    tag = feature.relative_to(root).as_posix()
    spec_path = feature / "spec.md"
    try:
        meta, body = parse_frontmatter(spec_path)
    except (OSError, ValueError) as exc:
        results.check(False, f"{tag}: spec parseable", str(exc))
        return

    missing = sorted(key for key in REQUIRED_FIELDS if key not in meta)
    results.check(not missing, f"{tag}: metadata obligatoria", ", ".join(missing) if missing else "")
    if missing:
        return

    feature_id = str(meta["id"])
    phase = str(meta["fase"])
    results.check(phase in PHASES, f"{tag}: fase valida", phase)
    if phase not in PHASES:
        return
    level = PHASES[phase]

    state = str(meta["estado"])
    results.check(state in {"Borrador", "Propuesto", "Cerrado"},
                  f"{tag}: estado de trabajo valido", state)
    lifecycle_ok = (
        (phase == "implemented" and state == "Cerrado")
        or (phase != "implemented" and state in {"Borrador", "Propuesto"})
    )
    results.check(
        lifecycle_ok,
        f"{tag}: estado y fase compatibles",
        f"estado={state} fase={phase}",
    )
    owner = str(meta["owner"]).strip()
    results.check(bool(owner) and owner.upper() not in {"TBD", "N/A"}, f"{tag}: owner declarado")

    dominio = str(meta["dominio"])
    if structural:
        # El árbol de Core Base solo resuelve el dominio del kernel.
        results.check(dominio in dominios_disponibles(root), f"{tag}: dominio resoluble", dominio)
    else:
        # Consumer: su dominio no vive en el registro del kernel (ADR-0016).
        results.check(bool(dominio.strip()) and dominio.upper() not in {"TBD", "N/A"},
                      f"{tag}: dominio declarado", dominio)
    sensitivity = str(meta["tramo_sensibilidad"])
    results.check(sensitivity in {"T0", "T1", "T2", "T3"}, f"{tag}: sensibilidad valida", sensitivity)

    traces = values(meta, "trazas")
    if structural:
        try:
            adr_catalog, rfc_catalog = reference_catalogs(root)
        except OSError as exc:
            results.check(False, f"{tag}: catalogos ADR/RFC disponibles", str(exc))
            adr_catalog, rfc_catalog = "", ""
        bad_traces: list[str] = []
        for ref in traces:
            if re.fullmatch(r"ADR-\d{4}", ref) and ref not in adr_catalog:
                bad_traces.append(ref)
            elif re.fullmatch(r"RFC-\d{4}", ref) and ref not in rfc_catalog:
                bad_traces.append(ref)
            elif not re.fullmatch(r"(?:ADR-\d{4}|RFC-\d{4})", ref):
                bad_traces.append(ref)
        results.check(bool(traces) and not bad_traces, f"{tag}: trazas resolubles", ", ".join(bad_traces))
    else:
        # Consumer: valida el FORMATO de las trazas; no exige el catálogo ADR/RFC
        # de Core, que vive en su árbol de gobierno (ADR-0016).
        bad_format = [ref for ref in traces if not re.fullmatch(r"(?:ADR-\d{4}|RFC-\d{4})", ref)]
        results.check(bool(traces) and not bad_format, f"{tag}: trazas con formato valido", ", ".join(bad_format))
    structural = str(meta["tipo_cambio"]) in {"architecture", "governance", "ontology"}
    has_adr = any(re.fullmatch(r"ADR-\d{4}", ref) for ref in traces)
    results.check(not structural or has_adr, f"{tag}: cambio estructural respaldado por ADR")

    typed_rel = str(meta["spec_tipada"])
    typed = (root / typed_rel).resolve()
    typed_ok = within(root, typed) and typed.exists() and typed.name.endswith((".spec.yaml", ".spec.yml"))
    typed_schema = ""
    results.check(typed_ok, f"{tag}: spec tipada existe", typed_rel)
    if typed_ok:
        spec_rel = spec_path.relative_to(root).as_posix()
        try:
            typed_data = parse_simple_yaml(typed.read_text(encoding="utf-8", errors="ignore"))
        except ValueError as exc:
            results.check(False, f"{tag}: spec tipada parseable", str(exc))
        else:
            typed_schema = str(typed_data.get("schema", "")).strip()
            results.check(
                str(typed_data.get("id", "")) == feature_id
                and str(typed_data.get("artifact", "")) == spec_rel,
                f"{tag}: contrato tipado enlaza id y artefacto",
                f"id={typed_data.get('id')} artifact={typed_data.get('artifact')}",
            )

    source_refs = values(meta, "fuentes")
    missing_sources = [ref for ref in source_refs if not within(root, root / ref) or not (root / ref).exists()]
    results.check(bool(source_refs) and not missing_sources, f"{tag}: fuentes existen", ", ".join(missing_sources))

    value_ref = str(meta["value_ledger"])
    value_ok = False
    if value_ref.startswith("N/A:") and len(value_ref.split(":", 1)[1].strip()) >= 8:
        value_ok = True
    elif re.fullmatch(r"VL-\d{3}", value_ref):
        try:
            ledger = (root / "governance" / "VALUE_LEDGER.md").read_text(encoding="utf-8")
            value_ok = value_ref in ledger
        except OSError:
            value_ok = False
    results.check(value_ok, f"{tag}: vinculo de valor explicito", value_ref)
    hypothesis = str(meta["hipotesis_valor"]).strip()
    results.check(len(hypothesis) >= 12 and hypothesis.upper() != "TBD", f"{tag}: hipotesis de valor")

    requirements = set(re.findall(r"\bFR-\d{3}\b", body))
    success = set(re.findall(r"\bSC-\d{3}\b", body))
    results.check(bool(requirements), f"{tag}: requisitos FR declarados")
    results.check(bool(success), f"{tag}: criterios SC declarados")

    # Disparador por ARTEFACTO, no por fase auto-declarada: la fase decide qué
    # debe EXISTIR; si el artefacto existe, su contenido se valida SIEMPRE.
    # (Degradar `fase:` en el frontmatter no apaga la validación de un plan con
    # VIOLA — ese era el bypass de primer orden de la revisión adversarial.)
    checklist = feature / "checklists" / "requirements.md"
    if level >= PHASES["planned"]:
        results.check(checklist.exists(), f"{tag}: checklist de requisitos existe")
    if checklist.exists():
        checklist_text = checklist.read_text(encoding="utf-8")
        results.check(TASK_LINE.search(checklist_text), f"{tag}: checklist con items reales")
        unchecked = re.findall(r"^- \[ \]", checklist_text, re.MULTILINE)
        results.check(not unchecked, f"{tag}: checklist sin pendientes", f"{len(unchecked)} pendientes")

    plan = feature / "plan.md"
    if level >= PHASES["planned"]:
        results.check(plan.exists(), f"{tag}: plan existe")
    if plan.exists():
        plan_text = plan.read_text(encoding="utf-8")
        validate_constitution_check(root, tag, plan_text, results)
        results.check(section_body(plan_text, "Gate Impact"),
                      f"{tag}: Gate Impact declarado con contenido")
        unresolved = re.findall(r"\b(?:NEEDS CLARIFICATION|TODO|TKTK)\b", plan_text)
        results.check(not unresolved, f"{tag}: plan sin placeholders bloqueantes")

    tasks = feature / "tasks.md"
    if level >= PHASES["tasked"]:
        results.check(tasks.exists(), f"{tag}: tasks existe")
    if tasks.exists():
        tasks_text = tasks.read_text(encoding="utf-8")
        task_lines = TASK_LINE.findall(tasks_text)
        results.check(task_lines, f"{tag}: tasks con tareas reales",
                      f"{len(task_lines)} tareas")
        task_body = "\n".join(task_lines)
        covered = set(re.findall(r"\bFR-\d{3}\b", task_body))
        missing_coverage = sorted(requirements - covered)
        unknown = sorted(covered - requirements)
        results.check(not missing_coverage, f"{tag}: cobertura FR -> tareas", ", ".join(missing_coverage))
        results.check(not unknown, f"{tag}: tareas sin FR inexistente", ", ".join(unknown))
        missing_closing = sorted(marker for marker in REQUIRED_CLOSING_TASKS if f"[{marker}]" not in task_body)
        results.check(not missing_closing, f"{tag}: tareas de cierre", ", ".join(missing_closing))
        results.check(sensitivity not in {"T2", "T3"} or "[PII]" in task_body, f"{tag}: gate PII cuando aplica")
        if level >= PHASES["implemented"]:
            unchecked = re.findall(r"^- \[ \]", tasks_text, re.MULTILINE)
            results.check(not unchecked, f"{tag}: implementacion sin tareas pendientes", f"{len(unchecked)} pendientes")
    verification = feature / "verification.md"
    if level >= PHASES["tasked"] and typed_schema == "edaios.sdd.feature/v2":
        results.check(verification.exists(), f"{tag}: matriz SC -> verificacion existe")
    if verification.exists():
        verification_text = verification.read_text(encoding="utf-8")
        missing_sc = sorted(success - set(re.findall(r"\bSC-\d{3}\b", verification_text)))
        results.check(not missing_sc, f"{tag}: cada SC tiene matriz de verificacion", ", ".join(missing_sc))
        results.check("evidence/" in verification_text, f"{tag}: matriz declara evidencia de cierre")
        evidence_refs = re.findall(r"`(evidence/[^`]+)`", verification_text)
        missing_evidence = sorted(ref for ref in evidence_refs if not (feature / ref).is_file())
        results.check(not missing_evidence, f"{tag}: paths de evidencia resolubles", ", ".join(missing_evidence))


def feature_paths(root: Path, explicit: str | None) -> list[Path]:
    if explicit:
        feature = (root / explicit).resolve()
        if not within(root / "specs", feature):
            # Normalizacion F3.4: aceptar el nombre pelado (`002-...`) ademas de
            # `specs/002-...` — la misma tolerancia que el adapter de ingesta.
            candidate = (root / "specs" / explicit).resolve()
            if within(root / "specs", candidate) and candidate != (root / "specs").resolve():
                feature = candidate
            else:
                raise ValueError("--feature debe vivir bajo specs/")
        return [feature]
    specs = root / "specs"
    if not specs.is_dir():
        return []
    # Fail-closed: TODO directorio de feature cuenta, tenga o no spec.md —
    # un directorio sin spec.md es un FAIL, no un invisible.
    return sorted(p.parent for p in specs.rglob("spec.md") if p.parent != specs and (p.parent / "feature.spec.yaml").is_file())


def registered_feature(root: Path, results: Results) -> Path | None:
    """Valida el handoff v2 y devuelve su ``active_feature``.

    El handoff canónico no es un selector local: conserva baseline, último cierre
    y foco activo como identidades separadas. Los selectores locales v1 los
    resuelve ``feature_context.py`` y nunca sustituyen este contrato versionado.
    """
    pointer = root / ".specify" / "feature.json"
    if not pointer.exists():
        return None
    try:
        data = json.loads(pointer.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("feature.json debe ser un objeto")
        if data.get("schema") not in FEATURE_HANDOFF_SCHEMAS:
            raise ValueError("schema de handoff no soportado")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        results.check(False, "feature.json parseable", str(exc))
        return None

    features: dict[str, Path] = {}
    directories: list[str] = []
    for role in FEATURE_HANDOFF_ROLES:
        row = data.get(role)
        if data.get("schema") == "edaios.feature-handoff/v3" and role == "active_feature" and row is None:
            results.check(True, "feature.json active_feature idle permitido")
            continue
        if not isinstance(row, dict):
            results.check(False, f"feature.json {role} parseable", "debe ser un objeto")
            continue
        try:
            pointer_id = str(row["id"]).strip()
            feature_rel = str(row["feature_directory"]).strip()
            if not pointer_id or not feature_rel:
                raise ValueError("id o feature_directory vacio")
        except (ValueError, KeyError, TypeError) as exc:
            results.check(False, f"feature.json {role} parseable", str(exc))
            continue

        feature = (root / feature_rel).resolve()
        ok = (
            within(root / "specs", feature)
            and feature != (root / "specs").resolve()
            and (feature / "spec.md").is_file()
        )
        results.check(ok, f"feature.json {role} apunta a una feature real", feature_rel)
        if not ok:
            continue
        features[role] = feature
        directories.append(feature_rel)

        try:
            meta, _body = parse_frontmatter(feature / "spec.md")
            typed = (root / str(meta["spec_tipada"])).resolve()
            if not within(root, typed) or not typed.is_file():
                raise ValueError("spec_tipada ausente o fuera del repositorio")
            typed_data = parse_simple_yaml(typed.read_text(encoding="utf-8", errors="ignore"))
            spec_id = str(meta["id"]).strip()
            typed_id = str(typed_data["id"]).strip()
        except (OSError, ValueError, KeyError, TypeError) as exc:
            results.check(False, f"feature.json {role} cruza identidad", str(exc))
            continue
        identity_ok = pointer_id == spec_id == typed_id
        results.check(
            identity_ok,
            f"feature.json {role} cruza identidad",
            f"pointer={pointer_id} spec={spec_id} typed={typed_id}",
        )
        if role in {"baseline_feature", "last_closed_feature"}:
            state = str(meta.get("estado", "")).strip()
            phase = str(meta.get("fase", "")).strip()
            results.check(
                state == "Cerrado" and phase == "implemented",
                f"feature.json {role} referencia una feature cerrada",
                f"estado={state or '<vacio>'} fase={phase or '<vacia>'}",
            )

    idle_v3 = data.get("schema") == "edaios.feature-handoff/v3" and data.get("active_feature") is None
    complete = len(features) == len(FEATURE_HANDOFF_ROLES) or (idle_v3 and len(features) == 2)
    results.check(complete, "feature.json declara baseline, ultimo cierre y foco activo")
    if complete:
        results.check(
            len(directories) == len(set(directories)),
            "feature.json mantiene referencias distintas",
            ", ".join(directories),
        )
    return features.get("active_feature")


def validate_tombstones(root: Path, results: Results) -> None:
    path = root / "specs" / "tombstones.json"
    if not path.exists():
        results.check(False, "specs/tombstones.json presente")
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data.get("tombstones") if data.get("schema") == "edaios.feature-tombstones/v1" else None
        if not isinstance(rows, list) or not rows:
            raise ValueError("tombstones debe ser lista no vacia")
        ids = set()
        for row in rows:
            required = {"id", "former_directory", "status", "authority", "replacement", "content", "claim_boundary"}
            if not isinstance(row, dict) or required - set(row) or row["status"] != "retired":
                raise ValueError("tombstone incompleto")
            if row["id"] in ids or row["content"] is not None:
                raise ValueError("tombstone duplicado o con contenido inventado")
            ids.add(row["id"])
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        results.check(False, "tombstones resolubles", str(exc))
    else:
        results.check(True, "tombstones resolubles", str(path.relative_to(root)))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", default=".", help="raiz del repositorio")
    parser.add_argument("--feature", help="feature bajo specs/; por defecto valida todas")
    parser.add_argument(
        "--profile", default="core-release",
        choices=("core-release", "initiative-adoption", "federation", "consumer-release"),
        help="perfil de conformidad; consumer-release es la raíz liviana (ADR-0016)",
    )
    args = parser.parse_args()
    root = Path(args.root).resolve()
    explicit = args.feature

    print(f"== Spec Kit Gate · EDAIOS · profile={args.profile} ==")
    results = Results()
    load_validation_profile(root, args.profile, results)
    # Fail-closed por allowlist: `consumer-release` es el ÚNICO perfil liviano
    # (ADR-0016). Cualquier otro —incluido un core-release con registry roto o
    # ausente— exige el bookkeeping del monorepo, para que su falta FALLE en vez
    # de degradar en silencio a modo consumer.
    structural = args.profile != "consumer-release"
    load_gate_ids(root, results, structural)
    if structural:
        validate_tombstones(root, results)
        registered = registered_feature(root, results)
    else:
        registered = None
    try:
        features = feature_paths(root, explicit)
    except ValueError as exc:
        results.check(False, "ruta de feature valida", str(exc))
        results.print()
        return 1
    if registered and registered not in features:
        features.append(registered)
    if not features:
        results.check(True, "sin features Spec Kit versionadas", "puerta no aplica")
    elif not explicit and structural:
        # F3.4: si hay features versionadas, el puntero de identidad es
        # OBLIGATORIO — sin el, el cruce de identidad triple era opt-in y
        # borrar feature.json apagaba el control en silencio. El handoff es
        # bookkeeping del monorepo de Core; no aplica a un consumer (ADR-0016).
        results.check((root / ".specify" / "feature.json").exists(),
                      "feature.json presente (identidad obligatoria con features versionadas)")
    ids_seen: dict[str, list[str]] = {}
    for feature in features:
        if not (feature / "spec.md").exists():
            rel = feature.relative_to(root).as_posix() if within(root, feature) else str(feature)
            results.check(False, f"{rel}: spec.md existe")
            continue
        validate_feature(root, feature, results, structural)
        try:
            meta, _ = parse_frontmatter(feature / "spec.md")
            fid = str(meta.get("id", "")).strip()
            if fid:
                ids_seen.setdefault(fid, []).append(feature.name)
        except (OSError, ValueError):
            pass  # ya reportado por validate_feature
    if not explicit:
        # F3.4: unicidad de id entre features — dos features con el mismo id
        # rompen la trazabilidad del puntero y de la ingesta.
        dup = {fid: dirs for fid, dirs in ids_seen.items() if len(dirs) > 1}
        results.check(not dup, "ids de feature unicos bajo specs/",
                      "; ".join(f"{fid}: {', '.join(dirs)}" for fid, dirs in sorted(dup.items())))
    results.print()
    return 0 if results.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
