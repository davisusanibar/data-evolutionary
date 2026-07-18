# data-kcd2026

Submódulo Maven para la demo de KCD 2026, gobernado bajo Spec-Driven
Development con EDAIOS Core (perfil `consumer-release`, ADR-0016).

## Estado

Módulo recién creado (`pom.xml`, clase `App` vacía). Ninguna feature está
implementada todavía; la primera vive en
[`specs/001-revenue-ventana-cliente/`](specs/001-revenue-ventana-cliente/spec.md).

## Gobierno

- `edaios.initiative.json` + `.edaios/` — attachment de iniciativa
  (`initiative-adoption`).
- `.specify/` + `.claude/skills/` — bundle Spec Kit gobernado.
- `tools/validation/spec_kit_gate.py` — gate vendorizado; ver procedencia en
  `tools/validation/spec_kit_gate.SOURCE.md`.

Validar el gate:

```bash
python3 tools/validation/spec_kit_gate.py . --profile consumer-release
```

## Relación con el resto del repositorio

Este módulo es independiente de
[`../data-cdc-kafka-flink-iceberg`](../data-cdc-kafka-flink-iceberg), que ya
demuestra un pipeline CDC + Join operativo (Kafka + Postgres CDC + Flink). La
infraestructura compartida (Kafka, Postgres, Flink) vive en
[`../infra/dockercompose`](../infra/dockercompose).
