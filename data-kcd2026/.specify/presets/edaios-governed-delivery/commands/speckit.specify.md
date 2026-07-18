---
description: Crea una especificacion de trabajo Spec Kit con metadatos y trazabilidad EDAIOS ejecutables.
---

<!-- GENERADO desde .specify/commands; no editar a mano. -->

# Especificar

## Entrada

```text
$ARGUMENTS
```

1. Leer `.specify/memory/constitution.md`, `AGENTS.md` y el contexto activo del programa.
2. Crear una sola carpeta `specs/NNN-nombre-corto/`; asignar el siguiente numero disponible sin depender del nombre de una rama.
3. Seleccionar la nueva feature para el worktree con `python3 tools/operations/feature_context.py select <carpeta>`; `.specify/feature.json` queda como handoff canonico del programa y solo cambia cuando el owner cambia ese foco mediante `--canonical`.
4. Crear primero `feature.spec.yaml`, contrato tipado de la feature, y despues `spec.md`.
5. Incluir en el frontmatter plano de `spec.md`: `id`, `estado`, `fase`, `dominio`, `tramo_sensibilidad`, `owner`, `tipo_cambio`, `trazas`, `spec_tipada`, `fuentes`, `value_ledger` e `hipotesis_valor`.
6. Usar requisitos `FR-NNN` y criterios de exito `SC-NNN`, medibles y sin decisiones de implementacion.
7. Para `T2` o `T3`, declarar como bloqueante la clasificacion y seudonimizacion anterior a cualquier ruta LLM.
8. No inventar baseline, owner ni verdad de dominio. Registrar `TBD` o `N/A: razon` cuando corresponda.
9. Regla IV (cero cifras sin fuente): toda cifra que la especificacion cite debe tener fuente, fecha, alcance y rotulo en `evidence/sources.md`; los benchmarks externos son referencia verificada, no promesa.
10. Ejecutar `python3 tools/validation/spec_kit_gate.py . --feature <carpeta>` y corregir la fuente si falla.
