---
description: Analiza sin modificar la coherencia, cobertura y cumplimiento constitucional de los artefactos SDD.
---

<!-- GENERADO desde .specify/commands; no editar a mano. -->

# Analizar

## Entrada

```text
$ARGUMENTS
```

1. Resolver la feature con `python3 tools/operations/feature_context.py resolve --path-only`; un argumento explicito tiene precedencia sobre el selector local y el handoff canonico.
2. Operar en modo estrictamente solo lectura sobre spec, contrato tipado, checklist, plan y tasks.
3. Ejecutar `python3 tools/validation/spec_kit_gate.py . --feature <carpeta-resuelta>`.
4. Reportar conflictos constitucionales, ambiguedades, duplicados, requisitos sin tareas, tareas sin requisito, referencias rotas y gates ausentes.
5. Clasificar hallazgos como `CRITICAL`, `HIGH`, `MEDIUM` o `LOW` y citar archivo/seccion.
6. Bloquear implementacion con cualquier `CRITICAL`, `HIGH` o gate rojo; proponer correcciones sin aplicarlas automaticamente.
