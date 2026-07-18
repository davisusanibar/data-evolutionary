---
description: Crea un plan tecnico trazable, condicionado por la Constitucion y los gates EDAIOS.
---

<!-- GENERADO desde .specify/commands; no editar a mano. -->

# Planificar

## Entrada

```text
$ARGUMENTS
```

1. Resolver la feature con `python3 tools/operations/feature_context.py resolve --path-only`; un argumento explicito tiene precedencia sobre el selector local y el handoff canonico.
2. Leer Constitucion, `spec.md`, `feature.spec.yaml`, checklist y ADR/RFC citados de la feature resuelta.
3. Crear `plan.md` con contexto tecnico, alternativas, decision, estructura de archivos, pruebas y estrategia de despliegue o reversa.
4. Incluir `## Constitution Check` ESTRICTO: los 7 principios (I..VII) enumerados, cada uno con veredicto `PASS | N/A | VIOLA` y evidencia; `VIOLA` detiene el plan (el camino es el ADR). Cerrar la seccion con el pin de la constitucion vigente: `Constitucion verificada: <version> · sha256:<huella de .specify/memory/constitution.md>`. Si la constitucion cambia despues del plan, el gate marca el check como obsoleto.
5. Incluir `## Gate Impact` usando `.specify/gates.json` como registro canonico.
6. Declarar impacto de arquitectura, ontologia, datos, IA, privacidad, costo y blast-radius cuando aplique.
7. Si el cambio es estructural y no existe ADR habilitante, detenerse y proponer el ADR antes de continuar.
8. Cambiar `fase` a `planned` y ejecutar el gate Spec Kit.
