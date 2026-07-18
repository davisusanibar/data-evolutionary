---
description: Genera tareas ordenadas y enlazadas a requisitos y gates del plan aprobado.
---

<!-- GENERADO desde .specify/commands; no editar a mano. -->

# Generar tareas

## Entrada

```text
$ARGUMENTS
```

1. Resolver la feature con `python3 tools/operations/feature_context.py resolve --path-only`; un argumento explicito tiene precedencia sobre el selector local y el handoff canonico.
2. Leer Constitucion, spec, contrato tipado, plan y checklist de la feature resuelta.
3. Crear `tasks.md` en orden de dependencias con formato `- [ ] [TNNN] [FR-NNN] accion y ruta`.
4. Cubrir cada `FR-NNN` con al menos una tarea; no crear trabajo sin requisito o gate que lo justifique.
5. Anadir siempre tareas de cierre `[GATES]`, `[LEDGER]`, `[INGEST]` y `[SEAL]`. `SEAL` prepara el cambio para revision; commit o push requieren autorizacion del owner o la politica del repositorio.
6. Para `T2` o `T3`, anadir `[PII]` como tarea bloqueante.
7. Cambiar `fase` a `tasked` y ejecutar el gate Spec Kit.
