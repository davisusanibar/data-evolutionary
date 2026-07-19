---
description: Verifica o recompila la constitucion operativa como proyeccion gobernada de Foundation.
---

<!-- GENERADO desde .specify/commands; no editar a mano. -->

# Constitucion operativa EDAIOS

## Entrada

```text
$ARGUMENTS
```

1. Tratar `.specify/memory/constitution.md` como vista generada, nunca como fuente editable.
2. Para un cambio normativo, detenerse y gobernar primero el KO en `core/foundation/` mediante ADR.
3. Para un cambio de destilacion, editar `.specify/memory/constitution.src.json` manteniendo citas verificables.
4. Ejecutar `python3 tools/publishing/compile_constitution.py --check` para verificar; recompilar sin `--check` solo cuando la receta o Foundation hayan cambiado.
5. Al recompilar con cambios, avisar al equipo: los pines `sha256` de los Constitution Check de los planes vivos quedan obsoletos y `spec_kit_gate` los marcara en rojo hasta re-validarlos.
6. En cualquier conflicto, aplicar `core/foundation/` como autoridad superior.
