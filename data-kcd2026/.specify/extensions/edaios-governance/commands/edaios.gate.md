---
description: Ejecuta la puerta contractual Spec Kit de EDAIOS (perfil consumer-release) sobre la feature activa.
---

# Gate EDAIOS (consumer-release)

Valida el contrato SDD de una feature de consumer con el perfil liviano
`consumer-release` (ADR-0016): exige el contrato SDD intrínseco (frontmatter,
Constitution Check con pin vigente, FR/SC, checklist, cobertura FR→tarea, cierre,
matriz SC→evidencia) y NO el bookkeeping del monorepo de Core (15 gate-IDs,
tombstones, dominio del kernel, catálogos de gobierno).

1. Resolver el directorio de la feature bajo `specs/` (la que se está entregando).
2. Verificar que el workspace tenga `tools/validation/spec_kit_gate.py` y la
   constitución proyectada en `.specify/memory/constitution.md`. Si faltan, detenerse
   e indicar que primero se debe inyectar EDAIOS Core (seed de constitución + gate).
   No se exigen `.specify/gates.json` con los 15 gates de Core ni los catálogos de
   gobierno: un consumer declara sus propios gates de dominio.
3. Ejecutar `python3 tools/validation/spec_kit_gate.py . --feature <feature_directory> --profile consumer-release`.
4. Detener el workflow si existe cualquier error (fail-closed).
5. Reportar cada incumplimiento con su archivo y regla; no modificar artefactos de forma automatica.
