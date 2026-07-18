---
name: speckit-edaios-gate
description: Ejecuta la puerta contractual Spec Kit de EDAIOS sobre la feature activa.
compatibility: Requires spec-kit project structure with .specify/ directory
metadata:
  author: github-spec-kit
  source: edaios-governance:commands/edaios.gate.md
---

# Gate EDAIOS

1. Resolver la feature desde `.specify/feature.json`.
2. Verificar que el workspace tenga el contrato EDAIOS: `.specify/gates.json`,
   `tools/validation/spec_kit_gate.py` y los ledgers de autoridad. Si faltan,
   detenerse e indicar que primero se debe bootstrappear el engine EDAIOS.
3. Ejecutar `python3 tools/validation/spec_kit_gate.py . --feature <feature_directory>`.
4. Detener el workflow si existe cualquier error.
5. Reportar cada incumplimiento con su archivo y regla; no modificar artefactos de forma automatica.