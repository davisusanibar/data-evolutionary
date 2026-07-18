# Verificación — Deck KCD 2026 y página del caso, regenerables por contrato

La matriz cierra el círculo: cada criterio de éxito resuelve a una tarea, un
test y un gate. Las rutas de evidencia se llenan al ejecutar; **una fila con
evidencia vacía no es un resultado anticipado, es trabajo pendiente**.

| SC | FR | Tarea | Test/marker | Gate | Evidencia de cierre |
|---|---|---|---|---|---|
| SC-001 | FR-001 | T001, T008, T009 | `deliverables_check.check_template_digest` | deliverables | `evidence/sc-001-007-deliverables-gate.json` (corrida `TBD-DIGEST` → exit 1 informando el digest; pin fijado; verde posterior) |
| SC-002 | FR-002 | T002, T009 | `deliverables_check.check_no_placeholders` | deliverables | `evidence/sc-001-007-deliverables-gate.json` (0 placeholders en 35 slides) |
| SC-003 | FR-003 | T003, T009, T010 | `deliverables_check.check_log_extracts` | deliverables | `evidence/sc-001-007-deliverables-gate.json` · `evidence/adr-0015-red-gate.json` (3 extractos vs evidencia, slides 28/30/32) |
| SC-004 | FR-004 | T004, T009 | `deliverables_check.check_boundary_slide` | deliverables | `evidence/sc-001-007-deliverables-gate.json` (slide 33 "Qué NO demostramos") |
| SC-005 | FR-005 | T005, T009 | `deliverables_check.check_speaker_notes` | deliverables | `evidence/sc-001-007-deliverables-gate.json` (33/35 notas ≥ umbral 30) |
| SC-006 | FR-006 | T011, T012, T013 | `generate_use_case_page.py --check` | page-check | `evidence/sc-001-007-deliverables-gate.json` (verde exit 0; mutación de un byte → exit 1; restaurado) |
| SC-006 (vista OS) | FR-006 | T014 | `generate_os_flink_page.py --check` | deliverables (`contract.page_os`) | `evidence/t014-os-flink-page.json` (verde exit 0; mutación de un byte `150.0`→`151.0` offset 16100 → exit 1 propagado por el gate; restaurado; sha256 estable entre dos corridas) |
| SC-007 | FR-007 | T008, T009 | salida del gate declara verificación estructural | deliverables | `evidence/sc-001-007-deliverables-gate.json` (salida verde declara "ESTRUCTURAL, no byte a byte"; ningún check pina el build) |
| SC-008 | FR-008 | T006, T007 | `validate.py --original template.pptx` | ooxml | `evidence/sc-008-ooxml-validate.json` ("All validations PASSED!", exit 0, 2026-07-17) |
| Anexos (T015) | FR-007, FR-008 | T015 | `deliverables_check.check_annexes` | deliverables (`contract.annexes`) | `evidence/t015-anexos-pdf.json` (pipeline de 4 pasos exit 0; deck 43, a1 51, a2 8, a3 10 páginas como `aN.pdf` independientes; texto seleccionable verificado en a2.pdf; rojo inducido moviendo a2.pdf → exit 1, restaurado; el PDF no reclama byte-determinismo — verificación estructural, ADR-0015 inv. 5) |
| Capturas (T016) | FR-007 | T016 | `deliverables_check.check_captures` | deliverables (`contract.captures`) | `evidence/t016-capturas.json` (4 capturas del owner copiadas byte a byte a `deck/anexos/` y pineadas por sha256; slides 39–42, deck 43; pipeline de 4 pasos exit 0, `validate.py` PASSED exit 0, gate verde exit 0; rojo inducido mutando 1 byte de `Flink-Job.jpeg` → exit 1 "captura reemplazada o ausente", restaurado y verde; fail-closed del generador probado con captura ausente → `FileNotFoundError`. Claim boundary: las capturas documentan la sesión del owner sobre el job real `608ee13c…`; son evidencia visual aportada, no generada por el pipeline, y no prueban por sí mismas ejecución reproducible) |

## Estado verificado

**Verificado por gate (2026-07-17, sobre el build vigente):**

- SC-001..SC-005, SC-007 — `deliverables_check.py` en verde (exit 0) sobre
  `deck/build/kcd2026.pptx`, build vigente de 43 slides (tras los anexos
  T015/T016): pin del template fijado y verificado
  (`sha256:6403764c…ce82a3`), 0 placeholders en 43 slides, 3 extractos de log
  idénticos a la evidencia (slides 28, 30 y 32 vía `build/mapa.txt`), slide 33
  "Qué NO demostramos" presente, 41/43 notas del orador sobre el umbral 30
  (sin nota solo la portada y el cierre), y salida verde que declara la
  verificación estructural. Rojo demostrado en la corrida inicial (sobre el
  build de 35 slides de entonces): con `TBD-DIGEST` y sin
  `adr-0015-red-gate.json` salió 1 nombrando ambas ausencias.
  `evidence/sc-001-007-deliverables-gate.json`.
- SC-003 — el extracto `proposed: 1` verifica contra
  `evidence/adr-0015-red-gate.json` (T010), reconstrucción registrada del
  historial de `edaiosv` (commit `274812f`) con su claim boundary: documenta el
  ciclo registrado, no una re-ejecución del gate rojo del baseline.
- SC-006 — `generate_use_case_page.py --check` en verde (exit 0, propagado por
  el gate); rojo inducido con mutación de un byte del HTML
  (`150.0`→`151.0`, offset 18301) → exit 1; restaurado y de vuelta en verde.
- SC-006 (vista OS, T014) — la vista reducida del sistema operativo
  (`docs/edaios-operating-system-flink.html`, ADR-0015: vive en el consumer) es
  byte-idéntica a su recompilación: dos corridas del generador producen el
  mismo sha256, `--check` en verde (exit 0) propagado por `deliverables_check.py`
  vía `contract.page_os`; rojo inducido con mutación de un byte
  (`150.0`→`151.0`, offset 16100) → exit 1; restaurado.
  `evidence/t014-os-flink-page.json`.
- SC-008 — `validate.py` del entorno sobre `deck/build/kcd2026.pptx` con
  `--original deck/template.pptx`: "All validations PASSED!", exit 0.
  `evidence/sc-008-ooxml-validate.json`.

- Capturas (T016) — sobre el build regenerado de 43 slides (2026-07-17): las 4
  capturas del owner (anexos A4/A5, slides 39–42) son byte-idénticas a su pin
  `contract.captures` y a los originales entregados; pipeline de 4 pasos
  exit 0, `validate.py --original` PASSED (exit 0), `deliverables_check.py`
  verde (exit 0) incluyendo `check_captures`; deck de 43 slides, anexos
  a1 51 + a2 8 + a3 10. Rojo inducido: 1 byte mutado en `deck/anexos/Flink-Job.jpeg`
  → exit 1 nombrando el archivo ("captura reemplazada o ausente");
  restaurado, verde. Fail-closed del generador probado (captura ausente →
  `FileNotFoundError` con acción correctiva). `evidence/t016-capturas.json`.

**Pendiente:**

- [SEAL] — la revisión, el commit/push y la publicación del deck siguen
  pendientes de firma humana (ADR-0015, invariante 6). Motivo: un gate en
  verde verifica; no autoriza.

El gate corre sobre el build vigente: cada regeneración futura debe volver a
pasar `deliverables_check.py` y `validate.py` antes de darse por terminada.

## Frontera del claim de esta matriz

La matriz demuestra la cadena SC → FR → tarea → test → gate para los dos
entregables, cerrada por gate sobre un build local: el deck se regenera, valida
OOXML, contiene frontera y guion, sus extractos de log coinciden con evidencia
versionada, y la página del caso es byte-idéntica a su recompilación.

**No demuestra** determinismo byte a byte del `.pptx` (frontera declarada,
SC-007), re-ejecución del gate rojo del baseline de `edaiosv` (la evidencia
`adr-0015-red-gate.json` es reconstrucción del historial), autorización de
publicación del deck, derechos sobre el template de KCD, ni nada sobre la
ejecución en vivo de la charla. Las capturas de los anexos A4/A5 documentan
la sesión del owner sobre el job real `608ee13c…`: son evidencia visual
aportada, no generada por el pipeline, y no prueban por sí mismas ejecución
reproducible. Un deck que valida en una laptop no es un deck
autorizado a mostrarse: esa autorización es humana, separada, y con evento y
versión declarados (ADR-0015).
