# Tareas — Deck KCD 2026 y página del caso, regenerables por contrato

Formato: `[TNNN] [FR-NNN] acción y ruta`. Cada FR tiene al menos una tarea; no
hay tarea sin requisito que la justifique.

## Deck (proyección renderizada)

- [x] [T001] [FR-001] construir la secuencia de 35 slides duplicando layouts del
  template oficial en `data-kcd2026/deck/build_estructura.py`, sin modificar
  `template.pptx`
- [x] [T002] [FR-002] llenar todo el contenido por generador en
  `data-kcd2026/deck/llenar.py` (importa `contenido_1`, `contenido_diagramas`,
  `contenido_2`); ningún cambio se aplica al binario
- [x] [T003] [FR-003] transcribir los logs reales en `contenido_2.py`: canary
  (slide 28), e2e (slide 30), gate rojo del ADR (slide 32), desde la evidencia
  de la spec 001 y el historial de ADR-0015
- [x] [T004] [FR-004] slide 33 "Qué NO demostramos" en `contenido_2.py` con los
  límites explícitos (fiscal, producción, outcome)
- [x] [T005] [FR-005] notas del orador en `contenido_1.py`,
  `contenido_diagramas.py` y `contenido_2.py` — 33 de 35 slides con guion
- [x] [T006] [FR-008] rebuild completo:
  `build_estructura.py && llenar.py && render.py` produce
  `build/kcd2026.pptx` y `build/render/s-NN.png`
- [x] [T007] [FR-008] validación OOXML contra el template con el `validate.py`
  del entorno — "All validations PASSED!", exit 0 sobre el build actual

## Contrato y gate de entregables

- [x] [T008] [FR-001] [FR-007] declarar el contrato en `feature.spec.yaml`:
  `template_sha256: TBD-DIGEST`, `log_extracts`, título del slide de frontera,
  umbral de notas y `determinism: structural` para el deck
- [x] [T009] [FR-001] [FR-002] [FR-003] [FR-004] [FR-005] [FR-007] implementar
  el gate en `data-kcd2026/tools/deliverables_check.py`: digest del template
  (calcula y exige fijar `TBD-DIGEST`; rojo si difiere una vez fijado), scan de
  placeholders = 0, extractos de log vs evidencia nombrando el slide
  divergente, presencia del slide "Qué NO demostramos", notas ≥ 30/35, y
  salida que declara verificación estructural, no byte a byte — la primera
  corrida calculó `sha256:6403764c…ce82a3` y el pin quedó fijado en
  `feature.spec.yaml`; evidencia en
  `evidence/sc-001-007-deliverables-gate.json`
- [x] [T010] [FR-003] registrar el gate rojo del ADR como evidencia versionada
  en `data-kcd2026/specs/002-conference-deck-and-use-case-page/evidence/adr-0015-red-gate.json`
  (fuente: historial de `edaiosv`, commit `274812f`, `day_zero_demo_check.py`
  con `'proposed': 1` durante la propuesta de ADR-0015; ver Clarification 4) —
  registrada como reconstrucción del historial con su claim boundary

## Anexos HTML en el PDF entregable

- [x] [T015] [FR-007] [FR-008] anexos HTML del deck: el pptx presenta, el PDF
  documenta. `deck/anexar.py --paginas` pagina las 3 vistas HTML con Chrome
  headless (`aN.pdf`, texto seleccionable) y exporta `preview-aN.png` (150 dpi)
  para los covers; `deck/contenido_anexos.py` añade el divisor "Anexos" (slide
  35) y 3 covers sobre el layout lienzo (slides 36–38, preview + fuente +
  remisión al anexo independiente `aN.pdf`), fallando cerrado si falta un
  preview; cada `aN.pdf` es un PDF paginado por Chrome (a1 51, a2 8, a3 10
  páginas). Contrato: `contract.annexes` con
  `determinism: structural` (un PDF de Chrome lleva metadatos de fecha; misma
  frontera que el pptx, ADR-0015 inv. 5). Gate: `deliverables_check.py`
  verifica fuentes y que cada aN.pdf tenga páginas; rojo inducido
  (a2.pdf ausente → exit 1) y revertido. Evidencia:
  `evidence/t015-anexos-pdf.json`
- [x] [T016] [FR-007] anexos A4 y A5: **4** capturas del owner (no 3: llegaron
  dos vistas de engram), entregadas en
  `~/Documents/ddsa/wks/edaios_tmp/claude/anexos/` y copiadas TAL CUAL (byte a
  byte, sin recortar ni retocar) a `deck/anexos/` como fuentes binarias del
  generador. Slides 39–42 sobre el layout lienzo (origen slide4), tras los
  covers y antes de gracias (deck 39→43): A4 = detalle del job de Flink
  (`Flink-Job-ID.jpeg`, Job ID `608ee13c…` = el de la evidencia
  `sc-002-004-e2e-compose.json`, operadores nombrados por FR) + overview del
  dashboard (`Flink-Job.jpeg`, con su "Failed 1" a la vista: el intento muerto
  por la KryoException del slide 29); A5 = TUI de engram con las 8
  observaciones de la sesión (`Engram-Logs.png`, "Observations (8)", #6–#13)
  + detalle de la Observación
  #7 "tres bugs que los tests verdes no atraparon" (`Engram-Logs-ID.png`).
  `contenido_anexos._captura` falla cerrado si falta un archivo. Contrato:
  `contract.deck.slides: 43` y `contract.captures` con sha256 REAL por
  captura; `deliverables_check.check_captures` pone el gate en rojo si una
  captura falta o no coincide con su pin ("captura reemplazada o ausente" —
  rojo inducido con mutación de 1 byte, exit 1, restaurado). Evidencia:
  `evidence/t016-capturas.json`

## Página del caso

- [x] [T011] [FR-006] declarar la fuente de la página en
  `data-kcd2026/docs/edaios-operating-system-flink-use-case.config.json`
  (la ruta prevista `use_case_page.config.yaml` cambió a JSON al implementar;
  `contract.page.config` en `feature.spec.yaml` quedó alineado y el gate
  verifica que resuelva)
- [x] [T012] [FR-006] implementar
  `data-kcd2026/tools/generate_use_case_page.py`: genera la página desde la
  config y `--check` compara byte a byte contra el árbol
- [x] [T013] [FR-006] generar
  `data-kcd2026/docs/edaios-operating-system-flink-use-case.html` y probar
  `--check` en verde (exit 0) y en rojo con mutación de un byte (exit 1) —
  mutación `150.0`→`151.0` en el offset 18301, exit 1 propagado por el gate,
  restaurado y de vuelta en verde (`evidence/sc-001-007-deliverables-gate.json`)
- [x] [T014] [FR-006] vista reducida del sistema operativo EDAIOS aplicada al
  caso Flink, viviendo en el consumer (ADR-0015): fuente
  `docs/edaios-operating-system-flink.config.json`, generador solo-stdlib
  `tools/generate_os_flink_page.py` (mismo patrón que T012: determinista,
  `--check` byte a byte) y derivado `docs/edaios-operating-system-flink.html`
  con los 7 artículos de la constitución de Core vs el Constitution Check del
  plan de la spec 001, los 15 gates de Core + 2 del consumer, el ciclo SDD de
  las specs 001/002, ADR-0015 y la frontera de claims; `deliverables_check.py`
  extendido con `contract.page_os` — verde exit 0, rojo inducido con mutación
  de un byte (`150.0`→`151.0`, offset 16100) exit 1 propagado por el gate,
  restaurado (`evidence/t014-os-flink-page.json`)

## Cierre

- [x] [GATES] `deliverables_check.py` y `generate_use_case_page.py --check` en
  verde sobre el árbol regenerado (2026-07-17, exit 0 ambos;
  `evidence/sc-001-007-deliverables-gate.json`)
- [x] [LEDGER] value ledger: `N/A` — material de conferencia; no hay outcome
- [ ] [SEAL] preparar el cambio para revisión; commit y push exigen
  autorización del owner, y la **publicación del deck es un permiso separado**
  con evento y versión declarados (ADR-0015)

## Estado

T001–T016 y [GATES] cerradas: el deck de 43 slides y sus anexos
independientes (`aN.pdf`: a1 51 + a2 8 + a3 10 páginas) se regeneran
completos desde el generador; las 4 capturas del owner
(anexos A4/A5, slides 39–42) se insertan tal cual y quedan pineadas por
sha256 en `contract.captures`. El deck
valida OOXML contra el template (exit 0, `evidence/sc-008-ooxml-validate.json`)
y el gate `deliverables_check.py` sostiene SC-001..SC-005 y SC-007 en verde,
con rojo inducido y revertido para SC-006. El pin del template quedó fijado
(`sha256:6403764c…ce82a3`) y los tres extractos de log verifican contra
evidencia versionada. La fase pasa a `implemented`. Queda abierta [SEAL]:
commit, push y publicación del deck son permisos humanos separados (ADR-0015);
un gate en verde no es una autorización.
