---
id: KCD2026-CONFERENCE-DECK-AND-USE-CASE-PAGE
estado: Propuesto
fase: implemented
dominio: conference-deliverables
tramo_sensibilidad: T0
owner: Data & AI Lead
tipo_cambio: feature
trazas:
  - ADR-0015
  - ADR-0001
  - ADR-0002
spec_tipada: data-kcd2026/specs/002-conference-deck-and-use-case-page/feature.spec.yaml
fuentes:
  - data-kcd2026/deck/build_estructura.py
  - data-kcd2026/deck/llenar.py
  - data-kcd2026/deck/contenido_1.py
  - data-kcd2026/deck/contenido_diagramas.py
  - data-kcd2026/deck/contenido_2.py
  - data-kcd2026/specs/001-orders-revenue-window/evidence/sc-005-contract-gate.json
  - data-kcd2026/specs/001-orders-revenue-window/evidence/sc-002-004-e2e-compose.json
value_ledger: "N/A: material de conferencia; no hay outcome"
hipotesis_valor: Un entregable de conferencia que se regenera por contrato no puede derivar en silencio entre lo que la evidencia registró y lo que la audiencia ve
---

# Deck KCD 2026 y página del caso, regenerables por contrato

## Intención y alcance

Gobernar los dos entregables de la charla "Construyendo Data Pipelines con
Apache Flink y Spec Driven Development" (KCD Lima Perú 2026, David Susanibar y
Jorge Alor) como **proyecciones regenerables**: el deck de 35 slides construido
sobre el template oficial de KCD, y la página HTML del caso
`edaios-operating-system-flink-use-case.html`. Ninguno de los dos se edita a
mano: el deck se regenera desde su generador
(`build_estructura.py` → `llenar.py` → `render.py`) y la página desde su config.

El deck es una **proyección renderizada** en el sentido de ADR-0015: su fuente
es texto versionado y gobernado (el generador y esta spec); el `.pptx` es el
render, y el render vive en este consumer, no en EDAIOS Core. La página HTML es
texto, y por eso sí admite el contrato fuerte: comparación byte a byte con su
recompilación.

Quedan fuera: la autorización de publicación del deck (permiso separado por
ADR-0015), los derechos sobre el template (pertenecen a KCD), el contenido del
pipeline demostrado (gobernado por la spec 001) y cualquier claim sobre lo que
los oradores digan en vivo.

## Requisitos funcionales

- **FR-001**: el deck debe construirse llenando el template oficial de KCD
  (`deck/template.pptx`) por duplicación de sus layouts, sin modificar el
  template jamás. El template se pina por sha256 en `feature.spec.yaml`
  (`template_sha256`, valor inicial `TBD-DIGEST`: lo calcula y fija el gate en
  su primera corrida).
- **FR-002**: el deck es una proyección: editar `deck/build/kcd2026.pptx` a
  mano no es un cambio válido; todo cambio de contenido o estilo va al
  generador y se regenera. Una regeneración completa debe producir un deck sin
  restos de texto placeholder del template.
- **FR-003**: los bloques de log del deck son transcripciones de evidencia
  real, no maquetas. Los extractos clave deben coincidir con la evidencia
  registrada: del canary, el par de campos divergentes `sum_o_totalprice` /
  `total_revenue` (evidencia `sc-005-contract-gate.json` de la spec 001); del
  e2e, `sum_o_totalprice` 150.0 con `order_count` 2 (evidencia
  `sc-002-004-e2e-compose.json` de la spec 001); del gate rojo del ADR,
  `proposed: 1` (evidencia `adr-0015-red-gate.json` de esta spec, pendiente de
  registro; ver Clarification 4).
- **FR-004**: el deck declara su frontera: debe existir un slide "Qué NO
  demostramos" con los límites explícitos (sin exactitud fiscal, sin
  producción, sin outcome de negocio). Hoy es el slide 33
  (`frontera-claims-charla`).
- **FR-005**: guion del orador: al menos 30 de los 35 slides deben llevar
  notas del orador no vacías.
- **FR-006**: la página `edaios-operating-system-flink-use-case.html` es un
  derivado determinista de su config: `generate_use_case_page.py --check`
  recompila desde la config y compara byte a byte contra la página publicada
  en el árbol.
- **FR-007**: el `.pptx` **no** reclama determinismo byte a byte: es un
  contenedor ZIP cuyos timestamps y orden de entradas varían entre corridas
  (ADR-0015, invariante 5). Esa frontera queda declarada aquí y en
  `feature.spec.yaml`, y la verificación estructural del gate (placeholders,
  logs vs evidencia, slide de frontera, notas) es el sustituto honesto.
- **FR-008**: cada rebuild del deck debe pasar la validación OOXML contra el
  template con el `validate.py` del entorno antes de dar el deck por terminado.

## Criterios de éxito

- **SC-001**: con `template_sha256` fijado, un digest sha256 de
  `deck/template.pptx` distinto del declarado pone el gate
  `deliverables_check.py` en rojo (exit distinto de cero) nombrando ambos
  digests. Mientras el valor sea `TBD-DIGEST`, el gate lo calcula, lo informa
  y exige fijarlo.
- **SC-002**: tras un rebuild completo, un scan de textos placeholder del
  template sobre `build/kcd2026.pptx` encuentra exactamente 0 ocurrencias; una
  ocurrencia pone el gate en rojo nombrando el slide.
- **SC-003**: si el texto de un bloque de log del deck y la evidencia
  registrada divergen en un extracto clave de FR-003, el gate falla con exit
  distinto de cero **nombrando el slide** divergente.
- **SC-004**: la ausencia de un slide cuyo título sea "Qué NO demostramos" pone
  el gate en rojo.
- **SC-005**: menos de 30 slides con notas del orador no vacías pone el gate en
  rojo, informando cuántos y cuáles no tienen nota.
- **SC-006**: una mutación de un byte en
  `edaios-operating-system-flink-use-case.html` (o en su config sin regenerar)
  hace que `generate_use_case_page.py --check` termine con exit 1.
- **SC-007**: ningún check del gate compara `build/kcd2026.pptx` byte a byte
  contra un digest esperado del build, y la salida en verde del gate declara
  explícitamente que su verificación del deck es estructural, no byte a byte.
- **SC-008**: `validate.py` sobre `build/kcd2026.pptx` con `--original
  template.pptx` termina con exit 0 tras un rebuild completo; un exit distinto
  de cero bloquea la entrega.

## Clarifications

1. **El generador vive en el consumer, no en Core.** ADR-0015 fija que Core no
   adopta dependencias de terceros para renderizar vistas (`tools/` de Core es
   stdlib pura) y que el render de una proyección renderizada es
   responsabilidad de un consumer. `python-pptx`, PyMuPDF y LibreOffice viven
   aquí, en `data-kcd2026/deck/`, y no se propagan hacia Core. Core gobierna la
   fuente y los claims; este módulo produce la vista.
2. **No se reclama byte-determinismo del `.pptx`.** Un `.pptx` es un ZIP con
   timestamps y orden de entradas no deterministas; la igualdad byte a byte no
   es una propiedad natural del formato sino algo que habría que construir
   normalizando el contenedor (ADR-0015, invariante 5 y alternativa rechazada).
   Este consumer no construye esa normalización, y en su lugar declara la
   verificación estructural de SC-001..SC-005 como sustituto honesto. Un check
   byte a byte que fallara por timestamps enseñaría a ignorar el gate. La
   página HTML, en cambio, es texto: ahí el contrato byte a byte sí aplica
   (FR-006).
3. **La licencia del template pertenece a KCD.** Pinear `template.pptx` por
   sha256 verifica identidad del artefacto, no otorga derechos sobre él. Usar
   el template en el evento es parte del contrato del evento; publicarlo o
   redistribuirlo no está autorizado por esta spec, y la publicación del deck
   es un permiso separado de commit y push (ADR-0015, invariante 6).
4. **Procedencia del extracto `proposed: 1`.** Los extractos del canary y del
   e2e resuelven a `specs/001-orders-revenue-window/evidence/*.json`. El
   extracto del gate rojo del ADR proviene del historial de `edaiosv`
   (`day_zero_demo_check.py` durante la propuesta de ADR-0015, ciclo registrado
   en git); no existe aún como evidencia versionada en este módulo. La tarea
   T010 registra una copia como `evidence/adr-0015-red-gate.json` en esta spec
   para que el gate compare contra evidencia local. Hasta entonces, esa
   comparación está pendiente, no aprobada.
5. **"Extracto clave" no es igualdad de línea completa.** El deck abrevia
   líneas por legibilidad (por ejemplo `...avsc` en el slide del canary). El
   gate compara los extractos declarados en `feature.spec.yaml`
   (`log_extracts`), no la transcripción íntegra; el contenido de los logs no
   se altera, se recorta.

## Frontera de claims

T0 local. La feature puede demostrar: que el deck se regenera desde su
generador sin tocar el template y sin placeholders residuales, que los
extractos clave de sus logs coinciden con evidencia registrada, que el slide de
frontera y el guion del orador están presentes, que el `.pptx` valida OOXML
contra el template, y que la página HTML es byte-idéntica a su recompilación.

**No demuestra** determinismo byte a byte del `.pptx` (Clarification 2),
fidelidad de lo que se proyecte en el evento, autorización de publicación,
derechos sobre el template de KCD, exactitud de lo que los oradores afirmen en
vivo, ni nada sobre el pipeline demostrado más allá de lo que la spec 001 ya
acota. Un deck verificado no es un deck autorizado a publicarse: esa es una
decisión humana separada, con evento y versión declarados (ADR-0015).
