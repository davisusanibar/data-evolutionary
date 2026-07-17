# Plan técnico — Deck KCD 2026 y página del caso, regenerables por contrato

**Feature:** KCD2026-CONFERENCE-DECK-AND-USE-CASE-PAGE
**Spec:** `data-kcd2026/specs/002-conference-deck-and-use-case-page/spec.md`
**Owner:** Data & AI Lead
**Autoridad estructural:** ADR-0015 (proyecciones renderizadas y superficie de publicación)

## Constitution Check

Verificado contra los 7 artículos de la Constitución EDAIOS. Un `VIOLA` detiene
el plan; el camino sería un ADR, no una excepción.

| # | Artículo | Veredicto | Evidencia |
|---|---|---|---|
| I | El conocimiento manda | PASS | La fuente del deck es texto versionado (generador + esta spec); `build/kcd2026.pptx` es un derivado que se regenera, no se edita. FR-002 declara inválido el cambio manual sobre el binario. |
| II | Spec antes que artefacto | PASS con salvedad declarada | El generador del deck precede a esta spec: la spec lo captura a posteriori como contrato de regeneración. Desde aquí, todo cambio va spec → generador → gate. El gate `deliverables_check.py` y la página HTML sí nacen después de esta spec. Ocultar el orden real sería peor que declararlo. |
| III | El canon crece por decisión | PASS | La frontera estructural que esta feature necesita (derivado binario, render en el consumer, publicación como permiso separado) fue decidida y firmada en ADR-0015. Esta feature la aplica; no introduce frontera nueva. |
| IV | Cero cifras sin fuente | PASS | Cada cifra del deck resuelve a evidencia registrada: `150.0` / `order_count 2` al e2e de la spec 001, los campos divergentes al gate de contrato de la spec 001, `proposed: 1` al historial de ADR-0015 (registro local pendiente, T010). FR-003 hace la coincidencia verificable por gate. |
| V | Una fuente, muchas vistas | PASS | Una fuente (generador + config) y varias vistas (`.pptx`, PDF, PNGs de render, página HTML). El gate verifica que las vistas no deriven de la fuente en lo verificable; donde la igualdad no es propiedad del formato, la frontera se declara (FR-007). |
| VI | La IA consume; el humano firma | PASS | Los gates verifican estructura y coincidencia; no aceptan. Commit, push y publicación del deck son permisos humanos separados (ADR-0015, invariante 6). `SEAL` queda abierta hasta la firma del owner. |
| VII | Privacidad por diseño | PASS | T0 declarado. El deck y la página muestran datos sintéticos TPC-H y logs de infraestructura local; ningún dato personal. Los nombres que aparecen (speakers) son públicos por el propio evento. |

**Constitución verificada:** 1.0.0 · pin no aplicable — este consumer no vive en
el árbol de Core y no puede resolver `constitution.md` localmente. La
verificación es manual y se registra aquí. Esa es la frontera honesta: el
consumer aplica los artículos, no los verifica mecánicamente.

## Gate Impact

| Gate | Efecto |
|---|---|
| `deliverables_check.py` | **Nuevo — pendiente (T009).** Verifica digest del template (SC-001), placeholders = 0 (SC-002), extractos de log vs evidencia nombrando el slide (SC-003), slide "Qué NO demostramos" (SC-004), notas ≥ 30/35 (SC-005) y declara frontera estructural (SC-007). Falla cerrado. |
| `generate_use_case_page.py --check` | **Nuevo — pendiente (T012).** Recompila la página desde su config y compara byte a byte (SC-006). Mismo patrón `destination.read_text() != content` de las proyecciones de Core. |
| `validate.py` (OOXML, skill del entorno) | Existente. Verifica `build/kcd2026.pptx` contra `template.pptx` (SC-008). Corre en cada rebuild; hoy en verde sobre el build actual. |
| `render.py` | No es un gate. Renderiza a PDF/PNG para inspección visual; ver que se ve bien no prueba que es correcto. |

## Enfoque técnico

1. **Proyección renderizada, no documento.** ADR-0015 divide el artefacto: la
   fuente y sus claims se gobiernan aquí (texto, spec, gate); el render y sus
   dependencias (`python-pptx`, PyMuPDF, LibreOffice) viven en este consumer y
   no se propagan a Core. Regenerar es siempre la secuencia completa
   `build_estructura.py` → `llenar.py` → `render.py`.
2. **Determinismo donde el formato lo permite.** La página HTML es texto: se
   exige igualdad byte a byte con su recompilación (FR-006). El `.pptx` es un
   ZIP no determinista: se exige verificación estructural y se declara la
   frontera (FR-007) en vez de fingir una garantía que el formato no da.
3. **La evidencia manda sobre la narrativa.** Los logs del deck no se redactan:
   se transcriben de la evidencia registrada, y `log_extracts` en
   `feature.spec.yaml` hace la coincidencia verificable. Si el deck y la
   evidencia divergen, gana la evidencia y el gate nombra el slide.
4. **El límite es parte del contenido.** El slide "Qué NO demostramos" es un
   requisito (FR-004), no una cortesía: un deck que muestra una demo verde sin
   declarar su frontera convierte la demo en el claim, que es la patología que
   la charla describe.
5. **`TBD-DIGEST` como estado explícito.** El pin del template nace sin valor
   para que el gate lo calcule y exija fijarlo, en lugar de copiar un digest a
   mano que nadie verificó. Un placeholder declarado es mejor que una cifra
   sin fuente.

## Alternativas consideradas

- **Editar `build/kcd2026.pptx` directamente:** rechazada; convierte el
  derivado en fuente, el siguiente rebuild pisa el cambio y nadie lo nota.
  FR-002 la declara inválida.
- **Comparar el `.pptx` byte a byte:** rechazada; la igualdad no es propiedad
  del contenedor ZIP y un check que falla por timestamps enseña a ignorar el
  gate (ADR-0015, alternativa rechazada).
- **Mover el generador a `edaiosv/tools/`:** rechazada; introduciría la primera
  dependencia de terceros en Core para una capacidad que no es de Core
  (ADR-0015, invariante 2).
- **Maquetar logs "limpios" en vez de transcribir evidencia:** rechazada;
  cifras sin fuente ante una audiencia que no puede leer las fuentes es
  exactamente lo que el Artículo IV prohíbe.
- **Exigir notas en 35/35 slides:** rechazada; portada, divisores y gracias no
  llevan guion sustantivo en todos los casos. 30/35 es el umbral honesto que
  el estado actual (33/35) supera sin holgura ficticia.
- **Publicar la página como demo de Core (`edaiosv/docs/demos/`):** rechazada;
  es una vista de consumer sobre un caso de consumer. Vive en
  `data-kcd2026/docs/` con su config y su check (ADR-0015, invariante 3).

## Frontera del claim del plan

El plan puede sostener que el deck es regenerable, estructuralmente verificado
y trazable a evidencia, y que la página es byte-determinista respecto de su
config. No sostiene determinismo byte a byte del `.pptx`, ni autorización de
publicación, ni derechos sobre el template, ni nada sobre la ejecución en vivo
de la charla. Los gates nuevos están especificados pero no implementados: hasta
T009 y T012, las verificaciones de SC-001..SC-007 son manuales y así se
registran.
