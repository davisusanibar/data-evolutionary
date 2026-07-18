# Ingesta de artefactos — feature 001-revenue-ventana-cliente (borrador, no promovido)

Fecha: 2026-07-18 · Estado: `unverified` · Attachment: kcd-001 (draft T0)

Artefactos producidos por la implementación de la feature, registrados como
borradores bajo el attachment. Ninguno se promueve: la promoción y el cierre
son decisiones del owner (Artículo VI).

## Artefactos de conocimiento

- `specs/001-revenue-ventana-cliente/` — spec, contrato tipado, plan, checklist,
  tasks, matriz de verificación y evidencia SC-001..SC-006.

## Artefactos de software

- `src/main/resources/model/order_event.avsc` — contrato de entrada (fuente única).
- `src/main/resources/model/customer_revenue_window.avsc` — contrato de salida.
- `src/main/java/com/topaya/kcd001/revenue/` — job y pipeline (6 clases).
- `src/test/java/com/topaya/kcd001/revenue/RevenueWindowPipelineTest.java` — SC-001.
- `src/test/resources/fixture/` — fixture determinista y tabla de referencia.
- `canary-deriva.sh` — canary de deriva de contrato (FR-005).
- `pom.xml` — build con avro-maven-plugin (puerta de compilación).
- `README.md` — operación del módulo.

## Vínculo de valor (LEDGER)

El consumer no tiene ledger de valor instalado. El vínculo declarado en la spec:
hipótesis de valor = "hacer tangible en 30 segundos que un contrato de datos
derivado se detecta en el build y no en producción". Resultado medido: canary
completo en 5 s (evidencia sc-004), muy por debajo del umbral de 30 s.
