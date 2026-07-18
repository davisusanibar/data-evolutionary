# ADR-0001 — Patrones Flink obligatorios del consumer kcd-001

**Estado:** Aceptado · **Fecha:** 2026-07-18 · **Owner:** david-susanibar
**Aceptación:** instruida por el owner en sesión el 2026-07-18; declaración
literal: "Procede con el ADR-0001 y el stub 002". La propuesta de los tres
patrones es requerimiento previo del propio owner.

## Contexto

La feature `001-revenue-ventana-cliente` demostró tres patrones de Flink en
código real, con evidencia verificada (SC-001..SC-006). Sin canon, cada feature
futura podría reabrir esas decisiones y degradar en silencio lo que la demo
prueba: reproducibilidad del resultado y deriva de contrato atrapada en el
build. El principio III de la constitución ("el canon crece por decisión")
exige que una regla estructural viva en un ADR aceptado, no en la costumbre.

## Decisión

Toda feature de este consumer que implemente procesamiento de streams declara
y cumple los tres patrones:

1. **Event-time con watermarks y política de lateness declarada.** Las cifras
   que alguien pueda leer como negocio se calculan sobre ventanas de tiempo de
   evento, con watermarks derivados del timestamp del evento y una política de
   eventos tardíos explícita en el código. Processing-time queda prohibido para
   resultados de negocio (produce cifras distintas en cada ejecución; ver
   `specs/001-revenue-ventana-cliente/plan.md`, decisión "event-time y no
   processing-time").

2. **Agregación incremental.** Las ventanas agregan con `AggregateFunction`
   (combinada con `ProcessWindowFunction` solo para adjuntar metadatos de la
   ventana cerrada). Materializar la ventana completa en estado queda
   prohibido. Referencia: `RevenueAggregateFunction` + `AttachWindowMetadata`
   de la feature 001.

3. **El contrato genera el código.** Los esquemas de datos viven como `.avsc`
   versionados y generan las clases en el build (`avro-maven-plugin`). El
   código accede a los campos por los accesores generados; el acceso dinámico
   por nombre (`record.get("campo")`) queda prohibido, porque anula la
   detección de deriva en compilación que el canary demuestra
   (`canary-deriva.sh`, FR-005 de la feature 001).

## Consecuencias

- Una feature con `tipo_cambio: architecture|governance|ontology` debe trazar
  a un ADR; el gate lo verifica fail-closed ("cambio estructural respaldado
  por ADR").
- Las features de dominio (`tipo_cambio: feature`) declaran este ADR en
  `trazas:` cuando implementen streams, y la revisión de implementación
  verifica los tres patrones.
- Apartarse de un patrón exige enmendar este ADR o crear uno nuevo que lo
  supersede — nunca una excepción tácita en el código.

## Alternativas consideradas

- **Declararlo solo en CLAUDE.md / memoria del agente** — descartado: la
  memoria orienta pero no gobierna; no hay gate que la verifique y no exige
  decisión del owner para cambiarla.
- **Un RFC** — descartado como destino final: el RFC explora alternativas;
  estos tres patrones ya están decididos y demostrados con evidencia. Un RFC
  sería el camino si se quisiera explorar un cuarto patrón (p. ej. estrategia
  de checkpointing, hoy sin verificar).
