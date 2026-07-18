# Checklist de calidad de requisitos — 001-revenue-ventana-cliente

Revisión aplicada sobre la spec antes de planificar. Cada ítem se marca solo
cuando se verificó contra el texto real de la spec, no por intención.

## Claridad y verificabilidad

- [x] Cada FR describe un comportamiento observable, no una implementación concreta.
- [x] Cada SC es medible por alguien distinto de quien lo escribió.
- [x] SC-004 declara un umbral numérico explícito (30 segundos) en lugar de "rápido".
- [x] SC-001 declara contra qué se compara el resultado (cálculo de referencia sobre fixture determinista).
- [x] SC-006 declara el escenario end-to-end (infra real de `infra/dockercompose`) y contra qué se compara.
- [x] No quedan campos sin resolver ni marcadores pendientes en la spec.

## Alcance

- [x] El alcance declara explícitamente qué queda fuera, no solo qué entra.
- [x] Se excluye modificar los casos de `data-cdc-kafka-flink-iceberg` de KCD 2025, evitando regresión sobre lo ya demostrado.
- [x] Se excluye levantar infraestructura nueva, y FR-006 lo convierte en requisito verificable.
- [x] La persistencia en Iceberg queda fuera y anotada como posible feature posterior.

## Contrato y datos

- [x] El contrato de entrada y el de salida están ambos declarados (FR-001, FR-003).
- [x] FR-004 exige que el contrato genere código, que es lo que hace posible el canary.
- [x] El tramo de sensibilidad es T0 y la spec solo usa fixtures sintéticos, sin PII ni datos productivos.
- [x] La política de eventos tardíos se declara como parte de FR-002 y no se deja implícita.

## Coherencia de la demo

- [x] La spec distingue explícitamente la puerta de compilación de la puerta SDD, sin mezclarlas.
- [x] SC-003 verifica la puerta de compilación y SC-005 la puerta SDD, cada una por separado.
- [x] El canary es reversible (FR-005), de modo que la demo puede repetirse sin dejar el repositorio roto.
- [x] La decisión de usar event-time está justificada contra la alternativa descartada.
