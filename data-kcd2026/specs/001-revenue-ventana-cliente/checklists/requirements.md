# Checklist de calidad de requisitos — 001-revenue-ventana-cliente

Revisión aplicada sobre la spec antes de planificar. Cada ítem se marca solo
cuando se verificó contra el texto real de `spec.md`, no por intención.

## Claridad y verificabilidad

- [x] Cada FR describe un comportamiento observable, no una decisión de implementación arbitraria.
- [x] Cada SC es medible por alguien distinto de quien lo escribió.
- [x] SC-001 declara contra qué se compara el resultado (cálculo de referencia sobre fixture determinista).
- [x] No quedan marcadores `NEEDS CLARIFICATION` pendientes en la spec (resueltos en `## Clarifications`).

## Alcance

- [x] El alcance declara explícitamente qué queda fuera, no solo qué entra.
- [x] Se excluye modificar el caso CDC + Join de 2025, evitando regresión sobre lo ya demostrado.
- [x] Se excluye levantar infraestructura nueva; el plan reutiliza `infra/dockercompose` existente.
- [x] La persistencia en Iceberg queda fuera de alcance explícitamente.

## Contrato y datos

- [x] El contrato de entrada y el de salida están ambos declarados (FR-001, FR-003).
- [x] FR-004 fija el formato del contrato (Avro `.avsc`) y exige que genere código, no que se copie a mano.
- [x] El tramo de sensibilidad es T0 y la spec deja explícito que "revenue" es un fixture sintético, no una cifra de negocio real.
- [x] La ventana (tumbling 1 minuto) y la tolerancia a eventos tardíos (5s) quedan declaradas de forma explícita en FR-002, no implícitas.

## Clarifications

- [x] Las tres preguntas formuladas en `/speckit.clarify` tienen respuesta registrada con su implicancia sobre los FR/SC afectados.
- [x] SC-001 se actualizó para cubrir el caso de evento tardío dentro y fuera de la tolerancia de 5s.
