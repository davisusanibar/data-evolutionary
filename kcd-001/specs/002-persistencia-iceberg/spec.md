---
id: 002-persistencia-iceberg
estado: Propuesto
fase: specified
dominio: data-streaming
tramo_sensibilidad: T0
owner: david-susanibar
tipo_cambio: architecture
trazas:
  - ADR-0001
  - RFC-0001
spec_tipada: specs/002-persistencia-iceberg/feature.spec.yaml
fuentes:
  - pom.xml
  - specs/002-persistencia-iceberg/evidence/sources.md
value_ledger: "N/A: demo KCD; el ledger de valor no esta instalado en este consumer"
hipotesis_valor: Persistir el revenue agregado en Iceberg lo hace consultable por lotes sin recomputar el stream.
---

# Persistencia en Iceberg del revenue agregado (stub)

## Contexto

La feature `001-revenue-ventana-cliente` dejó anotada la persistencia en
Iceberg como posible feature posterior, fuera de su alcance. Este stub la
declara como cambio de **arquitectura**: añade una capa de almacenamiento
analítico al caso de uso (hoy el resultado solo vive en un tópico Kafka) y
obliga a decidir catálogo (Hive, HDFS o REST) y estrategia de tabla (CoW/MoR),
decisiones ya exploradas en `data-cdc-kafka-flink-iceberg` pero no canonizadas
para este consumer.

Nota de gobierno: esta spec existe además para ejercitar el check del gate
"cambio estructural respaldado por ADR" en sus dos estados (rojo sin traza
ADR, verde con ella).

## Alcance (preliminar)

- Sink adicional del resultado agregado hacia una tabla Iceberg sobre la
  infraestructura existente de `infra/dockercompose`.
- Fuera de alcance: modificar el job de la feature 001; cambiar el contrato
  de salida.

## Requisitos funcionales

- **FR-001** — El resultado agregado (`CustomerRevenueWindow`) se persiste en
  una tabla Iceberg además del tópico Kafka, sin bifurcar el contrato.
- **FR-002** — La tabla es consultable por lotes y sus filas coinciden con lo
  publicado en el tópico para las mismas ventanas.

## Criterios de éxito

- **SC-001** — Una consulta por lotes sobre la tabla Iceberg devuelve, para el
  fixture de la feature 001, exactamente la tabla de referencia.
- **SC-002** — El gate SDD queda verde sobre esta feature con su traza ADR
  declarada, y en rojo si se retira.
