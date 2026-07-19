# ADR-0001 — Submódulo Maven `data-kcd2026` para la demo Flink/Kafka de KCD 2026

**Estado:** Aceptado
**Fecha:** 2026-07-18
**Fecha de aceptación:** 2026-07-18
**Owner:** David Dali Susanibar Arce

## Contexto

El monorepo `com.topaya:oss` agrupa cuatro módulos con propósitos distintos:
`data-hadoop`, `data-spark`, `data-cdc-kafka-flink-iceberg` y `data-kcd2025`.
El módulo `data-kcd2025` materializó la demo de la edición anterior del evento y
quedó fijado a las versiones de esa fecha (Flink 1.20.1, conector Kafka
3.3.0-1.20).

Para la edición 2026 se necesita una superficie de demostración propia. Reusar
`data-kcd2025` obligaría a mutar un artefacto que ya representa una entrega
pasada, y mezclaría dos demos en el mismo historial de versiones.

## Decisión

Se agrega al reactor un submódulo independiente `data-kcd2026`:

- registrado en `<modules>` del `pom.xml` raíz;
- heredando el parent `com.topaya:oss:1.0-SNAPSHOT`, con `groupId`
  `com.topaya.kcd2026` y `packaging` jar;
- con `org.apache.flink:flink-streaming-java:1.20.1` y
  `org.apache.flink:flink-connector-kafka:3.3.0-1.20` declaradas con versión
  explícita;
- exponiendo `com.topaya.kcd2026.KafkaHello` como punto de entrada mínimo.

El alcance de esta decisión es estructural: registro del módulo, herencia y
dependencias declaradas. No cubre despliegue, topología de topics, cluster de
destino ni rendimiento.

## Alternativas

1. **Extender `data-kcd2025`.** Descartada: reescribe un módulo que representa
   una entrega ya realizada y acopla dos demos en el mismo ciclo de versiones.
2. **Repositorio separado fuera del monorepo.** Descartada por ahora: duplicaría
   la configuración del parent y el `mvnw` sin necesidad demostrada; el reactor
   ya aísla módulos correctamente.
3. **Añadir la demo dentro de `data-cdc-kafka-flink-iceberg`.** Descartada: ese
   módulo tiene un contrato CDC + Iceberg propio que la demo no comparte.

## Consecuencias

- El ciclo agregado del reactor incluye un módulo más; `mvn` en la raíz compila
  `data-kcd2026`.
- Las versiones de Flink quedan fijadas en dos lugares (`data-kcd2025` y
  `data-kcd2026`). Si en el futuro deben moverse juntas, se requerirá una
  decisión posterior que introduzca `dependencyManagement` en el parent; este
  ADR no la anticipa.
- Ningún módulo existente cambia su contrato.

## Evidencia y frontera del claim

Observación local del workspace, 2026-07-18, sobre Apache Maven 3.9.8:

- `mvn -q -pl data-kcd2026 -am validate` → exit 0.
- `mvn -pl data-kcd2026 dependency:list` → resuelve
  `flink-streaming-java:jar:1.20.1:compile` y
  `flink-connector-kafka:jar:3.3.0-1.20:compile`.
- `mvn -pl data-kcd2026 clean package` → exit 0, produce
  `data-kcd2026-1.0-SNAPSHOT.jar` con `com/topaya/kcd2026/KafkaHello.class`.

Frontera: la evidencia es una resolución local contra el repositorio `~/.m2` de
esta máquina. No demuestra comportamiento en un cluster Flink, no contacta un
broker Kafka, no mide rendimiento y no constituye validación de producción.

## Aprobación

| Campo | Valor |
|---|---|
| Actor | David Dali Susanibar Arce |
| Rol | `initiative-owner` / `approver` (`.edaios/authority-registry.json`) |
| Fecha | 2026-07-18 |
| Veredicto | Aceptado |
| Evidencia | `specs/001-data-kcd2026/evidence/` · SC-001..SC-004 observados en exit 0 |
| EvidenceReceipt | `.edaios/receipts/EVR-415bd62c715d.json` |
| ApprovalReceipt | `.edaios/approvals/APR-95854eb8b28f.json` |
| Plan de rollback | `specs/001-data-kcd2026/evidence/rollback-plan.json` |

Alcance de la aceptación: la decisión de estructura descrita arriba — registro
del módulo en el reactor, herencia del parent y dependencias declaradas. La
aceptación **no** extiende el claim a despliegue, cluster, topología de topics ni
rendimiento, que permanecen fuera de alcance.

El número es estable; una decisión posterior deroga, no borra ni reusa.
