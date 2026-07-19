---
id: DATA-EVOLUTIONARY-KCD2026-FLINK-KAFKA
estado: Propuesto
fase: tasked
dominio: data-evolutionary
tramo_sensibilidad: T0
owner: David Dali Susanibar Arce
tipo_cambio: architecture
trazas:
  - ADR-0001
spec_tipada: specs/001-data-kcd2026/feature.spec.yaml
fuentes:
  - pom.xml
  - data-kcd2026/pom.xml
  - data-kcd2026/src/main/java/com/topaya/kcd2026/KafkaHello.java
  - governance/ADR-0001-data-evolutionary-kcd2026-module.md
  - specs/001-data-kcd2026/evidence/sources.md
value_ledger: "N/A: modulo de demostracion para KCD 2026 sin outcome de adopcion medible en este tramo"
hipotesis_valor: Un submodulo Flink aislado y versionado permite demostrar ingesta streaming desde Kafka sin acoplar los modulos Hadoop, Spark ni CDC existentes del monorepo
---

# Submódulo Maven `data-kcd2026` · streaming Flink sobre Kafka

## Intención y alcance

Incorporar al monorepo `com.topaya:oss` un submódulo Maven independiente,
`data-kcd2026`, que fija Apache Flink 1.20.1 y el conector Kafka 3.3.0-1.20 y
expone un punto de entrada mínimo `com.topaya.kcd2026.KafkaHello`.

El alcance es la estructura del módulo, su registro en el reactor y sus
dependencias declaradas. Esta feature no despliega un job, no define topología
de topics, no fija un cluster Kafka ni Flink de destino, y no modifica los
módulos `data-hadoop`, `data-spark`, `data-cdc-kafka-flink-iceberg` ni
`data-kcd2025`.

La decisión que respalda el módulo es ADR-0001, aún no firmada.

## Requisitos

- **FR-001:** el reactor raíz `pom.xml` debe declarar `data-kcd2026` en
  `<modules>`, de modo que el módulo participe del ciclo de vida agregado del
  monorepo sin invocación individual.
- **FR-002:** el módulo debe heredar del parent `com.topaya:oss:1.0-SNAPSHOT` y
  declarar sus coordenadas propias con `packaging` jar, sin redefinir la versión
  del parent.
- **FR-003:** el módulo debe declarar de forma explícita y con versión fija las
  dependencias `org.apache.flink:flink-streaming-java:1.20.1` y
  `org.apache.flink:flink-connector-kafka:3.3.0-1.20`, sin rangos ni versiones
  heredadas implícitas.
- **FR-004:** el módulo debe exponer la clase `com.topaya.kcd2026.KafkaHello`
  con un `main` que obtenga un `StreamExecutionEnvironment`, como superficie
  mínima verificable de arranque del runtime de Flink.

## Criterios de éxito

- **SC-001:** `mvn -q -pl data-kcd2026 -am validate` resuelve el módulo desde la
  raíz del monorepo, probando que el reactor lo reconoce.
- **SC-002:** `mvn -pl data-kcd2026 dependency:list` muestra ambas dependencias
  Flink en las versiones fijadas, sin conflicto de mediación.
- **SC-003:** la clase `com.topaya.kcd2026.KafkaHello` existe en el artefacto
  compilado y declara el paquete `com.topaya.kcd2026`.
- **SC-004:** `mvn -pl data-kcd2026 clean package` termina con exit code 0 y
  produce el jar del módulo.

## Fuera de alcance

Despliegue, checkpointing, serialización de eventos, esquema de topics,
credenciales, observabilidad y cualquier promesa de rendimiento.
