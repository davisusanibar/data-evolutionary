# Plan — 001-revenue-ventana-cliente

**Spec**: [spec.md](spec.md) · **Spec tipada**: [feature.spec.yaml](feature.spec.yaml)

## Summary

Job Flink que agrega revenue por cliente sobre ventanas tumbling de event-time
de 1 minuto (tolerancia a tardíos de 5s), leyendo un tópico Kafka de órdenes y
emitiendo el resultado a un tópico propio, con contratos Avro versionados que
generan las clases Java del job en el build.

## Technical Context

**Language/Version**: Java 11 — alineado con `data-cdc-kafka-flink-iceberg`
(`maven.compiler.source`/`target` = 11); `data-kcd2026/pom.xml` aún no lo
declara, se añade en la tarea de build.

**Primary Dependencies**: `flink-streaming-java` y `flink-connector-kafka`
(mismo patrón que `data-kcd2025`); `flink-avro` y `flink-avro-confluent-registry`
para (de)serializar contra el Schema Registry; `org.apache.avro:avro` +
`avro-maven-plugin` para generar las clases desde `.avsc`. Versión de Flink
1.20.2 y Avro 1.11.3, alineadas con `data-cdc-kafka-flink-iceberg` para no
introducir una tercera versión de Flink en el repositorio.

**Storage**: N/A — el job no persiste estado más allá del estado interno de
Flink durante la ventana (fuera de alcance: Iceberg, según `spec.md`).

**Testing**: JUnit 5 (Jupiter). Es una decisión nueva de este módulo: no hay
convención de testing previa en `data-kcd2026` (`data-cdc-kafka-flink-iceberg`
solo declara JUnit 3.8.1 en scope test, sin uso visible).

**Target Platform**: JVM, ejecutable en Flink Standalone en el host o contra
el `flink-jobmanager`/`flink-taskmanager` de `infra/dockercompose`, mismo
patrón que los demás módulos del repositorio.

**Project Type**: módulo Maven único dentro del monorepo multi-módulo
existente (`data-kcd2026`), sin proyectos adicionales.

**Performance Goals**: N/A — no se declara ni se cita ningún umbral de
throughput o latencia (Principio IV: cero cifras sin fuente).

**Constraints**: N/A funcional; el único límite de tiempo es el de la propia
presentación de la demo, que no es un requisito del job.

**Scale/Scope**: fixture sintético pequeño para SC-001; sin carga de
producción ni datos reales.

## Constitution Check

Verificación de los siete principios de la constitución operativa proyectada
antes de autorizar la implementación.

| Principio | Veredicto | Evidencia |
|---|---|---|
| I. El conocimiento manda | PASS | Los contratos `.avsc` (a crear) preceden al job; las clases Java se derivan del contrato por generación (`avro-maven-plugin`), no al revés. |
| II. Spec antes que artefacto | PASS | `spec.md` declara FR-001..FR-004, SC-001..SC-003 y owner antes de que exista una sola clase del job; `pom.xml` del módulo sigue sin dependencias de Flink/Kafka/Avro. |
| III. El canon crece por decisión | N/A | No hay cambio estructural del canon: es una feature de dominio bajo el perfil `consumer-release` ya aceptado en ADR-0016. |
| IV. Cero cifras sin fuente | PASS | La única cifra de negocio (revenue por ventana) se verifica en SC-001 contra un cálculo de referencia sobre un fixture sintético determinista versionado; no se cita ninguna cifra real. |
| V. Una fuente, muchas vistas | PASS | El `.avsc` es la fuente única de cada contrato; las clases Java se regeneran desde él en `generate-sources` y no se editan a mano. |
| VI. La IA consume; el humano firma | PASS | La feature queda en fase `planned`, estado `Borrador`; el paso a `implemented`/`Cerrado` exige la firma del owner declarado (`romaria85`). |
| VII. Privacidad por diseño | PASS | Tramo `T0` declarado y coherente con `.edaios/policies/sensitivity-t0.json`: solo fixtures sintéticos, sin PII ni datos de producción. |

Constitucion verificada: sha256:c05dfd28b564bcfa8fbda15ae8db0a12b6f169f00f9057fbea5e4a0fca9892a3

## Arquitectura de la solución

El job vive en el módulo `data-kcd2026` y reutiliza la infraestructura ya
existente en `infra/dockercompose` (servicios `broker` y `schema-registry`,
confirmados en `infra/dockercompose/docker-compose.yml`) sin añadir servicios
nuevos.

Flujo de datos:

1. **Fuente** — tópico Kafka de órdenes, deserializado contra el Schema
   Registry (`http://registry:8081` dentro de la red `topaya`), siguiendo el
   mismo patrón que `e_cdckafkaflink.JobStreamingCDCKafkaFlink` en
   `data-cdc-kafka-flink-iceberg`.
2. **Watermarks** — `WatermarkStrategy.forMonotonousTimestamps()` derivada del
   timestamp de evento de la orden; la tolerancia a tardíos vive en el propio
   mecanismo de ventana (punto 3), no en el watermark.
3. **Agregación** — `keyBy` por identificador de cliente sobre
   `TumblingEventTimeWindows.of(Time.minutes(1))` con
   `.allowedLateness(Time.seconds(5))` — la orden puede llegar hasta 5s después
   de que el watermark cruce el cierre de su ventana, tal como quedó decidido
   en `## Clarifications` de `spec.md`. Agrega importe total y conteo de
   órdenes en un único `AggregateFunction`, evitando materializar la ventana
   completa en estado.
4. **Sink** — tópico Kafka de resultado, serializado contra el contrato de
   salida vía el Schema Registry.

Contratos, en `src/main/resources/model/` (mismo directorio que usa
`avro-maven-plugin` en `data-cdc-kafka-flink-iceberg`):

- Contrato de entrada: identificador de cliente, importe, timestamp de evento.
- Contrato de salida: identificador de cliente, inicio y fin de ventana,
  revenue acumulado, número de órdenes.

## Project Structure

```text
specs/001-revenue-ventana-cliente/
├── spec.md
├── feature.spec.yaml
├── checklists/requirements.md
├── plan.md              # este archivo
└── tasks.md             # /speckit.tasks (siguiente fase)

data-kcd2026/
├── pom.xml                                   # añade flink/kafka/avro + avro-maven-plugin
├── src/main/resources/model/
│   ├── orden.avsc
│   └── revenue-ventana.avsc
├── src/main/java/com/topaya/kcd2026/revenue/
│   └── RevenueVentanaClienteJob.java
└── src/test/java/com/topaya/kcd2026/revenue/
    └── RevenueVentanaClienteJobTest.java      # fixture determinista de SC-001
```

**Structure Decision**: módulo único (sin frontend/backend separados); el
paquete `revenue` queda reservado para esta feature dentro de
`com.topaya.kcd2026`, sin tocar `App.java` ni las carpetas de otras features
futuras.

## Gate Impact

Impacto sobre las puertas existentes, declarado antes de implementar:

- **`.specify/gates.json`** — ausente en este consumer, de forma intencional:
  el perfil `consumer-release` (ADR-0016) es una raíz liviana que no exige el
  registro de 15 gates del monorepo de Core. El registro canónico de esta
  feature es el gate vendorizado en `tools/validation/spec_kit_gate.py`.
- **Puerta de compilación (contrato Avro)** — pasa a ser significativa en
  este módulo, que hasta ahora no declaraba Avro. Añadir `avro-maven-plugin`
  y las dependencias Avro hace que cualquier deriva de contrato sea un fallo
  de build, verificado por SC-002.
- **Gate SDD `consumer-release`** — esta es la primera feature bajo `specs/`
  del consumer. El gate deja de reportar "sin features versionadas" y pasa a
  validar contrato, cobertura y cierre de esta feature. Verificado por SC-003.
- **Sin impacto** sobre el caso CDC + Join de 2025 (`data-cdc-kafka-flink-iceberg`):
  no se modifica su módulo, su contrato ni su configuración de build.
- Sigue sin existir hook `pre-push` ni CI en el repositorio: ambas puertas
  (compilación y SDD) se ejecutan hoy por invocación explícita, no
  automática.

## Secuencia de implementación

1. Contratos y build primero (FR-004): `.avsc` de entrada/salida y
   `avro-maven-plugin` en `pom.xml`, antes de escribir el job.
2. Job de agregación (FR-001, FR-002, FR-003) contra el fixture sintético.
3. Verificación numérica (SC-001), incluidos los dos casos de evento tardío.
4. Compilación limpia del módulo (SC-002).
5. Gate SDD sobre la feature (SC-003).
