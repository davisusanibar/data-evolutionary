---
id: 001-revenue-ventana-cliente
estado: Cerrado
fase: implemented
dominio: data-streaming
tramo_sensibilidad: T0
owner: romaria85
tipo_cambio: feature
trazas:
  - ADR-0016
  - ADR-0003
spec_tipada: specs/001-revenue-ventana-cliente/feature.spec.yaml
fuentes:
  - pom.xml
  - edaios.initiative.json
  - README.md
value_ledger: "N/A: demo KCD2026; el ledger de valor no esta instalado en este consumer"
hipotesis_valor: Mostrar en la demo un pipeline Flink que agrega revenue por cliente en ventanas de event-time, gobernado con Spec-Driven Development antes de escribir codigo.
---

# Revenue por cliente en ventana temporal de event-time

## Clarifications

### Sesión 2026-07-18

- P: ¿Tamaño y tipo de ventana de agregación de event-time? → R: tumbling de
  1 minuto. Implicancia: la demo produce un resultado nuevo por cliente cada
  minuto de event-time; el fixture de referencia de SC-001 agrupa en
  intervalos de 1 minuto.
- P: ¿Tolerancia a eventos tardíos (allowed lateness)? → R: 5 segundos.
  Implicancia: el job acepta y reasigna a su ventana correcta cualquier orden
  cuyo timestamp de evento llegue hasta 5s después de que el watermark cruce
  el cierre de su ventana; el fixture de referencia de SC-001 debe incluir al
  menos un caso de evento tardío dentro de esa tolerancia y uno fuera de ella
  (descartado), para que el cálculo determinista cubra ambos caminos.
- P: ¿Formato del contrato de datos de entrada/salida? → R: Avro (`.avsc`),
  consistente con el patrón ya usado en `data-cdc-kafka-flink-iceberg`.
  Implicancia: FR-004 se resuelve con contratos `.avsc` versionados que
  generan clases Java en el build (`avro-maven-plugin`); SC-002 valida que esa
  generación sea parte del `compile`.

## Contexto

`data-kcd2026` es un submódulo nuevo (`pom.xml` sin dependencias, clase `App`
vacía) recién inyectado con el scaffold SDD de EDAIOS Core. Esta es su primera
feature.

El repositorio ya demuestra, en
[`../data-cdc-kafka-flink-iceberg`](../../data-cdc-kafka-flink-iceberg), un
caso CDC + Join operativo (`JobStreamingCDCKafkaFlink`): órdenes desde Kafka,
pagos desde Postgres vía CDC, join en ventana. Esta feature no lo modifica;
añade un caso de uso independiente centrado en agregación por cliente.

## Alcance

Entra en alcance:

- Un job Flink que agrega revenue por cliente sobre ventanas de event-time,
  leyendo un tópico Kafka de órdenes.
- Un contrato de datos declarado y versionado para la entrada y la salida del
  job.

Queda fuera de alcance:

- Modificar el caso CDC + Join existente.
- Persistir el resultado agregado en Iceberg.
- Levantar infraestructura nueva: se reutiliza `../infra/dockercompose`
  cuando la feature llegue a implementación.
- Un dashboard o UI de visualización del resultado.

## Requisitos funcionales

- **FR-001**: El job MUST consumir un tópico Kafka de órdenes donde cada
  evento declara, como mínimo, identificador de cliente, importe y timestamp
  de evento.
- **FR-002**: El job MUST calcular el revenue por cliente como la suma del
  importe de sus órdenes dentro de una ventana tumbling de event-time de 1
  minuto, con watermarks derivados del timestamp del evento y una tolerancia
  a eventos tardíos (allowed lateness) de 5 segundos; un evento que llegue
  después de esa tolerancia se descarta.
- **FR-003**: El job MUST emitir el resultado hacia un tópico Kafka propio,
  con un contrato de salida que incluya cliente, inicio y fin de ventana,
  revenue acumulado y número de órdenes de esa ventana.
- **FR-004**: Los contratos de entrada y salida MUST declararse como esquemas
  Avro (`.avsc`) versionados dentro del módulo, que generan las clases Java
  del job durante el build (`avro-maven-plugin`), de modo que el código
  dependa del contrato declarado y no de una estructura ad-hoc paralela.

### Entidades clave

- **Orden**: evento de entrada; cliente, importe, timestamp de evento.
- **Revenue por ventana**: resultado agregado; cliente, ventana (inicio/fin),
  revenue acumulado, número de órdenes.

## Criterios de éxito

- **SC-001**: Sobre un fixture sintético determinista que incluye al menos un
  evento tardío dentro de la tolerancia de 5s y uno fuera de ella, el revenue
  por cliente y ventana que produce el job coincide exactamente con el
  cálculo de referencia hecho aparte del job, incluido el descarte del evento
  fuera de tolerancia.
- **SC-002**: `mvn -pl data-kcd2026 -am compile` es verde con los contratos
  declarados íntegros.
- **SC-003**: El gate SDD (`tools/validation/spec_kit_gate.py . --profile
  consumer-release`) queda verde sobre esta feature.

## Supuestos

- La infraestructura de Kafka de `../infra/dockercompose` (ya usada por
  `data-cdc-kafka-flink-iceberg`) es reutilizable sin cambios para esta
  feature; no se valida aquí, se confirma en `/speckit.plan`.
- No hay owner de dominio de negocio real detrás de "revenue": es un fixture
  sintético para la demo, no una cifra reportable (Principio IV: cero cifras
  sin fuente — aquí no se cita ninguna).
