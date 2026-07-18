---
id: 001-revenue-ventana-cliente
estado: Propuesto
fase: tasked
dominio: data-streaming
tramo_sensibilidad: T0
owner: david-susanibar
tipo_cambio: feature
trazas:
  - ADR-0016
  - ADR-0003
spec_tipada: specs/001-revenue-ventana-cliente/feature.spec.yaml
fuentes:
  - README.md
  - pom.xml
  - docs/edaios-arquitectura-proyecto.svg
value_ledger: "N/A: demo KCD2026; el ledger de valor no esta instalado en este consumer"
hipotesis_valor: Hacer tangible en 30 segundos que un contrato de datos derivado se detecta en el build y no en produccion.
---

# Revenue por cliente en ventana temporal, con canary de deriva de contrato

## Contexto

En KCD 2025 este repositorio demostró un pipeline **CDC + Join** operativo
(`data-cdc-kafka-flink-iceberg`, `e_cdckafkaflink.JobStreamingCDCKafkaFlink`):
órdenes desde Kafka en Avro, pagos desde Postgres vía CDC, join en ventana y
sinks Avro de vuelta a Kafka. El pipeline funcionaba.

Lo que aquella demo no mostraba es **qué ocurre cuando el contrato de datos
deriva**. Un campo renombrado, un tipo cambiado, un productor desplegado antes
que su consumidor: en la mayoría de los pipelines eso se descubre en runtime,
con datos ya corruptos.

Esta feature añade un caso de uso nuevo — **revenue por cliente en ventana
temporal** — y lo usa como soporte para demostrar lo contrario: que el contrato
Avro es una **barrera de compilación**, y que la deriva se detecta antes de que
exista un job en ejecución.

## Alcance

Entra en alcance:

- Un job Flink que agrega revenue por cliente sobre ventanas de event-time.
- Contratos Avro versionados en el repositorio, que generan código en el build.
- Un procedimiento de canary reproducible que introduce deriva y demuestra el
  fallo cerrado del build.

Queda fuera de alcance:

- Reescribir o modificar el caso CDC + Join de 2025.
- Persistencia en Iceberg del resultado agregado (posible feature posterior).
- Levantar servicios de infraestructura nuevos.
- Registro de compatibilidad en el Schema Registry como puerta adicional
  (el canary de esta feature actúa en el build, no en el registry).

## Requisitos funcionales

- **FR-001** — El job consume el tópico Kafka de órdenes deserializando con el
  contrato Avro publicado en el Schema Registry, sin estructuras ad-hoc paralelas
  al contrato.
- **FR-002** — El job calcula el revenue por cliente como la suma del importe de
  las órdenes dentro de una ventana temporal de event-time, con watermarks
  derivados del timestamp del evento y una política declarada de eventos tardíos.
- **FR-003** — El resultado se emite en Avro hacia un tópico Kafka propio, con un
  contrato de salida que incluye cliente, inicio y fin de ventana, revenue
  acumulado y número de órdenes.
- **FR-004** — Los contratos de entrada y salida viven versionados en el módulo
  como archivos `.avsc` y generan las clases Java durante el build, de modo que
  el código del job dependa del contrato y no de una copia manual.
- **FR-005** — Existe un procedimiento reproducible y reversible que introduce
  una deriva deliberada en el contrato y demuestra que el build falla señalando
  el campo afectado, sin que el pipeline llegue a ejecutarse.
- **FR-006** — El caso se ejecuta sobre la infraestructura existente del
  repositorio (`infra/dockercompose`) sin añadir servicios nuevos.

## Criterios de éxito

- **SC-001** — Sobre un fixture sintético determinista, el revenue por cliente y
  ventana coincide exactamente con el cálculo de referencia calculado aparte.
- **SC-002** — Con los contratos íntegros, la compilación del módulo es verde y
  genera las clases Avro esperadas.
- **SC-003** — Con la deriva introducida, la compilación **falla**, el mensaje de
  error identifica el campo derivado, y no se produce ningún artefacto ejecutable.
- **SC-004** — El canary completo (romper, compilar, observar el fallo, revertir)
  se ejecuta en 30 segundos o menos desde un único comando documentado.
- **SC-005** — El gate SDD con perfil `consumer-release` queda verde sobre esta
  feature, y vuelve a rojo si se degrada cualquiera de sus artefactos.
- **SC-006** — El job, ejecutado sobre la infraestructura real del repositorio
  (Kafka, Schema Registry y un cluster Flink), produce para el mismo fixture el
  mismo resultado que la verificación local, comparado contra la tabla de
  referencia.

## Decisiones de diseño

**Por qué ventana de event-time y no processing-time.** El caso CDC + Join de
2025 usó `TumblingProcessingTimeWindows`, suficiente para una demo de join. Aquí
el revenue por ventana es una cifra que alguien podría leer como negocio, y una
ventana de processing-time produce cifras distintas en cada ejecución. Event-time
con watermarks hace el resultado reproducible, que es condición para SC-001.

**Por qué el canary rompe el `.avsc` y no el mensaje en el tópico.** Corromper un
mensaje en Kafka demuestra un fallo de runtime, que es exactamente lo que esta
feature quiere evitar como forma de descubrir la deriva. Romper el `.avsc`
demuestra la detección temprana: `avro-maven-plugin` regenera las clases y el
compilador rechaza el código que usaba el campo anterior.

**Dos puertas distintas.** La demo no debe confundirlas: el contrato Avro protege
la coherencia entre esquema y código y actúa en el build; el gate SDD protege
que la feature tenga contrato, criterios y trazas, y actúa en la entrega. SC-003
verifica la primera; SC-005 la segunda.
