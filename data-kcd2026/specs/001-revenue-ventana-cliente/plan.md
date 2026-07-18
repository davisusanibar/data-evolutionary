# Plan — 001-revenue-ventana-cliente

## Constitution Check

Verificación de los siete principios de la constitución operativa proyectada
antes de autorizar la implementación.

| Principio | Veredicto | Evidencia |
|---|---|---|
| I. El conocimiento manda | PASS | Los contratos `.avsc` y esta spec preceden al job; el código Java se deriva del contrato por generación, no al revés. |
| II. Spec antes que artefacto | PASS | La spec declara FR-001..FR-006, SC-001..SC-005 y owner antes de que exista una sola clase del job. |
| III. El canon crece por decisión | N/A | No hay cambio estructural del canon: es una feature de dominio bajo el perfil consumer-release ya aceptado en ADR-0016. |
| IV. Cero cifras sin fuente | PASS | La única cifra de negocio (revenue por ventana) se compara en SC-001 contra un cálculo de referencia sobre un fixture determinista versionado. |
| V. Una fuente, muchas vistas | PASS | El `.avsc` es la fuente única; las clases Java y el esquema registrado se regeneran desde él y no se editan a mano. |
| VI. La IA consume; el humano firma | PASS | La feature queda en fase tasked y estado Propuesto; el paso a implementado exige la firma del owner declarado. |
| VII. Privacidad por diseño | PASS | Tramo T0 declarado y coherente con la política del attachment: solo fixtures sintéticos, sin PII ni datos de producción. |

Constitución verificada: sha256:c05dfd28b564bcfa8fbda15ae8db0a12b6f169f00f9057fbea5e4a0fca9892a3

## Arquitectura de la solución

El job vive en el módulo `data-kcd2026` y reutiliza la infraestructura de
`infra/dockercompose` sin añadir servicios.

Flujo de datos:

1. **Fuente** — tópico Kafka de órdenes, deserializado con
   `ConfluentRegistryAvroDeserializationSchema` contra el Schema Registry en
   `http://registry:8081`, siguiendo el patrón ya probado en
   `e_cdckafkaflink.JobStreamingCDCKafkaFlink`.
2. **Watermarks** — estrategia de marca de agua con desorden acotado, derivada
   del campo de fecha de la orden. La política de eventos tardíos se declara de
   forma explícita en el código y se documenta junto al job.
3. **Agregación** — `keyBy` por identificador de cliente sobre
   `TumblingEventTimeWindows`, agregando importe total y conteo de órdenes en un
   único `AggregateFunction`, evitando materializar la ventana completa en estado.
4. **Sink** — tópico Kafka de resultado, serializado con
   `ConfluentRegistryAvroSerializationSchema` usando el contrato de salida.

Contratos, en `src/main/resources/model/`:

- Contrato de entrada: identificador de orden, identificador de cliente, importe
  y fecha de la orden.
- Contrato de salida: identificador de cliente, inicio y fin de ventana, revenue
  acumulado y número de órdenes.

El `avro-maven-plugin` se engancha en `generate-sources` y escribe en
`target/generated-sources/avro`, replicando la configuración ya validada en
`data-cdc-kafka-flink-iceberg`.

## El canary de deriva

Es el núcleo demostrativo de la feature y por eso se diseña, no se improvisa.

El procedimiento introduce una deriva de contrato de un solo tipo por ejecución,
sobre una copia de trabajo del `.avsc`, y siempre revierte:

1. Estado verde: compilar y mostrar que las clases se generan.
2. Introducir la deriva: renombrar el campo de importe en el contrato de entrada.
3. Compilar: `avro-maven-plugin` regenera la clase sin el campo anterior y
   `javac` rechaza el job, que sigue invocando el accesor viejo.
4. Mostrar el mensaje de error señalando el símbolo desaparecido.
5. Revertir el contrato y volver a verde.

La deriva por cambio de tipo (importe numérico a cadena) queda documentada como
variante secundaria: falla igual, con un error de tipos en vez de un símbolo
ausente, y sirve si se dispone de más tiempo en la presentación.

Riesgo identificado y su mitigación: si el job accediera a los campos por nombre
dinámico en lugar de por los accesores generados, la deriva **no** rompería la
compilación y el canary perdería su sentido. Por eso FR-001 prohíbe estructuras
ad-hoc paralelas al contrato, y la revisión de implementación debe verificarlo.

## Gate Impact

Impacto sobre las puertas existentes, declarado antes de implementar:

- **Puerta de compilación (contrato Avro)** — pasa a ser significativa en este
  módulo, que hasta ahora no declaraba Avro. Añadir `avro-maven-plugin` y las
  dependencias Avro convierte cualquier deriva de contrato en un fallo de build.
  Es el efecto buscado por FR-004 y verificado por SC-002 y SC-003.
- **Gate SDD `consumer-release`** — esta feature es la primera bajo `specs/` del
  consumer. A partir de aquí, el gate deja de reportar "sin features versionadas"
  y pasa a validar contrato, cobertura y cierre. Verificado por SC-005.
- **Puerta de obligatoriedad** — sin cambio en esta feature, y conviene decirlo:
  hoy no hay hook `pre-push` ni CI instalados en el repositorio, de modo que
  ambas puertas se ejecutan por invocación explícita. Cablearlas es trabajo
  separado y no se reclama aquí.
- **Sin impacto** sobre el caso CDC + Join de 2025: no se modifica su módulo, su
  contrato ni su configuración de build.

## Secuencia de implementación

1. Contratos y build primero (FR-004), porque el canary depende de que la
   generación de código exista antes que el job.
2. Job de agregación (FR-001, FR-002, FR-003) contra fixture sintético.
3. Verificación numérica (SC-001) antes de conectar la demo.
4. Canary y su cronometraje (FR-005, SC-003, SC-004).
5. Cierre gobernado: gates, ledger, ingesta y sello.
