# Tareas — 001-revenue-ventana-cliente

Implementación ejecutada tras la aprobación del owner ("Yo David apruebo la
implementación", 2026-07-18). Cada tarea se marcó solo tras ejecutarse y
verificarse. El cierre de la feature (estado Cerrado) sigue pendiente de la
declaración del owner.

## Contratos y build

- [x] [T001] [FR-004] Declarar el contrato Avro de entrada de órdenes en `src/main/resources/model/`, con identificador de orden, cliente, importe y fecha
- [x] [T002] [FR-003] [FR-004] Declarar el contrato Avro de salida con cliente, inicio y fin de ventana, revenue y conteo
- [x] [T003] [FR-004] Añadir al `pom.xml` del módulo las dependencias Avro y de registro Confluent alineadas con las versiones de `data-cdc-kafka-flink-iceberg`
- [x] [T004] [FR-004] Enganchar `avro-maven-plugin` 1.11.3 en `generate-sources` apuntando al directorio de contratos
- [x] [T005] [FR-004] Verificar que `./mvnw -pl kcd-001 clean compile` genera las clases y queda verde

## Job de agregación

- [x] [T006] [FR-001] Implementar la fuente Kafka con deserialización Avro contra el Schema Registry (`http://registry:8081`), sin estructuras paralelas al contrato
- [x] [T007] [FR-002] Definir la estrategia de watermarks con desorden acotado sobre la fecha de la orden y declarar la política de eventos tardíos
- [x] [T008] [FR-002] Implementar la agregación por cliente sobre `TumblingEventTimeWindows` con `AggregateFunction` de importe y conteo
- [x] [T009] [FR-003] Implementar el sink Kafka con serialización Avro del contrato de salida
- [x] [T010] [FR-006] Documentar en el README del módulo cómo lanzar el job sobre la infraestructura existente de `infra/dockercompose`, sin servicios nuevos

## Datos y verificación

- [x] [T011] [FR-001] Construir el fixture sintético determinista de órdenes, sin PII ni datos productivos
- [x] [T012] [FR-002] Calcular la tabla de revenue de referencia por cliente y ventana, independiente del job
- [x] [T013] [FR-002] Ejecutar el job contra el fixture y comparar el resultado con la referencia

## Canary de deriva

- [x] [T014] [FR-005] Escribir el procedimiento de canary como un comando único, reversible, que rompa el campo de importe del contrato de entrada
- [x] [T015] [FR-005] Capturar la salida del build fallido mostrando el símbolo desaparecido y confirmar que no se produce artefacto ejecutable
- [x] [T016] [FR-005] Cronometrar el canary de extremo a extremo y confirmar que cabe en el umbral declarado de 30 segundos
- [x] [T017] [FR-005] Documentar la variante por cambio de tipo como alternativa secundaria de la demostración
- [x] [T018] [FR-001] [FR-005] Verificar que el job usa los accesores generados y no acceso dinámico por nombre, condición sin la cual el canary no demuestra nada

## Validación end-to-end

- [x] [T019] [FR-006] Levantar Kafka, Schema Registry y el cluster Flink desde `infra/dockercompose`
- [x] [T020] [FR-001] Producir el fixture en Avro sobre el tópico de entrada, registrando el contrato en el Schema Registry
- [x] [T021] [FR-002] [FR-003] Someter el job al cluster Flink real y comprobar que alcanza estado RUNNING
- [x] [T022] [FR-002] Consumir el tópico de salida y comparar el resultado real contra la tabla de referencia, incluyendo los eventos de avance de watermark necesarios para cerrar la última ventana

## Cierre gobernado

- [x] [T023] [GATES] [FR-006] Ejecutar el gate SDD con perfil `consumer-release` sobre la feature y adjuntar la salida como evidencia
- [x] [T024] [LEDGER] [FR-006] Registrar el resultado de valor de la demo; el consumer no tiene ledger instalado, así que se documenta el vínculo declarado en la spec
- [x] [T025] [INGEST] [FR-006] Ingerir los artefactos producidos como borradores bajo el attachment, sin promoverlos
- [x] [T026] [SEAL] [FR-006] Preparar el cambio para revisión del owner; commit y push siguen requiriendo su autorización explícita

## Notas de ejecución

**Alcance verificado.** SC-001 verificó la aritmética en local sobre el fixture, y
SC-006 la volvió a verificar atravesando Kafka, Schema Registry y un cluster Flink
1.20.2 real (JobID 2947066e4ab33b57df57b9682b88ee68, RUNNING). Ambas coinciden con
la referencia calculada a mano.

**Hallazgo de semántica streaming.** Con fuente Kafka no acotada las ventanas solo
cierran cuando el watermark supera su fin; el fixture por sí solo deja la última
ventana abierta. La entrada end-to-end incluye dos eventos de avance de watermark
(cliente 99). Detallado en `verification.md`.

**Sin verificar.** Checkpoints, recuperación ante fallo y garantías de entrega más
fuertes que el `AT_LEAST_ONCE` configurado.

**Nota de entorno.** La validación se hizo íntegramente dentro de la red de
Docker, que es más fiel a cómo corre el job en el cluster. Para lanzar el job
desde el host haría falta mapear los hostnames del compose en `/etc/hosts`,
porque Kafka anuncia `broker:9092` y no `localhost`.
