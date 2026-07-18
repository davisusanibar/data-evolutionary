# Tareas — 001-revenue-ventana-cliente

Implementación ejecutada tras la aprobación del owner en el checkpoint
`approve-implementation`. Queda abierta la única tarea que no me corresponde: la
firma de cierre.

## Contratos y build

- [x] Declarar el contrato Avro de entrada de órdenes en `src/main/resources/model/`, con identificador de orden, cliente, importe y fecha — FR-004
- [x] Declarar el contrato Avro de salida con cliente, inicio y fin de ventana, revenue y conteo — FR-003, FR-004
- [x] Añadir al `pom.xml` del módulo las dependencias Avro y de registro Confluent alineadas con las versiones de `data-cdc-kafka-flink-iceberg` — FR-004
- [x] Enganchar `avro-maven-plugin` en `generate-sources` apuntando al directorio de contratos — FR-004
- [x] Verificar que `mvn -pl data-kcd2026 clean compile` genera las clases y queda verde — FR-004

## Job de agregación

- [x] Implementar la fuente Kafka con deserialización Avro contra el Schema Registry, sin estructuras paralelas al contrato — FR-001
- [x] Definir la estrategia de watermarks con desorden acotado sobre la fecha de la orden y declarar la política de eventos tardíos — FR-002
- [x] Implementar la agregación por cliente sobre ventana de event-time con `AggregateFunction` de importe y conteo — FR-002
- [x] Implementar el sink Kafka con serialización Avro del contrato de salida — FR-003
- [x] Documentar en el README del módulo cómo lanzar el job sobre la infraestructura existente, sin servicios nuevos — FR-006

## Datos y verificación

- [x] Construir el fixture sintético determinista de órdenes, sin PII ni datos productivos — FR-001
- [x] Calcular la tabla de revenue de referencia por cliente y ventana, independiente del job — FR-002
- [x] Ejecutar el job contra el fixture y comparar el resultado con la referencia — FR-002

## Canary de deriva

- [x] Escribir el procedimiento de canary como un comando único, reversible, que rompa el campo de importe del contrato de entrada — FR-005
- [x] Capturar la salida del build fallido mostrando el símbolo desaparecido y confirmar que no se produce artefacto ejecutable — FR-005
- [x] Cronometrar el canary de extremo a extremo y confirmar que cabe en el umbral declarado — FR-005
- [x] Documentar la variante por cambio de tipo como alternativa secundaria de la demostración — FR-005
- [x] Verificar que el job usa los accesores generados y no acceso dinámico por nombre, condición sin la cual el canary no demuestra nada — FR-001, FR-005

## Cierre gobernado

- [x] [GATES] Ejecutar el gate SDD con perfil `consumer-release` sobre la feature y adjuntar la salida como evidencia — FR-006
- [x] [LEDGER] Registrar el resultado de valor de la demo; el consumer no tiene ledger instalado, así que se documenta el vínculo declarado en la spec — FR-006
- [x] [INGEST] Ingerir los artefactos producidos como borradores bajo el attachment, sin promoverlos — FR-006
- [ ] [SEAL] Solicitar la firma del owner y sellar el cierre; sin firma la feature no pasa a implementado — FR-006

## Notas de ejecución

**Alcance real de lo verificado.** La aritmética del caso (SC-001) se verificó en
local sobre el fixture, ejercitando el mismo `RevenueWindowPipeline` que usa el
job. El transporte Kafka y Schema Registry **no** se ejecutó: la infraestructura de
`infra/dockercompose` no estaba levantada durante la implementación. El job
compila y está completo, pero su ejecución extremo a extremo contra Kafka queda
sin evidencia y no se reclama.

**Desviación respecto al plan.** El plan preveía `flink-connector-kafka` como
única dependencia de conector. Al compilar apareció que `DeliveryGuarantee` vive
en `flink-connector-base`, que el conector de Kafka no arrastra; se añadió
explícitamente.
