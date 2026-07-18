# Tareas — 001-revenue-ventana-cliente

Fase `tasked`: las tareas están declaradas y trazadas a requisitos, pendientes de
ejecución. El paso a `implemented` exige que todas queden cerradas y que el owner
firme.

## Contratos y build

- [ ] Declarar el contrato Avro de entrada de órdenes en `src/main/resources/model/`, con identificador de orden, cliente, importe y fecha — FR-004
- [ ] Declarar el contrato Avro de salida con cliente, inicio y fin de ventana, revenue y conteo — FR-003, FR-004
- [ ] Añadir al `pom.xml` del módulo las dependencias Avro y de registro Confluent alineadas con las versiones de `data-cdc-kafka-flink-iceberg` — FR-004
- [ ] Enganchar `avro-maven-plugin` en `generate-sources` apuntando al directorio de contratos — FR-004
- [ ] Verificar que `mvn -pl data-kcd2026 clean compile` genera las clases y queda verde — FR-004

## Job de agregación

- [ ] Implementar la fuente Kafka con deserialización Avro contra el Schema Registry, sin estructuras paralelas al contrato — FR-001
- [ ] Definir la estrategia de watermarks con desorden acotado sobre la fecha de la orden y declarar la política de eventos tardíos — FR-002
- [ ] Implementar la agregación por cliente sobre ventana de event-time con `AggregateFunction` de importe y conteo — FR-002
- [ ] Implementar el sink Kafka con serialización Avro del contrato de salida — FR-003
- [ ] Documentar en el README del módulo cómo lanzar el job sobre la infraestructura existente, sin servicios nuevos — FR-006

## Datos y verificación

- [ ] Construir el fixture sintético determinista de órdenes, sin PII ni datos productivos — FR-001
- [ ] Calcular la tabla de revenue de referencia por cliente y ventana, independiente del job — FR-002
- [ ] Ejecutar el job contra el fixture y comparar el resultado con la referencia — FR-002

## Canary de deriva

- [ ] Escribir el procedimiento de canary como un comando único, reversible, que rompa el campo de importe del contrato de entrada — FR-005
- [ ] Capturar la salida del build fallido mostrando el símbolo desaparecido y confirmar que no se produce artefacto ejecutable — FR-005
- [ ] Cronometrar el canary de extremo a extremo y confirmar que cabe en el umbral declarado — FR-005
- [ ] Documentar la variante por cambio de tipo como alternativa secundaria de la demostración — FR-005
- [ ] Verificar que el job usa los accesores generados y no acceso dinámico por nombre, condición sin la cual el canary no demuestra nada — FR-001, FR-005

## Cierre gobernado

- [ ] [GATES] Ejecutar el gate SDD con perfil `consumer-release` sobre la feature y adjuntar la salida como evidencia — FR-006
- [ ] [LEDGER] Registrar el resultado de valor de la demo; el consumer no tiene ledger instalado, así que se documenta el vínculo declarado en la spec — FR-006
- [ ] [INGEST] Ingerir los artefactos producidos como borradores bajo el attachment, sin promoverlos — FR-006
- [ ] [SEAL] Solicitar la firma del owner y sellar el cierre; sin firma la feature no pasa a implementado — FR-006
