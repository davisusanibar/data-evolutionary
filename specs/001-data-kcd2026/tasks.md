# Tareas · Feature 001 `data-kcd2026`

## Aprobación y contrato

- [x] [T001] [FR-001] [SEAL] Registrar la aceptación humana de ADR-0001, del plan y de estas tareas por David Dali Susanibar Arce antes de considerar cerrado el módulo. Aceptaciones en `.edaios/approvals/`: ADR-0001 en `APR-95854eb8b28f`, contrato SDD en `APR-e3e59a6e6474`, tareas en el receipt emitido sobre esta versión del archivo.
- [x] [T002] [FR-001] Crear la matriz `verification.md` enlazando cada SC con su FR, su tarea, su comando de verificación y su evidencia.

## Estructura del módulo

- [x] [T003] [FR-001] Declarar `data-kcd2026` en `<modules>` del `pom.xml` raíz del reactor `com.topaya:oss`.
- [x] [T004] [FR-002] Crear `data-kcd2026/pom.xml` heredando el parent `com.topaya:oss:1.0-SNAPSHOT` con packaging jar y coordenadas propias.
- [x] [T005] [FR-003] Fijar `flink-streaming-java:1.20.1` y `flink-connector-kafka:3.3.0-1.20` con versión explícita, sin rangos.
- [x] [T006] [FR-004] Implementar `com.topaya.kcd2026.KafkaHello` con un `main` que obtenga un `StreamExecutionEnvironment`.

## Verificación y cierre

- [x] [T007] [FR-003] [GATES] Ejecutar `mvn -q -pl data-kcd2026 -am validate`, `dependency:list` y `clean package`, y registrar la salida observada en `evidence/`.
- [x] [T008] [FR-004] [LEDGER] [INGEST] Registrar el límite del claim: observación local del workspace, sin despliegue, sin cluster Kafka y sin medición de rendimiento. Registrado en `evidence/sources.md` (columna Límite), `verification.md` (sección Límites), ADR-0001 (Evidencia y frontera del claim) y el campo `claim_boundary` de cada EvidenceReceipt.
