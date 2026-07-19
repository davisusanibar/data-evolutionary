# Matriz de verificación · Feature 001 `data-kcd2026`

Estado: `tasked`. La evidencia es una observación local de este workspace
(Apache Maven 3.9.8, 2026-07-18) y no constituye una validación en un cluster
Flink o Kafka real.

| SC | FR | Tarea | Comando de verificación | Evidencia |
|---|---|---|---|---|
| SC-001 | FR-001 | T003 | `mvn -q -pl data-kcd2026 -am validate` | `evidence/sc-001-module-registration.json` |
| SC-002 | FR-003 | T005, T007 | `mvn -pl data-kcd2026 dependency:list` | `evidence/sc-002-flink-dependencies.json` |
| SC-003 | FR-004 | T006 | `unzip -l` sobre el jar + `javap -p com/topaya/kcd2026/KafkaHello.class` | `evidence/sc-003-entrypoint.json` |
| SC-004 | FR-002 | T004, T007 | `mvn -pl data-kcd2026 clean package` | `evidence/sc-004-build.json` |

Las fuentes leídas para construir esta matriz están registradas en
`evidence/sources.md`.

## Límites

Ninguna fila afirma comportamiento en producción. SC-002 observa la lista de
dependencias resuelta contra el repositorio local `~/.m2` de esta máquina; otro
repositorio puede mediar versiones transitivas distintas. SC-003 acredita la
presencia y el paquete de la clase en el artefacto, no la ejecución de un job ni
la conexión a un broker. Ningún SC mide rendimiento, latencia ni throughput.
