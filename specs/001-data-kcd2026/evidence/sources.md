# Registro de fuentes · Feature 001 `data-kcd2026`

Observación local de este workspace el 2026-07-18, previa a la aceptación
humana. Las filas describen lo leído y ejecutado en el repositorio; no son un
assessment de producción.

| Rótulo | Fuente | Alcance observado | Límite |
|---|---|---|---|
| SRC-001 | `pom.xml` | El reactor `com.topaya:oss:1.0-SNAPSHOT` declara `data-kcd2026` entre sus módulos | No prueba que el módulo compile en otro entorno |
| SRC-002 | `data-kcd2026/pom.xml` | Flink 1.20.1 y conector Kafka 3.3.0-1.20 declarados con versión explícita | La declaración no es la mediación efectiva; ver SRC-005 |
| SRC-003 | `data-kcd2026/src/main/java/com/topaya/kcd2026/KafkaHello.java` | La clase declara el paquete `com.topaya.kcd2026` y su `main` obtiene un `StreamExecutionEnvironment` | No ejecuta un job ni contacta un broker Kafka |
| SRC-004 | `governance/ADR-0001-data-evolutionary-kcd2026-module.md` | Decisión de estructura del módulo, en estado Propuesto | No está firmada; no acredita aceptación |
| SRC-005 | Salida de `mvn -pl data-kcd2026 dependency:list` (Maven 3.9.8) | `flink-streaming-java:jar:1.20.1:compile` y `flink-connector-kafka:jar:3.3.0-1.20:compile` | Resolución contra el `~/.m2` de esta máquina |
| SRC-006 | Salida de `mvn -pl data-kcd2026 clean package` | Exit 0; jar `data-kcd2026-1.0-SNAPSHOT.jar` con `com/topaya/kcd2026/KafkaHello.class` | Build local, sin despliegue |
| SRC-007 | Instrucción humana de esta tarea | David Dali Susanibar Arce confirmado como owner; KCD Peru Program como value owner | No acepta por anticipado un ADR ni un plan aún no firmados |
