# Verificación — 001-revenue-ventana-cliente

Matriz criterio de éxito → evidencia.

| Criterio | Cómo se verifica | Evidencia (`evidence/`) | Estado |
|---|---|---|---|
| SC-001 | `RevenueVentanaClienteJobTest#agregaRevenuePorClienteYDescartaElEventoFueraDeTolerancia`: fixture determinista con evento tardío dentro (aceptado) y fuera (descartado) de la tolerancia de 5s, comparado contra un cálculo de referencia hecho aparte en el propio test. | `evidence/mvn-test.log` (Tests run: 1, Failures: 0) | Verificado |
| SC-002 | `mvn -pl data-kcd2026 -am clean compile`, contratos `.avsc` íntegros. | `evidence/mvn-compile.log` (BUILD SUCCESS) | Verificado |
| SC-003 | `python3 tools/validation/spec_kit_gate.py . --profile consumer-release` sobre esta feature. | `evidence/spec-kit-gate.log` (43/43 checks OK) | Verificado |

## Validación end-to-end adicional (post-cierre, 2026-07-18)

No exigida por ningún SC de `spec.md` (que solo pedía compile + gate + test
con MiniCluster embebido), pero corrida a pedido explícito para confirmar que
el jar plano realmente corre en un cluster Flink real, no solo en el test:

1. `mvn -pl data-kcd2026 -am package` con `maven-shade-plugin` (agregado a
   `pom.xml`) generó un jar plano de 23M con
   `Main-Class: com.topaya.kcd2026.revenue.RevenueVentanaClienteJob`.
2. Se levantó un cluster aislado (Kafka + Schema Registry + Flink
   jobmanager/taskmanager 1.20.2, mismas imágenes que `infra/dockercompose`)
   vía un compose separado en el scratchpad de la sesión — **no** vía
   `infra/dockercompose/docker-compose.yml` directamente, porque ese archivo
   falla al parsear con Docker Compose v2 (`env_file` de `namenode`/`datanode`
   usa claves con guiones, incompatible con el parser v2; bug preexistente,
   no relacionado con esta feature, no corregido aquí).
3. El jar se sometió al `flink-jobmanager` real vía `flink run -d`. Evidencia:
   `evidence/flink-jobs-overview.json`, `evidence/flink-job-detail.json` — job
   `revenue-por-cliente-en-ventana` en estado `RUNNING`.
4. Se produjeron órdenes Avro reales (`kafka-avro-console-producer` contra el
   Schema Registry real) al tópico `kcd2026.ordenes`, y se consumió el
   resultado real del tópico `kcd2026.revenue-ventana`. Evidencia:
   `evidence/e2e-kafka-output.txt` — `120.50 + 79.75 = 200.25` para
   `cliente-demo` en la ventana `[1784388000000, 1784388060000)`, coincide
   exactamente con la suma esperada.
5. `evidence/flink-taskmanager.log` y `evidence/docker-ps.txt`: sin errores,
   los 4 contenedores arriba.

No se probó aquí el caso de evento tardío contra infraestructura real (ya
cubierto por SC-001 en el test); esta corrida valida específicamente que el
jar plano funciona fuera del MiniCluster embebido del test.

## Nota sobre ingesta (T014)

No hay adapter de ingesta instalado en este consumer: los artefactos de
evidencia se agregan directamente a `evidence/` dentro de esta carpeta de
feature, sin un paso de ingesta automatizado. N/A explícito, no un artefacto
inventado.

## Cierre

Los tres criterios están verificados con evidencia real. El owner (`romaria85`)
firmó el paso a `fase: implemented` / `estado: Cerrado` el 2026-07-18 en el
chat de esta sesión (Artículo VI). Ningún commit o push es automático.
