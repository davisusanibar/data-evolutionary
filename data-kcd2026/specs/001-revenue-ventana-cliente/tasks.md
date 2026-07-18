# Tasks — 001-revenue-ventana-cliente

**Input**: [plan.md](plan.md), [spec.md](spec.md), [feature.spec.yaml](feature.spec.yaml)

Formato: `- [ ] [TNNN] [FR-NNN] acción y ruta`. Orden de dependencias según la
"Secuencia de implementación" de `plan.md`.

## Fase 1 — Contratos y build (bloqueante, FR-004)

- [x] [T001] [FR-004] Declarar el contrato de entrada en `data-kcd2026/src/main/resources/model/orden.avsc` (cliente, importe, timestamp de evento).
- [x] [T002] [FR-004] Declarar el contrato de salida en `data-kcd2026/src/main/resources/model/revenue-ventana.avsc` (cliente, inicio/fin de ventana, revenue acumulado, número de órdenes).
- [x] [T003] [FR-004] Añadir a `data-kcd2026/pom.xml` `flink-streaming-java`, `flink-connector-kafka`, `flink-avro`, `flink-avro-confluent-registry`, `org.apache.avro:avro` y el `avro-maven-plugin`, con las versiones fijadas en `plan.md` (Flink 1.20.2, Avro 1.11.3).
- [x] [T004] [FR-004] Confirmar que `avro-maven-plugin` genera las clases en `target/generated-sources/avro` a partir de los `.avsc` de T001/T002.

## Fase 2 — Job de agregación

- [x] [T005] [FR-001] Implementar la fuente Kafka con deserialización Avro contra el Schema Registry en `data-kcd2026/src/main/java/com/topaya/kcd2026/revenue/RevenueVentanaClienteJob.java`.
- [x] [T006] [FR-002] Implementar `WatermarkStrategy.forMonotonousTimestamps()` y `keyBy` por cliente sobre `TumblingEventTimeWindows.of(Time.minutes(1))` con `.allowedLateness(Time.seconds(5))` en el mismo job (decisión final: la tolerancia vive en `allowedLateness`, no en el watermark — ver `plan.md`).
- [x] [T007] [FR-002] Implementar el `AggregateFunction` que suma importe y cuenta órdenes por ventana, sin materializar la ventana completa en estado.
- [x] [T008] [FR-003] Implementar el sink Kafka que serializa el resultado contra el contrato de salida de T002.

## Fase 3 — Verificación

- [x] [T009] [FR-002] Crear el fixture sintético determinista en `data-kcd2026/src/test/java/com/topaya/kcd2026/revenue/RevenueVentanaClienteJobTest.java`, incluyendo un evento tardío dentro de la tolerancia de 5s y uno fuera de ella.
- [x] [T010] [FR-001] [FR-003] Comparar el resultado del job contra el cálculo de referencia hecho aparte (SC-001), sobre el fixture de T009. Evidencia: `evidence/mvn-test.log`.
- [x] [T011] [FR-004] Confirmar `mvn -pl data-kcd2026 -am compile` verde con los contratos de T001/T002 íntegros (SC-002). Evidencia: `evidence/mvn-compile.log`.

## Fase 4 — Cierre gobernado

- [x] [T012] [GATES] Ejecutar `python3 tools/validation/spec_kit_gate.py . --profile consumer-release` y confirmar verde sobre esta feature (SC-003). Evidencia: `evidence/spec-kit-gate.log`.
- [x] [T013] [LEDGER] Confirmar que `value_ledger` sigue siendo `N/A` explícito (sin ledger de valor instalado en este consumer) o vincularlo si se instala antes del cierre.
- [x] [T014] [INGEST] Dejar constancia en `verification.md` de que no hay adapter de ingesta instalado en este consumer; N/A explícito, no artefacto inventado.
- [x] [T015] [SEAL] Preparar el cambio para revisión humana. El owner (`romaria85`) firmó el paso a `implemented`/`Cerrado` (Artículo VI) el 2026-07-18, en el chat de esta sesión. Ningún commit o push es automático.

## Dependencias

- Fase 1 bloquea Fase 2 (el job depende de las clases generadas de los contratos).
- Fase 2 bloquea Fase 3 (no hay nada que verificar sin el job).
- Fase 3 bloquea Fase 4 (el cierre exige SC-001/SC-002 en verde antes de correr el gate).
- T001 y T002 son paralelizables entre sí (archivos distintos, sin dependencia mutua).
