# Checklist de calidad de requisitos

Evaluación de `spec.md`, `data-contract.yaml`, la Constitución operativa proyectada
y el registro de fuentes antes de planificar.

- [x] El alcance se limita a un job Flink y su contrato de datos; excluye Iceberg, CDC, exactitud fiscal, reprocesamiento y decisiones de negocio.
- [x] `Data & AI Lead` está declarado como owner humano; la aceptación futura no se infiere.
- [x] Cada FR describe una obligación observable sin prescribir librería ni proveedor concreto más allá de Kafka/Avro/registry ya fijados por el entorno.
- [x] Cada FR posee al menos un SC medible con resultado observable (suma, conteo, descarte, exit del gate, log de arranque).
- [x] El contrato de salida declara sus cinco campos con tipo y logicalType; el gate compara spec tipada, prosa FR-002 y `.avsc`.
- [x] La orden incompleta (sin `o_custkey` o `o_totalprice`) tiene comportamiento definido: descarte contado, sin abortar el job (FR-004/SC-004).
- [x] La ventana es de processing time y su tamaño es parametrizable con defecto declarado y observable en el log (Clarification 1, FR-003).
- [x] La única cifra (`sum_o_totalprice`) declara fuente, alcance y límite en `evidence/sources.md`; no reclama exactitud fiscal.
- [x] La sensibilidad es T0 sobre datos sintéticos TPC-H; ningún campo del contrato identifica a una persona.
- [x] El fallo del gate de contrato es fail-closed (exit ≠ 0) y nombra el campo divergente (SC-005).
- [x] Las trazas ADR-0001 y ADR-0002 existen en el catálogo; la feature no introduce frontera estructural (tipo_cambio: feature).
- [x] La hipótesis de valor no sustituye un Value Ledger ni atribuye valor a una iniciativa inexistente (value_ledger N/A declarado).
- [x] La frontera de claims separa lo que el compose local demuestra de lo que no (exactitud fiscal, datos tardíos, tolerancia a fallos, producción).
- [x] No se citan benchmarks, outcomes, adopción ni cifras de producción.
