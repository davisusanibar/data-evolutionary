# Tareas — Revenue por cliente en ventana temporal

Formato: `[TNNN] [FR-NNN] acción y ruta`. Cada FR tiene al menos una tarea; no
hay tarea sin requisito que la justifique.

## Contrato

- [x] [T001] [FR-002] declarar los cinco campos en `data-kcd2026/specs/001-orders-revenue-window/feature.spec.yaml`
- [x] [T002] [FR-002] materializar el contrato en `data-kcd2026/src/main/resources/model/orders_revenue_window.avsc`
- [x] [T003] [FR-006] implementar el gate en `data-kcd2026/tools/contract_check.py`

## Job

- [x] [T004] [FR-001] leer `orders` por registry sin esquema embebido en `JobOrdersRevenueWindow.java`
- [x] [T005] [FR-003] ventana fija parametrizable con defecto declarado en `JobOrdersRevenueWindow.leerVentanaSegundos`
- [x] [T006] [FR-004] descartar y contar órdenes incompletas en `FiltroOrdenCompleta.java`
- [x] [T007] [FR-002] construir el registro de salida en `RevenueVentana.construir`
- [x] [T008] [FR-005] escribir solo al tópico declarado en `JobOrdersRevenueWindow`
- [x] [T009] [FR-007] declarar el contrato en el log de arranque

## Verificación

- [x] [T010] [FR-002] tests SC-002/SC-003/SC-004/SC-006 en `RevenueVentanaTest.java`
- [x] [T011] [FR-001] **compilar y ejecutar los tests**: `./mvnw -pl data-kcd2026 clean test` → 5/5, exit 0
- [x] [T012] [FR-006] evidencia del gate en verde y en rojo → `evidence/sc-005-contract-gate.json`
- [x] [T013] [FR-003] ejecutar contra el compose y registrar la salida → `evidence/sc-002-004-e2e-compose.json`

## Correcciones que solo el compose reveló

Tareas no previstas en la planificación. Ningún test ni gate las detectó: el
módulo compilaba, los 5 tests estaban verdes y el job llegaba a `RUNNING`.

- [x] [T014] [FR-005] `maven-shade-plugin`: el thin jar de 14 KB no llevaba el
  conector de Kafka ni el formato Avro del registry. Habría fallado con
  `ClassNotFoundException` al hacer submit.
- [x] [T015] [FR-001] `maven-resources-plugin`: derivar `orders.avsc` desde la
  ruta que declara `contract.source_schema` en vez de duplicarlo en este módulo.
  El job falló cerrado al arrancar y nombró el contrato ausente.
- [x] [T016] [FR-002] `.returns(new GenericRecordAvroTypeInfo(...))`: sin esa
  declaración Flink no infiere el tipo de `GenericRecord`, cae a Kryo y revienta
  con `UnsupportedOperationException` al reenviar el primer elemento fuera de la
  ventana. El job llegaba a `RUNNING` y moría al recibir datos.

## Cierre

- [x] [GATES] `contract_check.py` y `mvnw -pl data-kcd2026 clean test` en verde
- [x] [LEDGER] value ledger: `N/A` — demo de conferencia sin iniciativa ni outcome
- [ ] [SEAL] preparar el cambio para revisión; commit y push exigen autorización del owner

## Estado

T001–T016 aplicadas y verificadas. El módulo compila desde cero sobre Temurin
11.0.31, sus 5 tests pasan, el gate de contrato falla cerrado ante deriva
inducida y el pipeline corre end-to-end en el compose produciendo la salida
esperada.

La fase pasa a `implemented`. **T014–T016 son la razón por la que T013 existía
como tarea separada**: tres defectos reales que ningún test ni gate atrapó, y que
solo aparecieron con datos fluyendo por un cluster.

`SEAL` sigue abierta: commit y push a `data-evolutionary` exigen autorización del
owner.
