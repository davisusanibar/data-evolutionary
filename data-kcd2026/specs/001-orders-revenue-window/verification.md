# Verificación — Revenue por cliente en ventana temporal

La matriz cierra el círculo: cada criterio de éxito resuelve a una tarea, un
test y un gate. Las rutas de evidencia se llenan al ejecutar; **una fila con
evidencia vacía no es un resultado anticipado, es trabajo pendiente**.

| SC | FR | Tarea | Test/marker | Gate | Evidencia de cierre |
|---|---|---|---|---|---|
| SC-001 | FR-001 | T004 | `contract_check.check_no_embedded_schema` | contract | `evidence/sc-005-contract-gate.json` |
| SC-002 | FR-002 | T007, T010, T013 | `RevenueVentanaTest::sc002_sumaYConteoEnLaMismaVentana` | test, e2e | `evidence/sc-002-004-006-tests.json`, `evidence/sc-002-004-e2e-compose.json` |
| SC-003 | FR-003 | T005, T010 | `RevenueVentanaTest::sc003_ventanasNoAcumulanEntreSi` | test | `evidence/sc-002-004-006-tests.json` |
| SC-004 | FR-004 | T006, T010, T013 | `RevenueVentanaTest::sc004_ordenIncompletaSeDescarta` | test, e2e | `evidence/sc-002-004-006-tests.json`, `evidence/sc-002-004-e2e-compose.json` |
| SC-005 | FR-006 | T003, T012 | `contract_check.check_schema_matches_contract` | contract | `evidence/sc-005-contract-gate.json` |
| SC-006 | FR-003, FR-007 | T009, T010, T013 | `RevenueVentanaTest::sc006_ventanaParametrizable` | test, e2e | `evidence/sc-002-004-006-tests.json`, `evidence/sc-002-004-e2e-compose.json` |

## Estado verificado

**Verificado sin cluster:**

- SC-002, SC-003, SC-004, SC-006 — `./mvnw -pl data-kcd2026 clean test` compila
  desde cero y ejecuta 5 tests sin fallos sobre Temurin 11.0.31.
- SC-001 y SC-005 — el gate de contrato en verde (exit 0) y en rojo (exit 1)
  induciendo deriva: renombrar `sum_o_totalprice` a `total_revenue` y cambiar su
  tipo a `string`. Nombró el campo divergente en ambos casos.

**Verificado end-to-end en el compose (T013):**

Cuatro fixtures a `orders`; salida observada en `orders_revenue_window`:

```json
{"o_custkey":7,"window_start":1784283820000,"window_end":1784283840000,"sum_o_totalprice":150.0,"order_count":2}
{"o_custkey":9,"window_start":1784283820000,"window_end":1784283840000,"sum_o_totalprice":200.0,"order_count":1}
```

- SC-002 — cliente 7: `100.50 + 49.50 = 150.0` con `order_count: 2`.
- SC-004 — la orden sin `custkey` (999.99) **no aparece**. De haber pasado, el
  total sería 1149.99.
- SC-006 — el log de arranque declaró tópicos, rutas `.avsc` y ventana efectiva.

## Lo que los tests no atraparon

T013 no fue una formalidad. Con los 5 tests en verde y el módulo compilando,
tres defectos reales seguían vivos:

1. **Thin jar de 14 KB** — sin conector de Kafka ni formato Avro del registry.
   `ClassNotFoundException` al hacer submit.
2. **Contrato de origen ausente en el classpath** — el job falló cerrado al
   arrancar y nombró `orders.avsc`.
3. **`KryoException: UnsupportedOperationException`** — el job llegaba a
   `RUNNING` y moría al pasar el primer elemento fuera de la ventana. Flink no
   infiere el tipo de `GenericRecord` y cae a Kryo.

Ninguno era detectable por un test unitario ni por un gate de contrato. **Un test
verde prueba la lógica, no el despliegue.**

## Frontera del claim de esta matriz

La matriz demuestra la cadena SC → FR → tarea → test → gate y que el pipeline
produce la salida esperada sobre 4 fixtures sintéticos, en un compose local de un
broker y un taskmanager.

**No demuestra** exactitud fiscal, corrección ante datos tardíos o desordenados,
tolerancia a fallos, exactly-once, rendimiento, escalabilidad, operación en
producción, calidad del dato de origen, adopción ni outcome de negocio. Un
pipeline verificado en un compose no es un pipeline en producción.
