# Registro de fuentes — Revenue por cliente en ventana temporal

Cada fuente del contrato de datos declara su origen, alcance y límite (Artículo IV).

## Contrato de salida (dentro del módulo)

- `src/main/resources/model/orders_revenue_window.avsc` — esquema Avro del tópico
  de salida `orders_revenue_window`. Fuente única del contrato tipado; lo verifica
  `tools/contract_check.py` contra `specs/001-orders-revenue-window/data-contract.yaml`.

## Contrato de origen (módulo hermano)

- Tópico `orders`, esquema `orders.avsc`. Vive en el módulo hermano
  `data-cdc-kafka-flink-iceberg/src/main/resources/model/orders.avsc`, fuera de la
  raíz de este workspace. El `pom.xml` lo deriva en `generate-resources` en cada
  empaquetado en vez de duplicarlo (una fuente, muchas vistas). Por eso no se lista
  en `fuentes:` del frontmatter: el gate exige que las fuentes vivan bajo la raíz, y
  este contrato de origen es un input externo declarado en FR-001, no un artefacto
  propio de la feature.

## Alcance y límite de la única cifra

- `sum_o_totalprice` es la suma aritmética de `o_totalprice` en la ventana de N
  segundos (defecto 60). Convertida a `double`: **no** reclama exactitud fiscal
  (Clarification 2). No descuenta cancelaciones ni devoluciones (Clarification 3).
- Datos sintéticos TPC-H (T0): no hay PII. `o_custkey` es una clave sintética.
