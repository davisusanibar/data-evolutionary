---
id: KCD2026-ORDERS-REVENUE-WINDOW
estado: Propuesto
fase: implemented
dominio: data-pipeline
tramo_sensibilidad: T0
owner: Data & AI Lead
tipo_cambio: feature
trazas:
  - ADR-0001
  - ADR-0002
spec_tipada: data-kcd2026/specs/001-orders-revenue-window/feature.spec.yaml
fuentes:
  - data-kcd2026/src/main/resources/model/orders_revenue_window.avsc
  - data-cdc-kafka-flink-iceberg/src/main/resources/model/orders.avsc
value_ledger: "N/A: demo de conferencia; no hay iniciativa, owner de negocio ni baseline de outcome"
hipotesis_valor: Un pipeline cuyo contrato de datos es verificable por un gate no puede derivar en silencio entre el esquema y lo que la especificación declara
---

# Revenue por cliente en ventana temporal

## Intención y alcance

Agregar el importe de las órdenes por cliente en ventanas temporales fijas a
partir del tópico Kafka `orders`, y publicar el resultado en un tópico propio
con un contrato Avro explícito.

El alcance es un job Flink de streaming y su contrato de datos. Quedan fuera:
persistencia en Iceberg, CDC, exactitud fiscal del importe, reprocesamiento
histórico y cualquier decisión de negocio sobre qué es "revenue".

## Requisitos funcionales

- **FR-001**: el job debe leer el tópico `orders` con el esquema Avro vigente
  `orders.avsc` mediante Schema Registry, sin declarar el esquema en el código.
- **FR-002**: la salida debe ajustarse al contrato `orders_revenue_window.avsc`
  con exactamente los campos `o_custkey` (long), `window_start` (long,
  timestamp-millis), `window_end` (long, timestamp-millis),
  `sum_o_totalprice` (double) y `order_count` (int).
- **FR-003**: la agregación debe usar ventanas fijas no solapadas de tamaño
  parametrizable, con un valor por defecto declarado y observable en el log.
- **FR-004**: una orden sin `o_custkey` o sin `o_totalprice` no debe abortar el
  job ni contaminar una ventana; debe descartarse y contarse como descartada.
- **FR-005**: el job no debe escribir en `orders` ni en ningún tópico de entrada;
  su única escritura es el tópico de salida declarado.
- **FR-006**: un gate debe verificar que los campos declarados en FR-002
  coinciden con el `.avsc` publicado; una divergencia debe fallar cerrado antes
  de compilar.
- **FR-007**: el job debe declarar su contrato de origen y destino en el log de
  arranque, incluyendo el nombre del tópico y la ruta del `.avsc`.

## Criterios de éxito

- **SC-001**: con `orders.avsc` disponible en el registry, el job arranca y
  consume sin esquema embebido en el código; un grep del fuente no encuentra
  literales de esquema Avro.
- **SC-002**: dado un lote con dos órdenes del mismo cliente en la misma
  ventana, la salida contiene exactamente un registro con `sum_o_totalprice`
  igual a la suma y `order_count` igual a 2.
- **SC-003**: dos órdenes del mismo cliente en ventanas distintas producen dos
  registros con `window_start` distintos y ningún acumulado entre ventanas.
- **SC-004**: una orden con `o_custkey` nulo no aparece en la salida, el job
  sigue vivo y el contador de descartes se incrementa en uno.
- **SC-005**: si se altera un campo de `orders_revenue_window.avsc` respecto de
  FR-002, el gate de contrato falla con exit distinto de cero y nombra el campo
  divergente.
- **SC-006**: el log de arranque contiene el tópico de origen, el tópico de
  destino, la ruta de ambos `.avsc` y el tamaño de ventana efectivo.

## Clarifications

1. La ventana es de *processing time*, no de *event time*. `o_orderdate` es una
   fecha sin hora y no sirve como marca temporal del evento; usar event time
   exigiría una decisión sobre watermarks y datos tardíos que esta demo no toma.
2. `sum_o_totalprice` es `double` y no `decimal`. El origen declara
   `o_totalprice` como decimal fixed(7) precision 15 scale 2; la conversión a
   double pierde exactitud y por eso esta feature **no** reclama exactitud
   fiscal. Un pipeline de facturación exigiría conservar el decimal.
3. "Revenue" aquí es la suma aritmética de `o_totalprice` de las órdenes
   observadas en la ventana. No descuenta cancelaciones, devoluciones ni
   considera `o_orderstatus`.
4. Una orden descartada por FR-004 se cuenta pero no se persiste; esta feature
   no instala dead-letter queue.

## Frontera de claims

T0 local sobre datos sintéticos. La feature puede demostrar, en el compose
local: lectura Avro por registry, agregación por ventana, descarte de registros
incompletos y fallo cerrado del gate de contrato ante deriva del esquema.

**No demuestra** exactitud fiscal del importe, corrección ante datos tardíos o
desordenados, tolerancia a fallos, exactly-once, rendimiento, escalabilidad,
operación en producción, calidad del dato de origen, adopción ni outcome de
negocio. El importe agregado no es una cifra de negocio: es una suma sobre
fixtures.
