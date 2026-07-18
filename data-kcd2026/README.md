# data-kcd2026 — Pipeline gobernado por Spec Driven Development

Demo de **KCD Lima Perú 2026**: *Construyendo Data Pipelines con Apache Flink y
Spec Driven Development*.

Este módulo agrega revenue por cliente en ventanas temporales desde Kafka. Lo
interesante no es el job: es que **el contrato de datos no puede derivar en
silencio**.

## La idea en una línea

El `.avsc` no es la verdad. Es una representación. La verdad es la
especificación, y un gate falla cerrado si dejan de coincidir.

## Estructura

```
data-kcd2026/
├── specs/
│   ├── 001-orders-revenue-window/        ← el pipeline (FR-001..007, SC-001..006)
│   │   ├── feature.spec.yaml · spec.md · plan.md · tasks.md · verification.md
│   │   └── evidence/                     ← tests, gate en rojo/verde, e2e real
│   └── 002-conference-deck-and-use-case-page/  ← el deck y la página (FR-001..008)
│       ├── feature.spec.yaml             ← pin sha256 del template, extractos de log
│       └── evidence/                     ← gate en verde, rojo inducido, OOXML
├── tools/
│   ├── contract_check.py                 ← gate 001: spec ↔ FR-002 ↔ .avsc
│   ├── deliverables_check.py             ← gate 002: deck ↔ evidencia ↔ página
│   └── generate_use_case_page.py         ← página HTML determinista (--check)
├── docs/
│   ├── edaios-operating-system-flink-use-case.config.json  ← la fuente
│   └── edaios-operating-system-flink-use-case.html         ← derivado; no editar
├── deck/                                 ← generador del deck KCD (ADR-0015: el
│   ├── build_estructura.py · llenar.py · kcd_estilo.py     render vive aquí, en
│   ├── contenido_1.py · contenido_diagramas.py · contenido_2.py   el consumer)
│   ├── template.pptx                     ← template oficial KCD, pineado por sha256
│   └── build/kcd2026.pptx                ← derivado; se regenera, no se edita
├── src/main/resources/model/orders_revenue_window.avsc
└── src/main/java/com/topaya/kcd2026/
    ├── JobOrdersRevenueWindow.java       ← wiring de Flink
    ├── RevenueVentana.java               ← lógica pura, testeable sin cluster
    └── FiltroOrdenCompleta.java          ← FR-004: descarta y cuenta
```

## El deck y la página del caso (spec 002)

Los dos materiales de la charla son proyecciones gobernadas por
`specs/002-conference-deck-and-use-case-page/`:

```bash
# regenerar el deck (39 slides sobre el template oficial KCD) y su PDF
# entregable con los 3 anexos HTML — el orden importa: build_estructura
# borra build/, los covers leen los previews de anexar.py --paginas y la
cd data-kcd2026/deck \
  && python3 build_estructura.py \
  && python3 anexar.py --paginas \
  && python3 llenar.py \
  && python3 render.py

# regenerar la página del caso
python3 data-kcd2026/tools/generate_use_case_page.py

# el gate de ambos: falla cerrado ante deriva
python3 data-kcd2026/tools/deliverables_check.py
```

Los anexos: el pptx presenta, cada `aN.pdf` documenta. Un `.pptx` no puede
contener HTML vivo, así que cada vista HTML (el OS day-zero de `edaiosv`, la
vista aplicada a Flink y la página del caso) viaja completa y paginada por
Chrome headless —texto seleccionable— como PDF independiente en
`build/anexos/aN.pdf`; el deck solo lleva un cover por anexo con la
portada real. Los PDF de anexos son render de Chrome del HTML determinista y
no reclaman byte-determinismo (metadatos de fecha): su verificación es
estructural, la misma frontera que el `.pptx` (ADR-0015, invariante 5).

El gate verifica que los extractos de log del deck coincidan con la evidencia de
la spec 001 (si el deck cuenta algo que la evidencia no registró, exit 1
nombrando el slide), que el slide «Qué NO demostramos» exista, que el template
no haya cambiado (pin sha256) y que la página sea byte-idéntica a su
recompilación. El `.pptx` no reclama determinismo byte a byte —es un ZIP con
timestamps—; su verificación es estructural, y esa frontera está declarada
(ADR-0015, invariante 5).

## El gate en 30 segundos (el momento de la demo)

```bash
# 1. Estado sano
python3 data-kcd2026/tools/contract_check.py
# [contract] OK: 5 campos coinciden entre feature.spec.yaml, FR-002 y ...

# 2. Alguien "mejora" el esquema sin tocar la especificación
sed -i '' 's/"sum_o_totalprice"/"total_revenue"/' \
  data-kcd2026/src/main/resources/model/orders_revenue_window.avsc

# 3. El gate no deja pasar
python3 data-kcd2026/tools/contract_check.py
# [contract] FAIL: el contrato derivo entre sus representaciones
#   - campo declarado en feature.spec.yaml y ausente del .avsc: 'sum_o_totalprice'
#   - campo presente en el .avsc y no declarado en feature.spec.yaml: 'total_revenue'
#
#   La especificacion manda. Corrige la fuente y regenera;
#   no ajustes el .avsc para que el gate calle.
```

El gate compara **tres** representaciones del mismo contrato: el `feature.spec.yaml`
(fuente), el texto de FR-002 (prosa) y el `.avsc` (lo que ejecuta Flink). Si
agregás un campo al esquema y al contrato tipado pero no lo explicás en el
requisito, también falla: el requisito dejó de describir lo que el pipeline hace.

Es el mismo patrón que EDAIOS Core aplica a su propia Constitución, que no
compila si Foundation dejó de decir lo que la Constitución afirma que dice.

## Ejecutar

Requiere JDK 11+ y el compose de `infra/dockercompose` arriba.

```bash
# Gate de contrato (no requiere JDK)
python3 data-kcd2026/tools/contract_check.py

# Tests: SC-002, SC-003, SC-004, SC-006 sin cluster
./mvnw -pl data-kcd2026 test

# Job contra el compose
./mvnw -pl data-kcd2026 package
# submit del jar al Flink cluster; ventana por defecto 60s
#   --ventana-segundos 30  para acortarla en vivo
```

Dependencias del gate: `pyyaml`.

## Contrato

| | |
|---|---|
| Origen | tópico `orders`, esquema `orders.avsc` vía registry |
| Destino | tópico `orders_revenue_window`, esquema `orders_revenue_window.avsc` |
| Ventana | processing time, fija, 60s por defecto, `--ventana-segundos` |
| Registry | `http://registry:8081` |
| Broker | `broker:9092` |

## Frontera de claims

T0 sobre datos sintéticos. Este módulo demuestra, en el compose local: lectura
Avro por registry, agregación por ventana, descarte de registros incompletos y
fallo cerrado del gate ante deriva del esquema.

**No demuestra** exactitud fiscal del importe (el origen es `decimal` y aquí se
degrada a `double`: ver Clarification 2), corrección ante datos tardíos o
desordenados, tolerancia a fallos, exactly-once, rendimiento, escalabilidad,
operación en producción, adopción ni outcome de negocio.

El importe agregado no es una cifra de negocio: es una suma sobre fixtures.

## Estado

**Verificado** sobre Temurin 11.0.31:

```
./mvnw -pl data-kcd2026 clean test
  Compiling 3 source files with javac [debug target 11]
  Tests run: 5, Failures: 0, Errors: 0, Skipped: 0
  BUILD SUCCESS
```

El gate de contrato se ejercitó en verde (exit 0) y en rojo (exit 1) induciendo
deriva. Evidencia con digests en `specs/001-orders-revenue-window/evidence/`.

**End-to-end en el compose** (T013), con 4 fixtures a `orders`:

```json
{"o_custkey":7,"window_start":1784283820000,"window_end":1784283840000,"sum_o_totalprice":150.0,"order_count":2}
{"o_custkey":9,"window_start":1784283820000,"window_end":1784283840000,"sum_o_totalprice":200.0,"order_count":1}
```

`100.50 + 49.50 = 150.0` en 2 órdenes (SC-002). La cuarta orden, sin `custkey` y
por 999.99, no aparece: FR-004 la descartó (SC-004).

## Lo que los tests no atraparon

T013 no fue una formalidad. Con los 5 tests verdes y el módulo compilando, tres
defectos seguían vivos y solo aparecieron con datos fluyendo por un cluster:

| Defecto | Síntoma |
|---|---|
| Thin jar de 14 KB, sin shade | `ClassNotFoundException` al hacer submit |
| `orders.avsc` ausente del classpath | el job **falló cerrado** al arrancar y nombró el contrato |
| Falta `.returns(GenericRecordAvroTypeInfo)` | `KryoException` al pasar el primer elemento fuera de la ventana |

El job llegaba a `RUNNING` con el log correcto y moría al recibir el primer dato.
**Un test verde prueba la lógica, no el despliegue.**

## Reproducir la demo

```bash
colima start --cpu 4 --memory 8
docker-compose -f infra/dockercompose/docker-compose.yml \
  up -d broker schema-registry flink-jobmanager flink-taskmanager

./mvnw -pl data-kcd2026 clean package

# submit por la REST API de Flink (localhost:18081)
curl -X POST -H "Expect:" -F "jarfile=@data-kcd2026/target/data-kcd2026-1.0-SNAPSHOT-shaded.jar" \
  http://localhost:18081/jars/upload
curl -X POST "http://localhost:18081/jars/<JAR_ID>/run" \
  -H "Content-Type: application/json" -d '{"programArgs":"--ventana-segundos 20","parallelism":1}'

# poblar: debe correr DENTRO de la red; el broker anuncia broker:9092,
# que no resuelve desde el host
docker run --rm --network topaya -v "$PWD/data-kcd2026/target:/jar" \
  flink:1.20.2-scala_2.12-java11 \
  java -cp "/jar/data-kcd2026-1.0-SNAPSHOT-shaded.jar:/opt/flink/lib/*" \
  com.topaya.kcd2026.PoblarOrdersFixture

# ver la salida
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker:9092 --topic orders_revenue_window \
  --property schema.registry.url=http://registry:8081 --from-beginning
```
