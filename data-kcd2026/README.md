# data-kcd2026 — Pipelines de datos gobernados por SDD

Módulo de demostración para KCD 2026. A diferencia del resto del repositorio,
**este módulo se construye bajo EDAIOS SDD**: ninguna clase se escribe antes de
que exista una spec con contrato, criterios de éxito y owner declarado.

## Qué demuestra

En KCD 2025 este repositorio mostró un pipeline **CDC + Join** funcionando
(`data-cdc-kafka-flink-iceberg`, clase
`e_cdckafkaflink.JobStreamingCDCKafkaFlink`). Funcionaba, pero nada impedía que
el contrato de datos derivara en silencio.

KCD 2026 añade el caso **revenue por cliente en ventana temporal** y, sobre él,
el punto que interesa: **el contrato Avro es una barrera de compilación**. Si el
esquema deriva, el pipeline no compila. La deriva se detecta en el build, no en
producción a las 3 de la madrugada.

## El canary de deriva

El momento central de la demo, reproducible en menos de 30 segundos:

1. El build está verde con el contrato íntegro.
2. Se rompe un campo del `.avsc` a propósito (renombrado o cambio de tipo).
3. `avro-maven-plugin` regenera las clases y **`mvn compile` falla**, señalando
   el campo derivado.
4. El pipeline nunca llega a ejecutarse con un contrato roto.

Son dos puertas distintas y conviene no confundirlas:

| Puerta | Qué protege | Cuándo actúa |
|---|---|---|
| Contrato Avro → compilación | que el código y el esquema no diverjan | en el build |
| Gate SDD (`consumer-release`) | que la feature tenga contrato, criterios y trazas | en la entrega |

## Gobierno

Este módulo tiene EDAIOS Core 3.1.0 inyectado (perfil `consumer-release`,
ADR-0016). Ver [docs/EDAIOS-INJECTION-MAP.html](docs/EDAIOS-INJECTION-MAP.html)
y [docs/edaios-arquitectura-proyecto.svg](docs/edaios-arquitectura-proyecto.svg)
para el mapa completo de lo inyectado.

La feature vigente vive en [`specs/001-revenue-ventana-cliente/`](specs/001-revenue-ventana-cliente/).

Validar la feature contra el gate:

```bash
python3 tools/validation/spec_kit_gate.py . \
  --feature specs/001-revenue-ventana-cliente \
  --profile consumer-release
```

## Cómo ejecutarlo

### El canary (no necesita infraestructura)

Es la demo. Un solo comando, reversible, ~5 segundos:

```bash
cd data-kcd2026 && ./canary-deriva.sh
```

Rompe `totalPrice` en el contrato, compila, muestra el fallo, restaura y vuelve a
verde. La variante por cambio de tipo:

```bash
./canary-deriva.sh --variante tipo
```

### La verificación numérica (tampoco necesita infraestructura)

`RevenueWindowPipelineTest` ejecuta **el mismo pipeline** que el job sobre un
fixture determinista y lo compara contra una tabla de referencia calculada a mano:

```bash
./mvnw -pl data-kcd2026 test
```

### El job completo (sí necesita infraestructura)

```bash
cd infra/dockercompose && docker compose up -d      # desde la raíz del repo
./mvnw -pl data-kcd2026 clean package               # genera el jar shaded
```

Subir `target/data-kcd2026-1.0-SNAPSHOT-shaded.jar` al Flink Dashboard
(`http://localhost:18081`) con entry class
`com.topaya.kcd2026.revenue.RevenueWindowJob`. Parámetros y sus valores por defecto:

| Parámetro | Defecto |
|---|---|
| `--bootstrap` | `broker:9092` |
| `--registry` | `http://registry:8081` |
| `--input-topic` | `orders-kcd2026` |
| `--output-topic` | `revenue-por-cliente-ventana` |
| `--window-seconds` | `60` |
| `--out-of-orderness-seconds` | `5` |

## Por qué el canary funciona

`avro-maven-plugin` genera las clases Java desde los `.avsc` en `generate-sources`,
y el job accede a los campos por los **accesores generados** (`getTotalPrice()`),
nunca por acceso dinámico por nombre. Esa decisión es la que hace que la deriva sea
un error de compilación:

```
symbol:   method getTotalPrice()
location: variable order of type com.topaya.kcd2026.model.avro.OrderEvent
```

Si alguien reescribiera el job usando `GenericRecord.get("totalPrice")`, la deriva
dejaría de romper el build y el canary perdería todo su valor. El script lo detecta:
si la compilación del paso 3 pasa, aborta y lo denuncia.

## Estado

Feature 001 en fase `tasked`. Implementación ejecutada y verificada; queda
pendiente la firma del owner para cerrarla (Artículo VI).
