# kcd-001 — Pipelines de datos gobernados por SDD

Módulo de demostración para KCD. A diferencia del resto del repositorio,
**este módulo se construye bajo EDAIOS SDD**: ninguna clase se escribe antes de
que exista una spec con contrato, criterios de éxito y owner declarado.

## Qué demuestra

En KCD 2025 este repositorio mostró un pipeline **CDC + Join** funcionando
(`data-cdc-kafka-flink-iceberg`, clase
`e_cdckafkaflink.JobStreamingCDCKafkaFlink`). Funcionaba, pero nada impedía que
el contrato de datos derivara en silencio.

Este módulo añade el caso **revenue por cliente en ventana temporal** y, sobre
él, el punto que interesa: **el contrato Avro es una barrera de compilación**.
Si el esquema deriva, el pipeline no compila. La deriva se detecta en el build,
no en producción a las 3 de la madrugada.

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
ADR-0016) desde `../edaios`.

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
cd kcd-001 && ./canary-deriva.sh
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
./mvnw -pl kcd-001 test
```

### El job completo (sí necesita infraestructura)

```bash
# 1. Levantar la infraestructura mínima
cd infra/dockercompose
docker-compose up -d broker schema-registry flink-jobmanager flink-taskmanager

# 2. Construir el jar sombreado
./mvnw -pl kcd-001 package -DskipTests

# 3. Someterlo al cluster (dentro de la red Docker)
docker cp kcd-001/target/kcd-001-1.0-SNAPSHOT-shaded.jar flink-jobmanager:/tmp/kcd-001.jar
docker exec flink-jobmanager flink run -d /tmp/kcd-001.jar
```

El job lee Avro de `orders-kcd001` vía Schema Registry (`http://registry:8081`),
agrega por cliente en ventanas de event-time de 60 s y publica en
`revenue-por-cliente-ventana`. UI de Flink: <http://localhost:18081>.

**Nota de entorno**: Kafka anuncia `broker:9092`; para operar desde el host hay
que mapear los hostnames del compose en `/etc/hosts`. La validación de la
feature se hizo íntegramente dentro de la red de Docker.

**Nota de semántica**: con fuente Kafka no acotada, una ventana solo cierra
cuando el watermark supera su fin. Para ver la última ventana hay que seguir
produciendo eventos (el fixture de la validación incluye dos eventos de avance
de watermark). Detalle en
[`specs/001-revenue-ventana-cliente/verification.md`](specs/001-revenue-ventana-cliente/verification.md).
