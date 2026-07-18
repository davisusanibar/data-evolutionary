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

## Infraestructura

No levanta servicios propios: reutiliza `infra/dockercompose` de la raíz del
repositorio (Kafka, Schema Registry, Flink 1.20.x, Postgres, MinIO/Iceberg).

```bash
cd ../infra/dockercompose && docker compose up -d
```

## Estado

La feature 001 está en fase `tasked` (contrato y plan aprobados, tareas
pendientes de implementación). Nada se cierra sin la firma del owner.
