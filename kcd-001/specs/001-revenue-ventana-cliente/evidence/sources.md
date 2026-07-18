# Fuentes de la feature 001-revenue-ventana-cliente

Regla IV de la constitución: cero cifras sin fuente. Registro de las fuentes
que sustentan las afirmaciones y cifras de `spec.md`.

| Afirmación / cifra | Fuente | Fecha | Alcance | Rótulo |
|---|---|---|---|---|
| El pipeline CDC + Join de KCD 2025 existe y funciona en este repositorio | `data-cdc-kafka-flink-iceberg/src/main/java/com/topaya/cdckafkaflinkiceberg/e_cdckafkaflink/JobStreamingCDCKafkaFlink.java` | 2026-07-18 | Este repositorio, rama `feature/kcd-001` | verdad de repositorio |
| La infraestructura se levanta con docker compose (Kafka, Schema Registry, Postgres, Flink, MinIO, Hive) | `infra/dockercompose/docker-compose.yml` | 2026-07-18 | Este repositorio | verdad de repositorio |
| Flink 1.20.1 y connector Kafka 3.3.0-1.20 como dependencias del módulo | `kcd-001/pom.xml` | 2026-07-18 | Módulo `kcd-001` | verdad de repositorio |
| "30 segundos" del canary (SC-004) e hipótesis de valor | Objetivo declarado por el owner; se verificará midiendo la ejecución del comando del canary | 2026-07-18 | Esta feature | objetivo, no benchmark externo |
| Cálculo de referencia del revenue por ventana (SC-001) | Se producirá como fixture determinista + tabla de referencia en `evidence/` durante la implementación | TBD | Esta feature | pendiente de generación |
