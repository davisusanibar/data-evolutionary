/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.b_cdc;

import com.topaya.cdckafkaflinkiceberg.utilitarios.DebeziumDeserializationPagoSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JobCdcPostgreSQL {
    public static void main(String[] args) {
        String SLOT_NAME =
                "cdc_pagos_v1"; // Caused by: org.postgresql.util.PSQLException: ERROR: replication
        // slot name "SLOT_PAGOS" contains invalid character
        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {
            JdbcIncrementalSource<Tuple1<Long>> jdbcIncrementalSource =
                    PostgresSourceBuilder.PostgresIncrementalSource.<Tuple1<Long>>builder()
                            .hostname("postgres")
                            .port(5432)
                            .database("postgres")
                            .schemaList("public")
                            .tableList("public.payments")
                            .username("postgres")
                            .password("flink2024!")
                            .slotName(
                                    SLOT_NAME) // Un slot de replicación en PostgreSQL es una forma
                            // de la base de datos para recordar el progreso de
                            // la replicación lógica, es decir, hasta qué punto
                            // los cambios (insertar, actualizar, eliminar
                            // operaciones) se han transmitido a los
                            // consumidores.
                            .decodingPluginName(
                                    "pgoutput") // Se utiliza para especificar el nombre del plugin
                            // de decodificación que se utilizará para el
                            // registro lógico de Postgres. PostgreSQL utiliza
                            // los complementos de decodificación para convertir
                            // los cambios de datos a nivel interno en un
                            // formato legible que pueda ser consumido por
                            // consumidores externos.
                            .deserializer(new DebeziumDeserializationPagoSchema())
                            .includeSchemaChanges(true)
                            .splitSize(
                                    2) // Por ejemplo, si tienes una tabla con millones de filas y
                            // estableces un splitSize de 2, entonces Flink tratará de
                            // dividir la tabla en dos y leer cada división por separado.
                            .build();
            DataStream<Tuple1<Long>> streamPagos =
                    env.fromSource(
                                    jdbcIncrementalSource,
                                    WatermarkStrategy.noWatermarks(),
                                    "leer-source-cdc-pagos")
                            .name("task-postgres-cdc-pagos")
                            .setParallelism(
                                    1); // La peculiaridad con las fuentes de CDC como Debezium es
            // que originalmente no soportan paralelismo debido al orden
            // necesario para procesar las transacciones de base de
            // datos. Por lo general, se debe mantener un orden estricto
            // de los eventos para garantizar la correcta evolución de
            // los datos. Al incrementar el paralelismo, varias
            // instancias paralelas del flanco intentarán leer desde el
            // mismo slot de replicación de PostgreSQL, lo que podría
            // ocasionar conflictos y errores.

            // imprimir registros PostgreSQL CDC descubiertos por Flink/Debezium
            streamPagos.print().name("task-print-lectura-cdc-pagos").setParallelism(2);

            env.execute("job-postgres-cdc-tabla-pagos");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
