/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.a_kafka;

import com.topaya.cdckafkaflinkiceberg.utilitarios.UtilitarioPoblarDataKafka;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JobKafka {
    public static void main(String[] args) {
        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Configuracion del deserializador que usaremos en las lectura de topicos orders de
            // Kafka
            InputStream resourceOrders =
                    UtilitarioPoblarDataKafka.class.getResourceAsStream("/model/orders.avsc");
            if (resourceOrders == null) {
                throw new RuntimeException(
                        "No fue posible leer el recurso avro definido como: /model/orders.avsc");
            }
            DeserializationSchema<GenericRecord> avroValueDeserializador =
                    ConfluentRegistryAvroDeserializationSchema.forGeneric(
                            new Schema.Parser().parse(resourceOrders), "http://registry:8081");
            // Configuración de las propiedades del broker de Kafka
            Properties kafkaBrokerProperties = new Properties();
            kafkaBrokerProperties.put("bootstrap.servers", "broker:9092");
            kafkaBrokerProperties.setProperty("group.id", "kcd-lima-arch-008");
            // Configuración de la fuente de datos Kafka del topico de 'orders'
            KafkaSource<GenericRecord> ordersSource =
                    KafkaSource.<GenericRecord>builder()
                            .setProperties(kafkaBrokerProperties)
                            .setTopics(
                                    Arrays.asList(
                                            "orders")) // Asegurate que el topico ya este creado en
                            // el servidor
                            .setValueOnlyDeserializer(avroValueDeserializador)
                            .setStartingOffsets(OffsetsInitializer.latest())
                            .build();
            DataStream<GenericRecord> streamOrders =
                    env.fromSource(ordersSource, WatermarkStrategy.noWatermarks(), "kafka-orders");

            streamOrders.print().name("task-print-kafka-record-leidos").setParallelism(3);

            env.execute("job-leer-topico-ordenes-venta");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
