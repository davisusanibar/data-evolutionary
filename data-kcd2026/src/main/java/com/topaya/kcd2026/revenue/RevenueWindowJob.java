package com.topaya.kcd2026.revenue;

import com.topaya.kcd2026.model.avro.CustomerRevenueWindow;
import com.topaya.kcd2026.model.avro.OrderEvent;
import java.time.Duration;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Revenue por cliente en ventana temporal — KCD 2026.
 *
 * <p>Lee órdenes en Avro desde Kafka usando el contrato registrado en el Schema
 * Registry, agrega el importe por cliente sobre ventanas de tiempo de evento y
 * publica el resultado en Avro en un tópico de salida.
 *
 * <p>Se ejecuta sobre la infraestructura existente del repositorio
 * ({@code infra/dockercompose}); no requiere servicios nuevos.
 */
public final class RevenueWindowJob {

  private RevenueWindowJob() {}

  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);

    String bootstrap = params.get("bootstrap", "broker:9092");
    String registry = params.get("registry", "http://registry:8081");
    String inputTopic = params.get("input-topic", "orders-kcd2026");
    String outputTopic = params.get("output-topic", "revenue-por-cliente-ventana");
    Duration window = Duration.ofSeconds(params.getLong("window-seconds", 60L));
    Duration outOfOrderness = Duration.ofSeconds(params.getLong("out-of-orderness-seconds", 5L));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // El contrato de entrada gobierna la deserialización: el registro entrega el
    // esquema y Avro materializa la clase específica generada desde el .avsc.
    KafkaSource<OrderEvent> source =
        KafkaSource.<OrderEvent>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inputTopic)
            .setGroupId("kcd2026-revenue-ventana")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(
                ConfluentRegistryAvroDeserializationSchema.forSpecific(OrderEvent.class, registry))
            .build();

    DataStream<OrderEvent> orders =
        env.fromSource(
            source,
            org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
            "orders (avro/schema-registry)",
            new AvroTypeInfo<>(OrderEvent.class));

    DataStream<CustomerRevenueWindow> revenue =
        RevenueWindowPipeline.build(orders, window, outOfOrderness);

    KafkaSink<CustomerRevenueWindow> sink =
        KafkaSink.<CustomerRevenueWindow>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.<CustomerRevenueWindow>builder()
                    .setTopic(outputTopic)
                    .setValueSerializationSchema(
                        ConfluentRegistryAvroSerializationSchema.forSpecific(
                            CustomerRevenueWindow.class, outputTopic + "-value", registry))
                    .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

    revenue.sinkTo(sink).name("revenue por cliente y ventana (avro)");

    env.execute("KCD2026 · revenue por cliente en ventana temporal");
  }
}
