package com.topaya.kcd2026.revenue;

import com.topaya.kcd2026.revenue.avro.Orden;
import com.topaya.kcd2026.revenue.avro.RevenueVentana;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Feature 001 (specs/001-revenue-ventana-cliente): revenue por cliente en
 * ventanas tumbling de event-time de 1 minuto, con tolerancia a eventos
 * tardios de 5 segundos (ver spec.md, seccion Clarifications).
 */
public class RevenueVentanaClienteJob {

    public static final int TAMANO_VENTANA_MINUTOS = 1;
    public static final int TOLERANCIA_TARDIOS_SEGUNDOS = 5;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "broker:9092");
        String schemaRegistryUrl = System.getProperty("schema.registry.url", "http://registry:8081");
        String topicoOrdenes = System.getProperty("topico.ordenes", "kcd2026.ordenes");
        String topicoRevenue = System.getProperty("topico.revenue", "kcd2026.revenue-ventana");

        KafkaSource<Orden> source =
                KafkaSource.<Orden>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setTopics(topicoOrdenes)
                        .setGroupId("kcd2026-revenue-ventana-cliente")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(
                                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                        Orden.class, schemaRegistryUrl))
                        .build();

        DataStream<Orden> ordenes =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-ordenes");

        DataStream<RevenueVentana> revenuePorVentana = agregarRevenuePorVentana(ordenes);

        KafkaRecordSerializationSchema<RevenueVentana> serializador =
                KafkaRecordSerializationSchema.<RevenueVentana>builder()
                        .setTopic(topicoRevenue)
                        .setValueSerializationSchema(
                                ConfluentRegistryAvroSerializationSchema.forSpecific(
                                        RevenueVentana.class,
                                        topicoRevenue + "-value",
                                        schemaRegistryUrl))
                        .build();

        KafkaSink<RevenueVentana> sink =
                KafkaSink.<RevenueVentana>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(serializador)
                        .build();

        revenuePorVentana.sinkTo(sink);

        env.execute("revenue-por-cliente-en-ventana");
    }

    /**
     * Transformacion pura FR-002/FR-003: ventana tumbling de event-time de 1
     * minuto por cliente, con tolerancia a eventos tardios de 5s implementada
     * como {@code allowedLateness} (la orden puede llegar hasta 5s despues de
     * que el watermark cruce el cierre de su ventana; ver spec.md, seccion
     * Clarifications). Sin I/O, para poder probarla igual en produccion
     * (Kafka) y en el fixture de SC-001.
     */
    public static DataStream<RevenueVentana> agregarRevenuePorVentana(DataStream<Orden> ordenes) {
        return ordenes
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Orden>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (orden, recordTimestamp) ->
                                                orden.getTimestampEvento().toEpochMilli()))
                .keyBy(orden -> orden.getClienteId().toString())
                .window(TumblingEventTimeWindows.of(Time.minutes(TAMANO_VENTANA_MINUTOS)))
                .allowedLateness(Time.seconds(TOLERANCIA_TARDIOS_SEGUNDOS))
                .aggregate(new RevenueAggregateFunction(), new RevenueWindowFunction());
    }

    private static class RevenueAggregateFunction
            implements AggregateFunction<Orden, RevenueAcumulador, RevenueAcumulador> {

        @Override
        public RevenueAcumulador createAccumulator() {
            return new RevenueAcumulador(0.0, 0);
        }

        @Override
        public RevenueAcumulador add(Orden orden, RevenueAcumulador acumulador) {
            return new RevenueAcumulador(
                    acumulador.revenueAcumulado + orden.getImporte(),
                    acumulador.numeroOrdenes + 1);
        }

        @Override
        public RevenueAcumulador getResult(RevenueAcumulador acumulador) {
            return acumulador;
        }

        @Override
        public RevenueAcumulador merge(RevenueAcumulador a, RevenueAcumulador b) {
            return new RevenueAcumulador(
                    a.revenueAcumulado + b.revenueAcumulado, a.numeroOrdenes + b.numeroOrdenes);
        }
    }

    private static final class RevenueAcumulador {
        final double revenueAcumulado;
        final int numeroOrdenes;

        RevenueAcumulador(double revenueAcumulado, int numeroOrdenes) {
            this.revenueAcumulado = revenueAcumulado;
            this.numeroOrdenes = numeroOrdenes;
        }
    }

    private static class RevenueWindowFunction
            extends ProcessWindowFunction<RevenueAcumulador, RevenueVentana, String, TimeWindow> {

        @Override
        public void process(
                String clienteId,
                Context context,
                Iterable<RevenueAcumulador> acumuladores,
                Collector<RevenueVentana> out) {
            RevenueAcumulador acumulador = acumuladores.iterator().next();
            out.collect(
                    RevenueVentana.newBuilder()
                            .setClienteId(clienteId)
                            .setInicioVentana(Instant.ofEpochMilli(context.window().getStart()))
                            .setFinVentana(Instant.ofEpochMilli(context.window().getEnd()))
                            .setRevenueAcumulado(acumulador.revenueAcumulado)
                            .setNumeroOrdenes(acumulador.numeroOrdenes)
                            .build());
        }
    }
}
