/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.e_cdckafkaflink;

import com.topaya.cdckafkaflinkiceberg.model.avro.CustomerTotalCount;
import com.topaya.cdckafkaflinkiceberg.model.avro.CustomerTotalPrice;
import com.topaya.cdckafkaflinkiceberg.utilitarios.DebeziumDeserializationPagoSchema;
import com.topaya.cdckafkaflinkiceberg.utilitarios.UtilitarioPoblarDataKafka;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class JobStreamingCDCKafkaFlink {

    protected static final Logger logger = LogManager.getLogger(JobStreamingCDCKafkaFlink.class);

    public static void main(String[] args) {
        ejecutarFlinkJoinKafkaPostgreSQLCDC();
    }

    public static void ejecutarFlinkJoinKafkaPostgreSQLCDC() {
        logger.info("/********************** AMBIENTE - INICIO **********************/");
        // Configuración del entorno de ejecucion
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(); ) {
            env.setParallelism(
                    1); // Para skipear errores de tipo: [Flink-Metric-View-Updater-thread-1] WARN
            // NetworkBufferPool - Memory usage [187%] is too high to satisfy all of the
            // requests. This can severely impact network throughput. Please consider
            // increasing available network memory, or decreasing configured size of network
            // buffer pools
            logger.info("/********************** AMBIENTE - FIN **********************/");

            logger.info(
                    "/********************** FLINK KAFKA SOURCE ORDERS - INICIO"
                            + " **********************/");
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
                            .setTopics(Arrays.asList("orders"))
                            .setValueOnlyDeserializer(avroValueDeserializador)
                            .setStartingOffsets(OffsetsInitializer.latest())
                            .build();
            DataStream<GenericRecord> streamOrders =
                    env.fromSource(ordersSource, WatermarkStrategy.noWatermarks(), "kafka-orders");

            streamOrders.print().name("task-print-kafka-record-leidos").setParallelism(3);
            logger.info(
                    "/********************** FLINK KAFKA SOURCE ORDERS - FIN"
                            + " **********************/");

            logger.info(
                    "/********************** FLINK POSTGRESQL CDC - INICIO"
                            + " **********************/");
            JdbcIncrementalSource<Tuple1<Long>> jdbcIncrementalSource =
                    PostgresSourceBuilder.PostgresIncrementalSource.<Tuple1<Long>>builder()
                            .hostname("postgres")
                            .port(5432)
                            .database("postgres")
                            .schemaList("public")
                            .tableList("public.payments")
                            .username("postgres")
                            .password("flink2024!")
                            .slotName(UtilitarioPoblarDataKafka.slotName)
                            .decodingPluginName("pgoutput")
                            .deserializer(new DebeziumDeserializationPagoSchema())
                            .includeSchemaChanges(true)
                            .splitSize(2)
                            .build();
            DataStream<Tuple1<Long>> streamPagos =
                    env.fromSource(
                                    jdbcIncrementalSource,
                                    WatermarkStrategy.noWatermarks(),
                                    "leer-source-cdc-pagos")
                            .name("task-mysql-cdc-pagos")
                            .setParallelism(1);

            // imprimir registros PostgreSQL CDC descubiertos por Flink/Debezium
            streamPagos.print().name("task-print-lectura-cdc-pagos").setParallelism(2);

            logger.info(
                    "/********************** FLINK POSTGRESQL CDC - FIN **********************/");

            logger.info(
                    "/********************** JOIN POSTGRESQL + KAFKA (WHERE ORDER_ID) - INICIO"
                            + " **********************/");
            DataStream<Tuple3<Long, Long, Double>> joinedOrdersYPagos =
                    streamPagos
                            .join(streamOrders)
                            .where((Tuple1<Long> pago) -> pago.f0)
                            .equalTo(
                                    (GenericRecord order) ->
                                            Long.valueOf(order.get("o_orderkey").toString()))
                            .window(
                                    TumblingProcessingTimeWindows.of(
                                            org.apache.flink.streaming.api.windowing.time.Time
                                                    .seconds(20)))
                            .apply(
                                    new JoinFunction<>() {
                                        @Override
                                        public Tuple3<Long, Long, Double> join(
                                                Tuple1<Long> pago, GenericRecord order)
                                                throws Exception {
                                            logger.info(
                                                    "Join ---> Order ID: "
                                                            + pago.f0
                                                            + ", Customer ID: "
                                                            + order.get("o_custkey"));
                                            BigDecimal oTotalprice =
                                                    new BigDecimal(
                                                            new BigInteger(
                                                                    ((GenericData.Fixed)
                                                                                    order.get(
                                                                                            "o_totalprice"))
                                                                            .bytes()),
                                                            2);

                                            return new Tuple3<>(
                                                    pago.f0,
                                                    Long.parseLong(
                                                            order.get("o_custkey").toString()),
                                                    oTotalprice.doubleValue());
                                        }
                                    });
            logger.info(
                    "/********************** JOIN POSTGRESQL + KAFKA (WHERE ORDER_ID) - FIN"
                            + " **********************/");

            joinedOrdersYPagos
                    .print()
                    .name("task-print-join-kafka-postgres-cdc-pagos")
                    .setParallelism(3);

            logger.info(
                    "/********************** REPARTICION DE DATA FOR KEY CONSUMER_ID - INICIO"
                            + " **********************/");
            KeyedStream<Tuple3<Long, Long, Double>, Long> groupedStreamPorCustomerKey =
                    joinedOrdersYPagos.keyBy(
                            record -> {
                                return record.f1; // // reparticiona topico bajo este key
                            });
            logger.info(
                    "/********************** REPARTICION DE DATA FOR KEY CONSUMER_ID - FIN"
                            + " **********************/");

            groupedStreamPorCustomerKey
                    .print()
                    .name("task-print-reparticion-join-kafka-postgres-cdc-pagos")
                    .setParallelism(3);

            logger.info(
                    "/********************** BRANCH 01: SUMAR TOTAL DINERO EN COMPRAS POR"
                            + " CUSTOMER_ID - INICIO **********************/");
            SingleOutputStreamOperator<Tuple3<Long, Long, Double>> streamNotificaciones =
                    groupedStreamPorCustomerKey
                            .sum(2)
                            .name("task-sums-total-compras")
                            .setParallelism(3);
            // Print de la data a enviar a Kafka Topic notificaciones-montototal-order
            streamNotificaciones
                    .map(
                            record -> {
                                logger.info(
                                        "SUMA ---> Orden ID: "
                                                + record.f0
                                                + ", Customer ID: "
                                                + record.f1
                                                + ", Monto Acumulado: "
                                                + record.f2);
                                return null;
                            })
                    .name("task-print-sums-total-compras")
                    .setParallelism(3);
            // Configuracion del serializador que usaremos en las escritura del topicos
            // notificaciones-montototal-order de Kafka
            ConfluentRegistryAvroSerializationSchema<CustomerTotalPrice> avroValueSerializador =
                    ConfluentRegistryAvroSerializationSchema.forSpecific(
                            CustomerTotalPrice.class,
                            "notificaciones-registry-total",
                            "http://registry:8081");
            KafkaRecordSerializationSchema<CustomerTotalPrice> recorValueSerializador =
                    KafkaRecordSerializationSchema.builder()
                            .setValueSerializationSchema(avroValueSerializador)
                            .setTopic("notificaciones-montototal-order")
                            .build();
            KafkaSink<CustomerTotalPrice> topicoNotificaciones =
                    KafkaSink.<CustomerTotalPrice>builder()
                            .setKafkaProducerConfig(kafkaBrokerProperties)
                            .setRecordSerializer(recorValueSerializador)
                            .build();
            // Parseando data stream notificaciones hacia modelo avro a enviar a Kafka
            SingleOutputStreamOperator<CustomerTotalPrice> streamNotificacionesToAvro =
                    streamNotificaciones
                            .map(
                                    new MapFunction<
                                            Tuple3<Long, Long, Double>, CustomerTotalPrice>() {
                                        @Override
                                        public CustomerTotalPrice map(
                                                Tuple3<Long, Long, Double> value) throws Exception {
                                            CustomerTotalPrice customerTotalPrice =
                                                    new CustomerTotalPrice(
                                                            value.f0, value.f1, value.f2);
                                            return customerTotalPrice;
                                        }
                                    })
                            .name("task-suma-comprar-map-tuple-hacia-avro")
                            .setParallelism(3);
            // Enviamos la sumas totales de ordenes a topico de notificaciones por monto total
            streamNotificacionesToAvro
                    .sinkTo(topicoNotificaciones)
                    .name("task-sink-kafka-suma-compras")
                    .setParallelism(2);
            logger.info(
                    "/********************** BRANCH 01: FLUJO EVALUAR TOTAL DINERO EN COMPRAS POR"
                            + " CUSTOMER_ID - FIN **********************/");

            logger.info(
                    "/********************** BRANCH 02: CONTAR CANTIDAD TOTAL DE COMPRAS POR"
                            + " CUSTOMER_ID - INICIO **********************/");
            SingleOutputStreamOperator<Tuple2<Long, Long>> streamNotificacionesConteo =
                    groupedStreamPorCustomerKey
                            .process(
                                    new KeyedProcessFunction<
                                            Long,
                                            Tuple3<Long, Long, Double>,
                                            Tuple2<Long, Long>>() {
                                        ValueState<Long> countState;

                                        @Override
                                        public void open(Configuration config) {
                                            ValueStateDescriptor<Long> countStateDescriptor =
                                                    new ValueStateDescriptor<>(
                                                            "countState", Types.LONG);
                                            countState =
                                                    getRuntimeContext()
                                                            .getState(countStateDescriptor);
                                        }

                                        @Override
                                        public void processElement(
                                                Tuple3<Long, Long, Double> value,
                                                Context ctx,
                                                Collector<Tuple2<Long, Long>> out)
                                                throws Exception {

                                            Long count = countState.value();

                                            if (count == null) {
                                                count = 0L;
                                            }
                                            count++;
                                            countState.update(count);

                                            out.collect(new Tuple2<>(value.f1, count));
                                        }
                                    })
                            .name("task-contar-compras")
                            .setParallelism(3);
            // Print de la data a enviar a Kafka Topic notificaciones-count-order
            streamNotificacionesConteo
                    .map(
                            record -> {
                                logger.info(
                                        "COUNT ---> Customer ID: "
                                                + record.f0
                                                + ", Total Count Acumulado TX: "
                                                + record.f1);
                                return null;
                            })
                    .name("task-print-contar-compras")
                    .setParallelism(3);
            // Configuracion del serializador que usaremos en las escritura del topicos
            // notificaciones-count-order de Kafka
            ConfluentRegistryAvroSerializationSchema<CustomerTotalCount>
                    serializadorTotalCountRegistry =
                            ConfluentRegistryAvroSerializationSchema.forSpecific(
                                    CustomerTotalCount.class,
                                    "notificaciones-registry-count",
                                    "http://registry:8081");
            KafkaRecordSerializationSchema<CustomerTotalCount> serializadorTotalCountKafka =
                    KafkaRecordSerializationSchema.builder()
                            .setValueSerializationSchema(serializadorTotalCountRegistry)
                            .setTopic("notificaciones-count-order")
                            .build();
            KafkaSink<CustomerTotalCount> topicoNotificacionesTotalCount =
                    KafkaSink.<CustomerTotalCount>builder()
                            .setKafkaProducerConfig(kafkaBrokerProperties)
                            .setRecordSerializer(serializadorTotalCountKafka)
                            .build();
            // Parseando data stream notificaciones hacia modelo avro a enviar a Kafka
            SingleOutputStreamOperator<CustomerTotalCount> streamNotificacionesTotalCountToAvro =
                    streamNotificacionesConteo
                            .map(
                                    new MapFunction<Tuple2<Long, Long>, CustomerTotalCount>() {
                                        @Override
                                        public CustomerTotalCount map(Tuple2<Long, Long> value)
                                                throws Exception {
                                            CustomerTotalCount customerTotalCount =
                                                    new CustomerTotalCount(value.f0, value.f1);
                                            return customerTotalCount;
                                        }
                                    })
                            .name("task-contar-compras-de-tuple-a-avro")
                            .setParallelism(3);
            // Enviamos la cantidad totales de ordenes a topico de notificaciones por customer
            streamNotificacionesTotalCountToAvro
                    .sinkTo(topicoNotificacionesTotalCount)
                    .name("task-sink-kafka-contar-compras")
                    .setParallelism(2);
            logger.info(
                    "/********************** BRANCH 02: CONTAR CANTIDAD TOTAL DE COMPRAS POR"
                            + " CUSTOMER_ID - FIN **********************/");

            logger.info(
                    "/********************** FLINK CORRER PROCESO - INICIO"
                            + " **********************/");
            // Correr job de proceso
            try {
                env.execute("join-kafka-orders-mysql-pagos-to-kafka-notificaciones");
            } catch (Exception e) {
                logger.error("Error al ejecutar el job. Detalle del error: " + e.getMessage());
                throw new RuntimeException(e);
            }
            logger.info(
                    "/********************** FLINK CORRER PROCESO - FIN **********************/");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
