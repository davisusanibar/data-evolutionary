/* (C)2026 */
package com.topaya.kcd2026;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Revenue por cliente en ventana temporal fija.
 *
 * <p>Este job no define su contrato: lo consume. Los esquemas viven en el registry y en los .avsc
 * gobernados por specs/001-orders-revenue-window/spec.md, y tools/contract_check.py falla cerrado si
 * el esquema deriva de lo que la especificacion declara.
 *
 * <p>Trazas: FR-001 (registry, sin esquema embebido), FR-002 (contrato de salida), FR-003 (ventana
 * parametrizable), FR-004 (descarte de incompletas), FR-005 (una sola escritura), FR-007 (contrato
 * en el log de arranque).
 */
public class JobOrdersRevenueWindow {

    private static final Logger logger = LoggerFactory.getLogger(JobOrdersRevenueWindow.class);

    /** Contrato declarado en feature.spec.yaml. Cambiar aqui sin cambiar la spec rompe el gate. */
    static final String TOPICO_ORIGEN = "orders";

    static final String TOPICO_DESTINO = "orders_revenue_window";
    static final String ESQUEMA_ORIGEN = "/model/orders.avsc";
    static final String ESQUEMA_DESTINO = "/model/orders_revenue_window.avsc";
    static final String REGISTRY = "http://registry:8081";
    static final String BOOTSTRAP_SERVERS = "broker:9092";
    static final String GRUPO = "kcd-lima-2026-revenue-window";

    /** FR-003: por defecto declarado; se sobreescribe con --ventana-segundos. */
    static final long VENTANA_SEGUNDOS_POR_DEFECTO = 60L;

    public static void main(String[] args) throws Exception {
        long ventanaSegundos = leerVentanaSegundos(args);
        ejecutarRevenuePorVentana(ventanaSegundos);
    }

    /** FR-003: el tamano de ventana es parametrizable y su valor efectivo es observable. */
    static long leerVentanaSegundos(String[] args) {
        for (int i = 0; i < args.length - 1; i++) {
            if ("--ventana-segundos".equals(args[i])) {
                long valor = Long.parseLong(args[i + 1]);
                if (valor <= 0) {
                    throw new IllegalArgumentException(
                            "--ventana-segundos debe ser positivo, recibido: " + valor);
                }
                return valor;
            }
        }
        return VENTANA_SEGUNDOS_POR_DEFECTO;
    }

    public static void ejecutarRevenuePorVentana(long ventanaSegundos) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(1);

            // FR-007: el contrato se declara en el arranque; nadie tiene que leer el codigo
            // para saber que lee y que escribe este job.
            logger.info("/********************** CONTRATO - INICIO **********************/");
            logger.info("origen  : topico={} esquema={}", TOPICO_ORIGEN, ESQUEMA_ORIGEN);
            logger.info("destino : topico={} esquema={}", TOPICO_DESTINO, ESQUEMA_DESTINO);
            logger.info("registry: {}", REGISTRY);
            logger.info("ventana : {} segundos (processing time, no solapada)", ventanaSegundos);
            logger.info("gobierno: specs/001-orders-revenue-window/spec.md");
            logger.info("/********************** CONTRATO - FIN **********************/");

            // FR-001: el esquema de lectura sale del registry. El .avsc solo se usa para
            // resolver el reader schema; no hay literal de esquema en este archivo.
            Schema esquemaOrigen = leerEsquema(ESQUEMA_ORIGEN);
            Schema esquemaDestino = leerEsquema(ESQUEMA_DESTINO);

            DeserializationSchema<GenericRecord> deserializador =
                    ConfluentRegistryAvroDeserializationSchema.forGeneric(esquemaOrigen, REGISTRY);

            Properties propiedadesKafka = new Properties();
            propiedadesKafka.put("bootstrap.servers", BOOTSTRAP_SERVERS);
            propiedadesKafka.setProperty("group.id", GRUPO);

            KafkaSource<GenericRecord> origen =
                    KafkaSource.<GenericRecord>builder()
                            .setProperties(propiedadesKafka)
                            .setTopics(Collections.singletonList(TOPICO_ORIGEN))
                            .setValueOnlyDeserializer(deserializador)
                            .setStartingOffsets(OffsetsInitializer.latest())
                            .build();

            DataStream<GenericRecord> ordenes =
                    env.fromSource(origen, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                                    "kafka-orders")
                            .name("FR-001 lectura Avro por registry");

            // FR-004: una orden incompleta se descarta y se cuenta; no aborta el job
            // ni contamina la ventana.
            DataStream<GenericRecord> completas =
                    ordenes.filter(new FiltroOrdenCompleta()).name("FR-004 descarte de incompletas");

            // FR-003 + FR-002: agregacion por cliente en ventana fija.
            //
            // El .returns() no es decorativo: GenericRecord es una interfaz y Flink no
            // puede inferir su TypeInformation, asi que cae a Kryo, que no serializa el
            // Schema que el registro lleva adentro y revienta al reenviar el elemento.
            // Declarar GenericRecordAvroTypeInfo usa el serializador Avro, que es el
            // unico que conoce el contrato.
            DataStream<GenericRecord> revenue =
                    completas
                            .keyBy(RevenueVentana::custkeyDe)
                            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(ventanaSegundos)))
                            .aggregate(new RevenueVentana.Agregador(), new RevenueVentana.AdjuntarVentana())
                            .returns(new GenericRecordAvroTypeInfo(esquemaDestino))
                            .name("FR-002 revenue por ventana");

            // FR-005: la unica escritura del job es el topico de salida declarado.
            KafkaSink<GenericRecord> destino =
                    KafkaSink.<GenericRecord>builder()
                            .setKafkaProducerConfig(propiedadesKafka)
                            .setRecordSerializer(
                                    KafkaRecordSerializationSchema.builder()
                                            .setTopic(TOPICO_DESTINO)
                                            .setValueSerializationSchema(
                                                    ConfluentRegistryAvroSerializationSchema.forGeneric(
                                                            TOPICO_DESTINO + "-value", esquemaDestino, REGISTRY))
                                            .build())
                            .build();

            revenue.sinkTo(destino).name("FR-005 escritura unica");

            env.execute("KCD 2026 - revenue por cliente en ventana de " + ventanaSegundos + "s");
        }
    }

    /** Carga un .avsc del classpath. Falla cerrado: sin contrato no hay job. */
    static Schema leerEsquema(String recurso) {
        try (InputStream stream = JobOrdersRevenueWindow.class.getResourceAsStream(recurso)) {
            if (stream == null) {
                throw new IllegalStateException(
                        "contrato ausente en el classpath: "
                                + recurso
                                + "; el job no arranca sin su esquema declarado");
            }
            return new Schema.Parser().parse(stream);
        } catch (java.io.IOException exc) {
            throw new IllegalStateException("no fue posible leer el contrato " + recurso, exc);
        }
    }
}
