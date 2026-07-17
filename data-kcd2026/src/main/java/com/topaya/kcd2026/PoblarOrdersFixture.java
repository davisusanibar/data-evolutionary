/* (C)2026 */
package com.topaya.kcd2026;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Properties;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Puebla el topico `orders` con fixtures sinteticos para la demo.
 *
 * <p>No es parte del pipeline: es utilitario de demo. Los importes son deterministas para que
 * SC-002 sea verificable a ojo en el escenario, e incluye deliberadamente una orden incompleta para
 * ejercitar FR-004 en vivo.
 *
 * <p>Debe ejecutarse dentro de la red del compose: el broker anuncia `broker:9092`, que no resuelve
 * desde el host.
 *
 * <pre>
 * docker run --rm --network topaya -v "$PWD/data-kcd2026/target:/jar" \
 *   flink:1.20.2-scala_2.12-java11 \
 *   java -cp /jar/data-kcd2026-1.0-SNAPSHOT-shaded.jar \
 *   com.topaya.kcd2026.PoblarOrdersFixture
 * </pre>
 */
public class PoblarOrdersFixture {

    private static final String SUBJECT = JobOrdersRevenueWindow.TOPICO_ORIGEN + "-value";

    public static void main(String[] args) throws Exception {
        String bootstrap = arg(args, "--bootstrap", JobOrdersRevenueWindow.BOOTSTRAP_SERVERS);
        String registry = arg(args, "--registry", JobOrdersRevenueWindow.REGISTRY);

        Schema esquema = JobOrdersRevenueWindow.leerEsquema(JobOrdersRevenueWindow.ESQUEMA_ORIGEN);
        ConfluentRegistryAvroSerializationSchema<GenericRecord> serializador =
                ConfluentRegistryAvroSerializationSchema.forGeneric(SUBJECT, esquema, registry);
        serializador.open(null);

        Properties propiedades = new Properties();
        propiedades.put("bootstrap.servers", bootstrap);
        propiedades.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        propiedades.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        // Fixtures deterministas. Cliente 7: 100.50 + 49.50 = 150.00 en 2 ordenes (SC-002).
        // Cliente 9: 200.00 en 1 orden. La cuarta no tiene custkey: FR-004 debe descartarla.
        Object[][] fixtures = {
            {1L, 7L, new BigDecimal("100.50")},
            {2L, 7L, new BigDecimal("49.50")},
            {3L, 9L, new BigDecimal("200.00")},
            {4L, null, new BigDecimal("999.99")},
        };

        try (KafkaProducer<byte[], byte[]> productor = new KafkaProducer<>(propiedades)) {
            for (Object[] fixture : fixtures) {
                GenericRecord orden =
                        construirOrden(esquema, (Long) fixture[0], (Long) fixture[1], (BigDecimal) fixture[2]);
                productor.send(
                        new ProducerRecord<>(
                                JobOrdersRevenueWindow.TOPICO_ORIGEN, null, serializador.serialize(orden)));
                System.out.printf(
                        "producido: o_orderkey=%s o_custkey=%s o_totalprice=%s%n",
                        fixture[0], fixture[1], fixture[2]);
            }
            productor.flush();
        }
        System.out.println("fixtures enviados a " + JobOrdersRevenueWindow.TOPICO_ORIGEN);
        System.out.println("esperado: custkey 7 -> 150.00 (2 ordenes) | custkey 9 -> 200.00 (1 orden)");
        System.out.println("la orden 4 no tiene custkey: FR-004 debe descartarla");
    }

    static GenericRecord construirOrden(Schema esquema, Long orderkey, Long custkey, BigDecimal importe) {
        GenericRecord orden = new GenericData.Record(esquema);
        orden.put("o_orderkey", orderkey);
        orden.put("o_custkey", custkey);
        orden.put("o_orderstatus", "O");
        orden.put("o_totalprice", aFixedDecimal(esquema, importe));
        orden.put("o_orderdate", (int) LocalDate.now().toEpochDay());
        orden.put("o_orderpriority", "1-URGENT");
        orden.put("o_clerk", "Clerk#000000001");
        orden.put("o_shippriority", 0);
        orden.put("o_comment", "fixture kcd 2026");
        return orden;
    }

    /** El contrato declara o_totalprice como decimal fixed(7); hay que codificarlo como tal. */
    static Object aFixedDecimal(Schema esquema, BigDecimal importe) {
        Schema campo = noNulo(esquema.getField("o_totalprice").schema());
        LogicalType logico = campo.getLogicalType();
        if (logico == null) {
            logico = LogicalTypes.decimal(15, 2);
        }
        return new Conversions.DecimalConversion()
                .toFixed(importe.setScale(2), campo, logico);
    }

    private static Schema noNulo(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema opcion : schema.getTypes()) {
                if (opcion.getType() != Schema.Type.NULL) {
                    return opcion;
                }
            }
        }
        return schema;
    }

    private static String arg(String[] args, String nombre, String porDefecto) {
        for (int i = 0; i < args.length - 1; i++) {
            if (nombre.equals(args[i])) {
                return args[i + 1];
            }
        }
        return porDefecto;
    }
}
