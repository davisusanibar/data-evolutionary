/* (C)2026 */
package com.topaya.kcd2026;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Logica de agregacion del revenue, separada del wiring de Flink para poder verificarla sin
 * cluster.
 *
 * <p>SC-002, SC-003 y SC-004 se demuestran con unit tests sobre estas funciones. Un pipeline que
 * solo se puede verificar levantando Docker no tiene criterios de exito falsables.
 */
public final class RevenueVentana {

    private RevenueVentana() {}

    /** Campos del contrato de origen que FR-004 exige presentes. */
    static final String CUSTKEY = "o_custkey";

    static final String TOTALPRICE = "o_totalprice";

    /** Acumulador de una ventana: suma e importe observado. */
    public static class Acumulado {
        public double suma;
        public int conteo;

        public Acumulado() {
            this(0.0d, 0);
        }

        public Acumulado(double suma, int conteo) {
            this.suma = suma;
            this.conteo = conteo;
        }
    }

    /**
     * FR-004: una orden sin custkey o sin importe se descarta.
     *
     * <p>Publica como funcion pura para que SC-004 no dependa de Flink.
     */
    public static boolean esCompleta(GenericRecord orden) {
        if (orden == null) {
            return false;
        }
        return orden.get(CUSTKEY) != null && orden.get(TOTALPRICE) != null;
    }

    public static long custkeyDe(GenericRecord orden) {
        return ((Number) orden.get(CUSTKEY)).longValue();
    }

    /**
     * Convierte el importe del origen a double.
     *
     * <p>Clarification 2 de la spec: el origen declara decimal fixed(7) precision 15 scale 2 y aqui
     * se degrada a double. Por eso la feature NO reclama exactitud fiscal. Un pipeline de
     * facturacion conservaria el decimal.
     */
    public static double importeDe(GenericRecord orden) {
        Object valor = orden.get(TOTALPRICE);
        if (valor instanceof Number) {
            return ((Number) valor).doubleValue();
        }
        if (valor instanceof GenericFixed) {
            return decimalDeBytes(((GenericFixed) valor).bytes(), esquemaDe(orden, TOTALPRICE));
        }
        if (valor instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) valor).duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return decimalDeBytes(bytes, esquemaDe(orden, TOTALPRICE));
        }
        if (valor instanceof CharSequence) {
            return Double.parseDouble(valor.toString());
        }
        throw new IllegalArgumentException(
                "importe con tipo no soportado por el contrato: " + valor.getClass().getName());
    }

    private static Schema esquemaDe(GenericRecord orden, String campo) {
        Schema campoSchema = orden.getSchema().getField(campo).schema();
        if (campoSchema.getType() == Schema.Type.UNION) {
            for (Schema opcion : campoSchema.getTypes()) {
                if (opcion.getType() != Schema.Type.NULL) {
                    return opcion;
                }
            }
        }
        return campoSchema;
    }

    private static double decimalDeBytes(byte[] bytes, Schema schema) {
        int escala = 0;
        Object propiedad = schema.getObjectProp("scale");
        if (propiedad instanceof Number) {
            escala = ((Number) propiedad).intValue();
        }
        return new BigDecimal(new BigInteger(bytes), escala).doubleValue();
    }

    /** Suma el importe y cuenta las ordenes de la ventana. */
    public static class Agregador implements AggregateFunction<GenericRecord, Acumulado, Acumulado> {

        @Override
        public Acumulado createAccumulator() {
            return new Acumulado();
        }

        @Override
        public Acumulado add(GenericRecord orden, Acumulado acumulado) {
            acumulado.suma += importeDe(orden);
            acumulado.conteo += 1;
            return acumulado;
        }

        @Override
        public Acumulado getResult(Acumulado acumulado) {
            return acumulado;
        }

        @Override
        public Acumulado merge(Acumulado a, Acumulado b) {
            return new Acumulado(a.suma + b.suma, a.conteo + b.conteo);
        }
    }

    /**
     * FR-002: construye el registro de salida con los cinco campos del contrato.
     *
     * <p>El esquema se resuelve del classpath, no se declara aqui.
     */
    public static class AdjuntarVentana
            extends ProcessWindowFunction<Acumulado, GenericRecord, Long, TimeWindow> {

        private transient Schema esquema;

        @Override
        public void open(org.apache.flink.configuration.Configuration configuracion) {
            esquema = JobOrdersRevenueWindow.leerEsquema(JobOrdersRevenueWindow.ESQUEMA_DESTINO);
        }

        @Override
        public void process(
                Long custkey,
                Context contexto,
                Iterable<Acumulado> acumulados,
                Collector<GenericRecord> salida) {
            Acumulado acumulado = acumulados.iterator().next();
            salida.collect(
                    construir(
                            esquema,
                            custkey,
                            contexto.window().getStart(),
                            contexto.window().getEnd(),
                            acumulado));
        }
    }

    /** Construye el registro de salida. Publico para que SC-002 lo verifique sin cluster. */
    public static GenericRecord construir(
            Schema esquema, long custkey, long inicio, long fin, Acumulado acumulado) {
        GenericRecord registro = new GenericData.Record(esquema);
        registro.put("o_custkey", custkey);
        registro.put("window_start", inicio);
        registro.put("window_end", fin);
        registro.put("sum_o_totalprice", acumulado.suma);
        registro.put("order_count", acumulado.conteo);
        return registro;
    }
}
