/* (C)2026 */
package com.topaya.kcd2026;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

/**
 * FR-004: descarta ordenes incompletas sin abortar el job y las cuenta.
 *
 * <p>El contador es la evidencia de SC-004: un descarte silencioso es indistinguible de un dato que
 * nunca llego. La feature no instala dead-letter queue (Clarification 4): cuenta, no persiste.
 */
public class FiltroOrdenCompleta extends RichFilterFunction<GenericRecord> {

    private static final long serialVersionUID = 1L;

    private transient Counter descartadas;

    @Override
    public void open(Configuration configuracion) {
        descartadas = getRuntimeContext().getMetricGroup().counter("ordenes_descartadas");
    }

    @Override
    public boolean filter(GenericRecord orden) {
        boolean completa = RevenueVentana.esCompleta(orden);
        if (!completa) {
            descartadas.inc();
        }
        return completa;
    }
}
