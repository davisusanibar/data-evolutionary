/* (C)2026 */
package com.topaya.kcd2026;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifica los criterios de exito de specs/001-orders-revenue-window/spec.md.
 *
 * <p>Cada test nombra su SC. Un criterio sin test es un claim sin evidencia.
 */
class RevenueVentanaTest {

    private static final Schema DESTINO =
            JobOrdersRevenueWindow.leerEsquema(JobOrdersRevenueWindow.ESQUEMA_DESTINO);

    /** Esquema minimo de origen: solo los campos que el contrato de este job consume. */
    private static final Schema ORIGEN =
            new Schema.Parser()
                    .parse(
                            "{\"type\":\"record\",\"name\":\"OrdersFixture\","
                                    + "\"namespace\":\"com.topaya.kcd2026.fixture\",\"fields\":["
                                    + "{\"name\":\"o_custkey\",\"type\":[\"null\",\"long\"],\"default\":null},"
                                    + "{\"name\":\"o_totalprice\",\"type\":[\"null\",\"double\"],\"default\":null}]}");

    private static GenericRecord orden(Long custkey, Double importe) {
        GenericRecord registro = new GenericData.Record(ORIGEN);
        registro.put("o_custkey", custkey);
        registro.put("o_totalprice", importe);
        return registro;
    }

    private static RevenueVentana.Acumulado acumular(GenericRecord... ordenes) {
        RevenueVentana.Agregador agregador = new RevenueVentana.Agregador();
        RevenueVentana.Acumulado acumulado = agregador.createAccumulator();
        for (GenericRecord orden : ordenes) {
            if (RevenueVentana.esCompleta(orden)) {
                acumulado = agregador.add(orden, acumulado);
            }
        }
        return agregador.getResult(acumulado);
    }

    @Test
    @DisplayName("SC-002: dos ordenes del mismo cliente en la misma ventana suman y cuentan 2")
    void sc002_sumaYConteoEnLaMismaVentana() {
        RevenueVentana.Acumulado acumulado = acumular(orden(7L, 100.50d), orden(7L, 49.50d));

        assertEquals(150.00d, acumulado.suma, 0.0001d);
        assertEquals(2, acumulado.conteo);

        GenericRecord salida = RevenueVentana.construir(DESTINO, 7L, 0L, 60_000L, acumulado);
        assertEquals(7L, salida.get("o_custkey"));
        assertEquals(150.00d, (double) salida.get("sum_o_totalprice"), 0.0001d);
        assertEquals(2, salida.get("order_count"));
    }

    @Test
    @DisplayName("SC-003: ventanas distintas no acumulan entre si")
    void sc003_ventanasNoAcumulanEntreSi() {
        RevenueVentana.Acumulado primera = acumular(orden(7L, 100.00d));
        RevenueVentana.Acumulado segunda = acumular(orden(7L, 25.00d));

        GenericRecord v1 = RevenueVentana.construir(DESTINO, 7L, 0L, 60_000L, primera);
        GenericRecord v2 = RevenueVentana.construir(DESTINO, 7L, 60_000L, 120_000L, segunda);

        assertEquals(100.00d, (double) v1.get("sum_o_totalprice"), 0.0001d);
        assertEquals(25.00d, (double) v2.get("sum_o_totalprice"), 0.0001d);
        assertFalse(v1.get("window_start").equals(v2.get("window_start")));
    }

    @Test
    @DisplayName("SC-004: una orden sin custkey se descarta y no contamina la ventana")
    void sc004_ordenIncompletaSeDescarta() {
        assertFalse(RevenueVentana.esCompleta(orden(null, 100.00d)));
        assertFalse(RevenueVentana.esCompleta(orden(7L, null)));
        assertTrue(RevenueVentana.esCompleta(orden(7L, 100.00d)));

        RevenueVentana.Acumulado acumulado =
                acumular(orden(7L, 100.00d), orden(null, 999.00d), orden(7L, 50.00d));

        assertEquals(150.00d, acumulado.suma, 0.0001d);
        assertEquals(2, acumulado.conteo);
    }

    @Test
    @DisplayName("SC-006: el tamano de ventana por defecto es declarado y parametrizable")
    void sc006_ventanaParametrizable() {
        assertEquals(60L, JobOrdersRevenueWindow.leerVentanaSegundos(new String[] {}));
        assertEquals(
                30L,
                JobOrdersRevenueWindow.leerVentanaSegundos(new String[] {"--ventana-segundos", "30"}));
    }

    @Test
    @DisplayName("FR-002: el contrato de salida tiene exactamente los cinco campos declarados")
    void fr002_contratoDeSalida() {
        assertEquals(5, DESTINO.getFields().size());
        for (String campo :
                new String[] {
                    "o_custkey", "window_start", "window_end", "sum_o_totalprice", "order_count"
                }) {
            assertTrue(DESTINO.getField(campo) != null, "falta el campo del contrato: " + campo);
        }
    }
}
