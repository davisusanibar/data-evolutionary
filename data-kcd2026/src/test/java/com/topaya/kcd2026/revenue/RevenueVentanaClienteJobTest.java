package com.topaya.kcd2026.revenue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.topaya.kcd2026.revenue.avro.Orden;
import com.topaya.kcd2026.revenue.avro.RevenueVentana;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.jupiter.api.Test;

/**
 * SC-001 (specs/001-revenue-ventana-cliente/spec.md): fixture sintetico
 * determinista con un evento tardio dentro de la tolerancia de 5s y uno
 * fuera de ella, verificado contra un calculo de referencia hecho aparte del
 * job (no reutiliza la logica de produccion para calcular lo esperado).
 *
 * <p>El fixture se alimenta por fases con pausas cortas: el generador de
 * watermarks periodico de Flink necesita esos huecos para avanzar entre cada
 * fase (patron estandar para probar ventanas de event-time sin depender del
 * arnes interno de operadores).
 */
class RevenueVentanaClienteJobTest {

    private static final String CLIENTE = "c1";
    private static final String CLIENTE_RELLENO = "cliente-de-relleno";

    @Test
    void agregaRevenuePorClienteYDescartaElEventoFueraDeTolerancia() throws Exception {
        // Fixture: ventana [0, 60_000) para CLIENTE.
        List<Orden> ordenes = new ArrayList<>();
        ordenes.add(orden(CLIENTE, 100.00, 5_000L)); // a tiempo
        ordenes.add(orden(CLIENTE, 250.50, 40_000L)); // a tiempo
        ordenes.add(orden(CLIENTE_RELLENO, 1.00, 61_000L)); // empuja el watermark justo tras el cierre
        ordenes.add(orden(CLIENTE, 75.25, 59_000L)); // tardio DENTRO de la tolerancia de 5s
        ordenes.add(orden(CLIENTE_RELLENO, 1.00, 70_000L)); // empuja el watermark mas alla de la tolerancia
        ordenes.add(orden(CLIENTE, 999.00, 59_500L)); // tardio FUERA de la tolerancia: debe descartarse

        List<RevenueVentana> resultados = ejecutarJobConFixture(ordenes);

        List<RevenueVentana> resultadosCliente = new ArrayList<>();
        for (RevenueVentana r : resultados) {
            if (CLIENTE.contentEquals(r.getClienteId())) {
                resultadosCliente.add(r);
            }
        }

        // Calculo de referencia, hecho aparte de la logica del job: solo los
        // tres eventos dentro de tolerancia cuentan; el de 999.00 se excluye.
        double revenueEsperado = 100.00 + 250.50 + 75.25;
        int ordenesEsperadas = 3;

        assertTrue(
                resultadosCliente.size() >= 1,
                "el job debe emitir al menos un resultado para " + CLIENTE);

        RevenueVentana ultimoResultado = resultadosCliente.get(resultadosCliente.size() - 1);
        assertEquals(revenueEsperado, ultimoResultado.getRevenueAcumulado(), 0.001);
        assertEquals(ordenesEsperadas, ultimoResultado.getNumeroOrdenes());

        // Ningun resultado emitido debe incluir el evento fuera de tolerancia
        // (su importe, 999.00, nunca aparece reflejado en el acumulado).
        for (RevenueVentana r : resultadosCliente) {
            assertTrue(
                    r.getRevenueAcumulado() <= revenueEsperado + 0.001,
                    "un resultado incluyo el evento fuera de tolerancia: " + r);
        }
    }

    private static List<RevenueVentana> ejecutarJobConFixture(List<Orden> ordenes) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(10L);

        DataStream<Orden> fuente =
                env.addSource(new OrdenesFixtureSource(ordenes)).returns(Orden.class);

        DataStream<RevenueVentana> revenuePorVentana =
                RevenueVentanaClienteJob.agregarRevenuePorVentana(fuente);

        List<RevenueVentana> resultados = new ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<RevenueVentana> it =
                revenuePorVentana.executeAndCollect()) {
            it.forEachRemaining(resultados::add);
        }
        return resultados;
    }

    private static Orden orden(String clienteId, double importe, long timestampEventoMillis) {
        return Orden.newBuilder()
                .setClienteId(clienteId)
                .setImporte(importe)
                .setTimestampEvento(Instant.ofEpochMilli(timestampEventoMillis))
                .build();
    }

    /**
     * Fuente de prueba: alimenta el fixture por fases con una pausa corta
     * entre cada orden, para darle tiempo al generador de watermarks
     * periodico de Flink a avanzar antes de la siguiente fase.
     */
    private static final class OrdenesFixtureSource implements SourceFunction<Orden> {

        private static final long serialVersionUID = 1L;
        private final List<Orden> ordenes;
        private volatile boolean cancelado = false;

        OrdenesFixtureSource(List<Orden> ordenes) {
            this.ordenes = ordenes;
        }

        @Override
        public void run(SourceContext<Orden> ctx) throws Exception {
            for (Orden orden : ordenes) {
                if (cancelado) {
                    return;
                }
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collectWithTimestamp(orden, orden.getTimestampEvento().toEpochMilli());
                }
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            cancelado = true;
        }
    }
}
