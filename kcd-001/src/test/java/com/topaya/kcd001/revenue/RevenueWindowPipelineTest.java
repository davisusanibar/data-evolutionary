package com.topaya.kcd001.revenue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.topaya.kcd001.model.avro.CustomerRevenueWindow;
import com.topaya.kcd001.model.avro.OrderEvent;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verificación de SC-001: el revenue por cliente y ventana coincide exactamente con
 * la tabla de referencia.
 *
 * <p>Se ejecuta en local, sin Kafka, sin Schema Registry y sin cluster, sobre el
 * <em>mismo</em> {@link RevenueWindowPipeline} que usa el job de producción. La
 * infraestructura queda fuera del alcance de esta comprobación a propósito: lo que
 * se verifica aquí es la aritmética del caso de uso, no el transporte.
 */
class RevenueWindowPipelineTest {

  private static final Duration WINDOW = Duration.ofSeconds(60);
  private static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(5);
  private static final double EPSILON = 1e-9;

  @Test
  @DisplayName("SC-001 · el revenue agregado coincide con la tabla de referencia")
  void revenueCoincideConLaReferencia() throws Exception {
    List<OrderEvent> orders = leerOrdenes("/fixture/orders.csv");
    Map<String, String> referencia = leerReferencia("/fixture/revenue-referencia.csv");

    assertEquals(9, orders.size(), "el fixture debe aportar las 9 ordenes declaradas");
    assertEquals(4, referencia.size(), "la referencia debe declarar 4 filas (2 clientes x 2 ventanas)");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
    // Paralelismo 1: el fixture es pequeño y el orden de emisión importa para
    // ejercitar el evento desordenado de forma reproducible.
    env.setParallelism(1);

    DataStream<OrderEvent> source =
        env.fromCollection(orders, new AvroTypeInfo<>(OrderEvent.class));

    DataStream<CustomerRevenueWindow> resultado =
        RevenueWindowPipeline.build(source, WINDOW, OUT_OF_ORDERNESS);

    Map<String, CustomerRevenueWindow> obtenido = new LinkedHashMap<>();
    try (CloseableIterator<CustomerRevenueWindow> it = resultado.executeAndCollect()) {
      while (it.hasNext()) {
        CustomerRevenueWindow fila = it.next();
        String clave = clave(fila.getCustomerId(), fila.getWindowStart(), fila.getWindowEnd());
        CustomerRevenueWindow previo = obtenido.put(clave, fila);
        assertTrue(
            previo == null,
            "la ventana " + clave + " emitio mas de un resultado; la agregacion no es unica");
      }
    }

    assertEquals(
        referencia.keySet(),
        obtenido.keySet(),
        "el conjunto de (cliente, ventana) producido no coincide con la referencia");

    for (Map.Entry<String, String> esperado : referencia.entrySet()) {
      String clave = esperado.getKey();
      String[] valores = esperado.getValue().split(",");
      double revenueEsperado = Double.parseDouble(valores[0]);
      long ordenesEsperadas = Long.parseLong(valores[1]);

      CustomerRevenueWindow real = obtenido.get(clave);
      assertEquals(
          revenueEsperado,
          real.getRevenueTotal(),
          EPSILON,
          "revenue distinto del de referencia en " + clave);
      assertEquals(
          ordenesEsperadas,
          real.getOrderCount(),
          "numero de ordenes distinto del de referencia en " + clave);
    }
  }

  private static String clave(long customerId, long windowStart, long windowEnd) {
    return customerId + "@" + windowStart + "-" + windowEnd;
  }

  private static List<OrderEvent> leerOrdenes(String recurso) throws Exception {
    List<OrderEvent> ordenes = new ArrayList<>();
    for (String linea : leerLineas(recurso)) {
      String[] c = linea.split(",");
      ordenes.add(
          OrderEvent.newBuilder()
              .setOrderId(Long.parseLong(c[0]))
              .setCustomerId(Long.parseLong(c[1]))
              .setTotalPrice(Double.parseDouble(c[2]))
              .setOrderTs(Long.parseLong(c[3]))
              .build());
    }
    return ordenes;
  }

  /** Devuelve clave (cliente@ventana) -> "revenue,ordenes". */
  private static Map<String, String> leerReferencia(String recurso) throws Exception {
    Map<String, String> filas = new LinkedHashMap<>();
    for (String linea : leerLineas(recurso)) {
      String[] c = linea.split(",");
      filas.put(
          clave(Long.parseLong(c[0]), Long.parseLong(c[1]), Long.parseLong(c[2])),
          c[3] + "," + c[4]);
    }
    return filas;
  }

  private static List<String> leerLineas(String recurso) throws Exception {
    List<String> lineas = new ArrayList<>();
    try (InputStream in = RevenueWindowPipelineTest.class.getResourceAsStream(recurso);
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String linea;
      while ((linea = reader.readLine()) != null) {
        linea = linea.trim();
        if (!linea.isEmpty() && !linea.startsWith("#")) {
          lineas.add(linea);
        }
      }
    }
    return lineas;
  }
}
