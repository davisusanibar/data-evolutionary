package com.topaya.kcd2026.revenue;

import com.topaya.kcd2026.model.avro.CustomerRevenueWindow;
import com.topaya.kcd2026.model.avro.OrderEvent;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * La lógica de negocio del caso, aislada de las fuentes y los sinks.
 *
 * <p>Separarla no es un adorno: permite que la verificación numérica de SC-001 se
 * ejecute sobre un fixture determinista en local, sin Kafka, sin Schema Registry y
 * sin cluster. El job de producción y el test ejercitan <em>el mismo</em> código de
 * agregación, así que la cifra verificada es la cifra que se ejecuta.
 */
public final class RevenueWindowPipeline {

  /**
   * Política de eventos tardíos, declarada y no implícita.
   *
   * <p>Un evento que llega con un retraso mayor que el desorden tolerado, pero
   * dentro de esta tolerancia adicional, reabre su ventana y emite un resultado
   * corregido. Más allá de este margen, el evento se descarta. Se declara aquí y
   * se documenta en la spec porque una política de descarte silenciosa es una
   * cifra sin fuente.
   */
  public static final Duration ALLOWED_LATENESS = Duration.ofSeconds(10);

  private RevenueWindowPipeline() {}

  /**
   * Construye la agregación de revenue por cliente y ventana.
   *
   * @param orders flujo de órdenes sin marcas de agua asignadas
   * @param windowSize tamaño de la ventana de tiempo de evento
   * @param outOfOrderness desorden máximo tolerado antes de considerar tardío un evento
   */
  public static DataStream<CustomerRevenueWindow> build(
      DataStream<OrderEvent> orders, Duration windowSize, Duration outOfOrderness) {

    WatermarkStrategy<OrderEvent> watermarks =
        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(outOfOrderness)
            // El timestamp del evento sale del contrato, no del reloj del cluster:
            // es lo que hace reproducible el resultado entre ejecuciones.
            .withTimestampAssigner((order, recordTimestamp) -> order.getOrderTs());

    return orders
        .assignTimestampsAndWatermarks(watermarks)
        .keyBy(new CustomerKeySelector())
        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize.toMillis())))
        .allowedLateness(Time.milliseconds(ALLOWED_LATENESS.toMillis()))
        .aggregate(new RevenueAggregateFunction(), new AttachWindowMetadata())
        .returns(new AvroTypeInfo<>(CustomerRevenueWindow.class));
  }
}
