package com.topaya.kcd2026.revenue;

import com.topaya.kcd2026.model.avro.CustomerRevenueWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Convierte el acumulador de una ventana cerrada en el contrato de salida,
 * adjuntando los límites de la ventana.
 *
 * <p>Se combina con {@link RevenueAggregateFunction} en la variante de
 * {@code aggregate} que recibe ambas funciones: la agregación sigue siendo
 * incremental y esta función solo se invoca una vez por ventana cerrada, con el
 * resultado ya reducido.
 */
public class AttachWindowMetadata
    extends ProcessWindowFunction<RevenueAccumulator, CustomerRevenueWindow, Long, TimeWindow> {

  private static final long serialVersionUID = 1L;

  @Override
  public void process(
      Long customerId,
      Context context,
      Iterable<RevenueAccumulator> aggregated,
      Collector<CustomerRevenueWindow> out) {

    RevenueAccumulator acc = aggregated.iterator().next();

    out.collect(
        CustomerRevenueWindow.newBuilder()
            .setCustomerId(customerId)
            .setWindowStart(context.window().getStart())
            .setWindowEnd(context.window().getEnd())
            .setRevenueTotal(acc.revenue)
            .setOrderCount(acc.orders)
            .build());
  }
}
