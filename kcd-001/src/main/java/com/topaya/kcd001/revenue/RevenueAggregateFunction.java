package com.topaya.kcd001.revenue;

import com.topaya.kcd001.model.avro.OrderEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Suma incremental del revenue por cliente dentro de una ventana.
 *
 * <p>Agrega de forma incremental: no retiene los eventos de la ventana en estado,
 * solo el acumulador.
 *
 * <p><strong>Este es el punto donde muerde el canary de deriva.</strong> El acceso
 * al importe se hace por el accesor generado desde el contrato
 * ({@code getTotalPrice()}), no por acceso dinámico por nombre. Si el campo
 * {@code totalPrice} desaparece o cambia de tipo en {@code order_event.avsc}, la
 * clase generada deja de exponer este accesor y <em>esta línea no compila</em>.
 * Usar {@code record.get("totalPrice")} haría que la deriva pasara inadvertida
 * hasta runtime, que es justo lo que esta feature existe para evitar.
 */
public class RevenueAggregateFunction
    implements AggregateFunction<OrderEvent, RevenueAccumulator, RevenueAccumulator> {

  private static final long serialVersionUID = 1L;

  @Override
  public RevenueAccumulator createAccumulator() {
    return new RevenueAccumulator();
  }

  @Override
  public RevenueAccumulator add(OrderEvent order, RevenueAccumulator acc) {
    return new RevenueAccumulator(acc.revenue + order.getTotalPrice(), acc.orders + 1L);
  }

  @Override
  public RevenueAccumulator getResult(RevenueAccumulator acc) {
    return acc;
  }

  @Override
  public RevenueAccumulator merge(RevenueAccumulator a, RevenueAccumulator b) {
    return new RevenueAccumulator(a.revenue + b.revenue, a.orders + b.orders);
  }
}
