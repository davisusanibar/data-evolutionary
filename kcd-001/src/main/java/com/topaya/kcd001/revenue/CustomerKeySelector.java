package com.topaya.kcd001.revenue;

import com.topaya.kcd001.model.avro.OrderEvent;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Particiona por cliente.
 *
 * <p>Clase nombrada en lugar de lambda a propósito: una lambda pierde su tipo por
 * borrado y obliga a anotar {@code TypeInformation} a mano. Este repositorio ya
 * documenta esa trampa en {@code c_flink.intermedio.batch}.
 *
 * <p>El acceso al cliente también va por accesor generado, de modo que el contrato
 * gobierna igualmente la clave de particionado.
 */
public class CustomerKeySelector implements KeySelector<OrderEvent, Long> {

  private static final long serialVersionUID = 1L;

  @Override
  public Long getKey(OrderEvent order) {
    return order.getCustomerId();
  }
}
