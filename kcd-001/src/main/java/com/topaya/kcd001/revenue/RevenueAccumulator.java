package com.topaya.kcd001.revenue;

import java.io.Serializable;

/**
 * Acumulador de la agregación: importe sumado y número de órdenes.
 *
 * <p>POJO deliberado (campos públicos y constructor sin argumentos) para que Flink
 * lo serialice con su serializador de POJO en vez de caer a Kryo.
 */
public class RevenueAccumulator implements Serializable {

  private static final long serialVersionUID = 1L;

  public double revenue;
  public long orders;

  public RevenueAccumulator() {
    this(0.0d, 0L);
  }

  public RevenueAccumulator(double revenue, long orders) {
    this.revenue = revenue;
    this.orders = orders;
  }
}
