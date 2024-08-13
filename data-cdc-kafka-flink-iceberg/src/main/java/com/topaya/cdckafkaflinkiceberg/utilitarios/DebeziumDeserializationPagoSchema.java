/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.utilitarios;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

// necesitamos: order_id, amount
public class DebeziumDeserializationPagoSchema
        implements DebeziumDeserializationSchema<Tuple1<Long>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<Tuple1<Long>> collector)
            throws Exception {
        Struct sourceRecordValue = (Struct) sourceRecord.value();
        if (sourceRecordValue != null) {
            Struct after = sourceRecordValue.getStruct("after");
            Long orderId = after.getInt64("order_id");
            collector.collect(new Tuple1<>(orderId));
        }
    }

    @Override
    public TypeInformation<Tuple1<Long>> getProducedType() {
        return Types.TUPLE(Types.LONG);
    }
}
