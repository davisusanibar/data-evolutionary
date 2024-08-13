/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.c_flink.intermedio.batch;

import com.topaya.cdckafkaflinkiceberg.utilitarios.CreadorGenericoDeSource;
import com.topaya.cdckafkaflinkiceberg.utilitarios.DevolverSourceConDataFake;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobBatchConErrorTypeErasure {
    private static final Logger logger = LoggerFactory.getLogger(JobBatchConErrorTypeErasure.class);

    public static void main(String[] args) {
        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {

            final CreadorGenericoDeSource<Tuple5<String, String, String, Integer, String>>
                    randomTupleSource =
                            new CreadorGenericoDeSource<>(
                                    3, new DevolverSourceConDataFake.RandomTupleSupplier());

            final SingleOutputStreamOperator<Tuple5<String, String, String, Integer, String>>
                    batchRandomTuples =
                            env.fromSource(
                                            randomTupleSource,
                                            WatermarkStrategy.noWatermarks(),
                                            "from-tuple5")
                                    .name("batch-random-demo")
                                    .setParallelism(3);

            batchRandomTuples.print().name("print-batch-random-tuples").setParallelism(2);

            batchRandomTuples
                    .map(
                            registro -> {
                                logger.info("Data llega en batch es: " + registro);
                                return null;
                            })
                    .name("print-batch-random-tuples-custom")
                    .setParallelism(2);

            env.execute("Demo-Ejemplo-Intermedio-Error-TypeErasure");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
