/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.c_flink.intermedio.streaming;

import com.topaya.cdckafkaflinkiceberg.utilitarios.CreadorGenericoDeSource;
import com.topaya.cdckafkaflinkiceberg.utilitarios.DevolverSourceConDataFake;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobStreamingInputStatusEND_OF_INPUT {
    private static final Logger logger =
            LoggerFactory.getLogger(JobStreamingInputStatusEND_OF_INPUT.class);

    public static void main(String[] args) {
        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.enableCheckpointing(5000);

            final CreadorGenericoDeSource<Tuple5<String, String, String, Integer, String>>
                    randomTupleSource =
                            new CreadorGenericoDeSource<>(
                                    3, new DevolverSourceConDataFake.RandomTupleSupplier());

            final SingleOutputStreamOperator<Tuple5<String, String, String, Integer, String>>
                    batchRandomTuples =
                            env.fromSource(
                                            randomTupleSource,
                                            WatermarkStrategy.noWatermarks(),
                                            "from-tuple5",
                                            TypeInformation.of(
                                                    new TypeHint<
                                                            Tuple5<
                                                                    String,
                                                                    String,
                                                                    String,
                                                                    Integer,
                                                                    String>>() {}))
                                    .name("batch-random-demo")
                                    .setParallelism(3);

            final SingleOutputStreamOperator<Tuple5<String, String, String, Integer, String>>
                    streamingRandomTuples =
                            env.fromSource(
                                            randomTupleSource,
                                            WatermarkStrategy.noWatermarks(),
                                            "from-tuple5",
                                            TypeInformation.of(
                                                    new TypeHint<
                                                            Tuple5<
                                                                    String,
                                                                    String,
                                                                    String,
                                                                    Integer,
                                                                    String>>() {}))
                                    .name("streaming-random-demo")
                                    .setParallelism(3);

            streamingRandomTuples.print().name("print-streaming-random-tuples").setParallelism(2);

            streamingRandomTuples
                    .map(
                            registro -> {
                                logger.info("Data llega en streaming es: " + registro);
                                return null;
                            })
                    .name("print-streaming-random-tuples-custom")
                    .setParallelism(2);

            env.execute("Demo-Ejemplo-Intermedio-Streaming-END_OF_INPUT");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
