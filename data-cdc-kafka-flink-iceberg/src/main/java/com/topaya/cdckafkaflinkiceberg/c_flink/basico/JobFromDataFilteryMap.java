/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.c_flink.basico;

import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobFromDataFilteryMap {
    private static final Logger logger = LoggerFactory.getLogger(JobFromDataFilteryMap.class);

    public static void main(String[] args) {

        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {

            final DataStreamSource<String> nombres =
                    env.fromData(List.of("joe", "days", "david", "gladis", "mara", "stelle"));

            nombres.print().name("print-nombres").setParallelism(2);

            nombres.filter(x -> x.startsWith("joe"))
                    .print()
                    .setParallelism(1)
                    .name("filter-nombre");

            nombres.map(name -> name + "-agregado")
                    .print()
                    .name("print-nombres-agregado")
                    .setParallelism(3);

            env.execute("Demo-Ejemplo-Basico");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
