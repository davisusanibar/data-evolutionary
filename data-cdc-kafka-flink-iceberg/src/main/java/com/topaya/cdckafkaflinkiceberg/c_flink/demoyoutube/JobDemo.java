/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.c_flink.demoyoutube;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JobDemo {
    public static void main(String[] args) {

        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.getParallelism();
            System.out.println(env.getParallelism()); // serializar

            // Source: de donde viene de data: topico kafka, db, file, ....
            // Sink: a donde quiere enviar la data: topico kafka, db, file, iceberg

            final List<String> listaNombres = List.of("david", "mara", "gladis");

            final DataStreamSource<String> source = env.fromData(listaNombres);

            source.map(x -> x.toUpperCase(), TypeInformation.of(new TypeHint<String>() {}))
                    .setParallelism(3)
                    .name("task-mapeo")
                    .print();

            source.filter(x -> x.contains("mara")).print().name("task-filter").setParallelism(2);

            final DataStreamSink<String> sink =
                    source.print().name("task-impresion-nombres").setParallelism(2);

            env.execute("job-demo-01");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
