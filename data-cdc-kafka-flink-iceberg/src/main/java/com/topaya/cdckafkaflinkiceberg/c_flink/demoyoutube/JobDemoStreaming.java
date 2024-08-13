/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.c_flink.demoyoutube;

import com.topaya.cdckafkaflinkiceberg.utilitarios.DevolverSourceFunctionConDataFake;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

public class JobDemoStreaming {
    public static void main(String[] args) {
        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {

            final DevolverSourceFunctionConDataFake devolverSourceFunctionConDataFake =
                    new DevolverSourceFunctionConDataFake();

            final DataStreamSource<RowData> dataSource =
                    env.addSource(devolverSourceFunctionConDataFake);

            // map, filter, > MapFunction

            final DataStreamSink<RowData> sink = dataSource.print();

            env.execute("job-demo-02");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
