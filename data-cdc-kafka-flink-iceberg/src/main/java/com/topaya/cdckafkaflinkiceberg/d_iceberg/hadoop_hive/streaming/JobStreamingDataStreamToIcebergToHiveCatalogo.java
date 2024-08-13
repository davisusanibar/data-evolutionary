/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.d_iceberg.hadoop_hive.streaming;

import com.topaya.cdckafkaflinkiceberg.utilitarios.DevolverSourceFunctionConDataFakeDemo;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobStreamingDataStreamToIcebergToHiveCatalogo {
    private static final Logger logger =
            LoggerFactory.getLogger(JobStreamingDataStreamToIcebergToHiveCatalogo.class);

    public static void main(String[] args) {

        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {

            env.enableCheckpointing(5000);

            final DevolverSourceFunctionConDataFakeDemo sourceFunctionConDataFake =
                    new DevolverSourceFunctionConDataFakeDemo();
            final DataStreamSource<RowData> rowDataDataStreamSource =
                    env.addSource(sourceFunctionConDataFake);

            rowDataDataStreamSource.print().name("data-stream-row-data").setParallelism(2);

            Schema esquemaIceberg =
                    new Schema(
                            Types.NestedField.optional(1, "usuario", Types.StringType.get()),
                            Types.NestedField.optional(
                                    2, "tiempo_evento", Types.TimestampType.withoutZone()));

            Map<String, String> configuracionIceberg = new HashMap<>();
            configuracionIceberg.put("type", "iceberg");
            configuracionIceberg.put("uri", "thrift://hive:9083");
            configuracionIceberg.put("catalog-type", "hive");
            configuracionIceberg.put("property-version", "1");

            Configuration configuracionHadoop = new Configuration();
            configuracionHadoop.set("fs.defaultFS", "hdfs://namenode");
            configuracionHadoop.set("dfs.client.use.datanode.hostname", "true");
            configuracionHadoop.set("dfs.datanode.use.datanode.hostname", "true");

            String catalogName = "topaya";
            String baseDatos = "default";
            String tableName = "usuarios_hive_streaming";

            CatalogLoader tipoCatalogoParaIceberg =
                    CatalogLoader.hive(catalogName, configuracionHadoop, configuracionIceberg);

            TableIdentifier identificadorTablaEnIceberg = TableIdentifier.of(baseDatos, tableName);

            if (!tipoCatalogoParaIceberg.loadCatalog().tableExists(identificadorTablaEnIceberg)) {
                tipoCatalogoParaIceberg
                        .loadCatalog()
                        .createTable(identificadorTablaEnIceberg, esquemaIceberg);
            }

            TableLoader cargarTablaDeIceberg =
                    TableLoader.fromCatalog(tipoCatalogoParaIceberg, identificadorTablaEnIceberg);

            FlinkSink.forRowData(rowDataDataStreamSource)
                    .tableLoader(cargarTablaDeIceberg)
                    .append()
                    .setParallelism(1)
                    .name("sink-iceberg-catalogo-hive-streaming");

            env.execute("Demo-Ejemplo-Iceberg-Streaming-Hive");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
