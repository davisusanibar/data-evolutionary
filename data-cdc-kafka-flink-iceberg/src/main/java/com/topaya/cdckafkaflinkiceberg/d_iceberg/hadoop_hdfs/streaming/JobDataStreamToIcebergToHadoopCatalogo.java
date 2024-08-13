/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.d_iceberg.hadoop_hdfs.streaming;

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

public class JobDataStreamToIcebergToHadoopCatalogo {
    private static final Logger logger =
            LoggerFactory.getLogger(JobDataStreamToIcebergToHadoopCatalogo.class);

    public static void main(String[] args) {

        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {

            final DevolverSourceFunctionConDataFakeDemo sourceFunctionConDataFake =
                    new DevolverSourceFunctionConDataFakeDemo();
            final DataStreamSource<RowData> rowDataDataStreamSource =
                    env.addSource(sourceFunctionConDataFake);

            env.enableCheckpointing(5000);

            rowDataDataStreamSource.print().setParallelism(3).name("print-rowdata-streaming");

            Schema esquemaIceberg =
                    new Schema(Types.NestedField.optional(1, "usuario", Types.StringType.get()));

            Map<String, String> configuracionIceberg = new HashMap<>();
            configuracionIceberg.put("type", "iceberg");
            configuracionIceberg.put("warehouse", "hdfs://namenode/catalogo/hadoop/streaming");
            configuracionIceberg.put("property-version", "1");

            Configuration configuracionHadoop = new Configuration();
            configuracionHadoop.set("fs.defaultFS", "hdfs://namenode");
            configuracionHadoop.set("dfs.client.use.datanode.hostname", "true");
            configuracionHadoop.set("dfs.datanode.use.datanode.hostname", "true");

            String catalogName = "topaya";
            String baseDatos = "db";
            String tableName = "usuario_hadoop_streaming";

            CatalogLoader tipoCatalogoParaIceberg =
                    CatalogLoader.hadoop(catalogName, configuracionHadoop, configuracionIceberg);

            TableIdentifier identificadorTablaEnIceberg =
                    TableIdentifier.of(catalogName, baseDatos, tableName);

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
                    .name("sink-streaming-iceberg-catalogo-hdfs");

            env.execute("Demo-Ejemplo-Iceberg-Batch-Hadoop");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
