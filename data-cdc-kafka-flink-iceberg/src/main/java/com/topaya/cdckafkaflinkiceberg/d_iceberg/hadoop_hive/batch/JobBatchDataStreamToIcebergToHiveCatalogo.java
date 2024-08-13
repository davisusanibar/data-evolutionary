/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.d_iceberg.hadoop_hive.batch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobBatchDataStreamToIcebergToHiveCatalogo {
    private static final Logger logger =
            LoggerFactory.getLogger(JobBatchDataStreamToIcebergToHiveCatalogo.class);

    public static void main(String[] args) {

        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {

            final DataStreamSource<String> nombres =
                    env.fromData(List.of("joe", "days", "david", "gladis", "mara", "stelle"));

            Schema esquemaIceberg =
                    new Schema(Types.NestedField.optional(1, "nombre", Types.StringType.get()));

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
            String baseDatos =
                    "default"; // Si no se elige default se observa el siguiente error: Namespace
            // does not exist
            String tableName = "nombres_hive_batch";

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

            FlinkSink.builderFor(
                            nombres,
                            (MapFunction<String, RowData>)
                                    nombre -> {
                                        logger.info("Insertando Nombre: " + nombre);
                                        GenericRowData genericRowData = new GenericRowData(1);
                                        genericRowData.setField(0, StringData.fromString(nombre));
                                        return genericRowData;
                                    },
                            TypeInformation.of(new TypeHint<>() {}))
                    .tableLoader(cargarTablaDeIceberg)
                    .append()
                    .setParallelism(3)
                    .name("sink-iceberg-catalogo-hive");

            env.execute("Demo-Ejemplo-Iceberg-Batch-Hive");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
