/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.d_iceberg.demoyoutube;

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

public class JobDemoStreaming {
    public static void main(String[] args) {
        try (final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {

            // flink
            final List<String> listaNombres = List.of("david", "mara", "gladis");

            final DataStreamSource<String> source = env.fromData(listaNombres);

            // iceberg

            Schema esquemaIceberg =
                    new Schema(Types.NestedField.optional(1, "nombre", Types.StringType.get()));

            Map<String, String> configuracionIceberg = new HashMap<>();
            configuracionIceberg.put("type", "iceberg");
            configuracionIceberg.put("warehouse", "hdfs://namenode/demo/hadoop/batch");
            configuracionIceberg.put("property-version", "1");

            Configuration configuracionHadoop = new Configuration();
            configuracionHadoop.set("fs.defaultFS", "hdfs://namenode");
            configuracionHadoop.set("dfs.client.use.datanode.hostname", "true");
            configuracionHadoop.set("dfs.datanode.use.datanode.hostname", "true");

            String catalogName = "demo-catalog";
            String baseDatos = "youtube-demo";
            String tableName = "nombres";

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

            FlinkSink.builderFor(
                            source,
                            new MapFunction<String, RowData>() {
                                @Override
                                public RowData map(String nombre) throws Exception {
                                    GenericRowData rowData = new GenericRowData(3);
                                    rowData.setField(0, StringData.fromString(nombre));
                                    return rowData;
                                }
                            },
                            TypeInformation.of(new TypeHint<RowData>() {}))
                    .tableLoader(cargarTablaDeIceberg)
                    .append()
                    .name("task-cargar-tabla-metadata-iceberg");

            env.execute("job-demo-iceberg-01");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
