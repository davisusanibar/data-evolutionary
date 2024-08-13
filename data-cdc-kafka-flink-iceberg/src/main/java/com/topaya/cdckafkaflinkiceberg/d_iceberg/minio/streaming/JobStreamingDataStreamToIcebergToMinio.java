/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.d_iceberg.minio.streaming;

import com.topaya.cdckafkaflinkiceberg.utilitarios.DevolverSourceFunctionConDataFakeDemo;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.utils.ParameterTool;
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

public class JobStreamingDataStreamToIcebergToMinio {
    private static final Logger logger =
            LoggerFactory.getLogger(JobStreamingDataStreamToIcebergToMinio.class);

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

            Configuration configuracionHadoop = new Configuration();
            configuracionHadoop.set("fs.defaultFS", "hdfs://namenode");
            configuracionHadoop.set("dfs.client.use.datanode.hostname", "true");
            configuracionHadoop.set("dfs.datanode.use.datanode.hostname", "true");

            Map<String, String> configuracionCustomCatalogo = new HashMap<>();
            configuracionCustomCatalogo.put("uri", "http://rest:8181");
            configuracionCustomCatalogo.put(
                    "io-impl",
                    "org.apache.iceberg.aws.s3.S3FileIO"); // CatalogUtil - Loading custom FileIO
            // implementation:
            // org.apache.iceberg.aws.s3.S3FileIO

            configuracionCustomCatalogo.put("warehouse", "s3://warehouse/catalogo/minio");
            configuracionCustomCatalogo.put("s3.endpoint", "http://minio:9000");

            String baseDatos = "icebergminio";
            String tableName = "usuarios_minio_streaming";

            ParameterTool parameters = ParameterTool.fromArgs(args);
            System.setProperty(
                    "aws.region",
                    parameters.get(
                            "AWS_REGION",
                            "us-east-1")); // Unable to load region from system settings. Region
            // must be specified either via environment variable
            // (AWS_REGION) or system property (aws.region)
            System.setProperty(
                    "aws.accessKeyId",
                    parameters.get(
                            "AWS_ACCESS_KEY_ID",
                            "admin")); // Unable to load credentials from any of the providers in
            // the chain
            // AwsCredentialsProviderChain(credentialsProviders=
            System.setProperty(
                    "aws.secretAccessKey",
                    parameters.get(
                            "AWS_SECRET_ACCESS_KEY",
                            "password")); // Unable to load credentials from any of the providers in
            // the chain
            // AwsCredentialsProviderChain(credentialsProviders=

            CatalogLoader tipoCatalogoParaIceberg =
                    CatalogLoader.custom(
                            "flink-iceberg-minio",
                            configuracionCustomCatalogo,
                            configuracionHadoop,
                            "org.apache.iceberg.rest.RESTCatalog");

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
                    .name("sink-iceberg-catalogo-minio-streaming");

            env.execute("Demo-Ejemplo-Iceberg-Streaming-Minio");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
