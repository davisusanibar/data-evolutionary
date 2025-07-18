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
    private static final Logger LOGGER = LoggerFactory.getLogger(JobStreamingDataStreamToIcebergToMinio.class);
    private static final String S3_ENDPOINT = "http://minio:9000";
    private static final String S3_WAREHOUSE = "s3://warehouse/catalogo/minio";
    private static final String CATALOG_URI = "http://rest:8181";
    private static final String AWS_REGION_KEY = "aws.region";
    private static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";
    private static final String AWS_SECRET_ACCESS_KEY = "aws.secretAccessKey";

    public static void main(String[] args) {
        try (final StreamExecutionEnvironment executionEnvironment =
                     StreamExecutionEnvironment.getExecutionEnvironment()) {

            configureCheckpointing(executionEnvironment);
            DataStreamSource<RowData> sourceStream = configureFakeSource(executionEnvironment);

            Schema icebergSchema = createIcebergSchema();
            Map<String, String> customCatalogConfig = createCustomCatalogConfig();

            ParameterTool parameters = ParameterTool.fromArgs(args);
            configureAwsEnvironment(parameters);

            CatalogLoader catalogLoader = loadCatalog(customCatalogConfig, new Configuration());
            TableIdentifier tableIdentifier = TableIdentifier.of("icebergminio", "usuarios_minio_streaming");
            initializeIcebergTable(catalogLoader, tableIdentifier, icebergSchema);

            TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
            writeToIceberg(sourceStream, tableLoader);

            executionEnvironment.execute("Demo-Ejemplo-Iceberg-Streaming-Minio");
        } catch (Exception exception) {
            LOGGER.error(exception.getMessage(), exception);
            throw new RuntimeException(exception);
        }
    }

    private static void configureCheckpointing(StreamExecutionEnvironment environment) {
        environment.enableCheckpointing(5000);
    }

    private static DataStreamSource<RowData> configureFakeSource(StreamExecutionEnvironment environment) {
        DevolverSourceFunctionConDataFakeDemo sourceFunction = new DevolverSourceFunctionConDataFakeDemo();
        DataStreamSource<RowData> sourceStream = environment.addSource(sourceFunction);
        sourceStream.print().name("data-stream-row-data").setParallelism(2);
        return sourceStream;
    }

    private static Schema createIcebergSchema() {
        return new Schema(
                Types.NestedField.optional(1, "usuario", Types.StringType.get()),
                Types.NestedField.optional(2, "tiempo_evento", Types.TimestampType.withoutZone())
        );
    }

    private static Map<String, String> createCustomCatalogConfig() {
        Map<String, String> catalogConfig = new HashMap<>();
        catalogConfig.put("uri", CATALOG_URI);
        catalogConfig.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogConfig.put("warehouse", S3_WAREHOUSE);
        catalogConfig.put("s3.endpoint", S3_ENDPOINT);
        return catalogConfig;
    }

    private static void configureAwsEnvironment(ParameterTool parameters) {
        System.setProperty(AWS_REGION_KEY, parameters.get("AWS_REGION", "us-east-1"));
        System.setProperty(AWS_ACCESS_KEY_ID, parameters.get("AWS_ACCESS_KEY_ID", "admin"));
        System.setProperty(AWS_SECRET_ACCESS_KEY, parameters.get("AWS_SECRET_ACCESS_KEY", "password"));
    }

    private static CatalogLoader loadCatalog(Map<String, String> customCatalogConfig, Configuration hadoopConfig) {
        return CatalogLoader.custom(
                "flink-iceberg-minio",
                customCatalogConfig,
                hadoopConfig,
                "org.apache.iceberg.rest.RESTCatalog"
        );
    }

    private static void initializeIcebergTable(CatalogLoader catalogLoader, TableIdentifier tableIdentifier, Schema schema) {
        if (!catalogLoader.loadCatalog().tableExists(tableIdentifier)) {
            catalogLoader.loadCatalog().createTable(tableIdentifier, schema);
        }
    }

    private static void writeToIceberg(DataStreamSource<RowData> sourceStream, TableLoader tableLoader) {
        FlinkSink.forRowData(sourceStream)
                .tableLoader(tableLoader)
                .append()
                .setParallelism(1)
                .name("sink-iceberg-catalogo-minio-streaming");
    }
}