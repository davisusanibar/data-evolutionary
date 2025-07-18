package com.topaya.cdckafkaflinkiceberg.d_iceberg.minio.streaming;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.CatalogLoader;

import java.util.HashMap;
import java.util.Map;

public class IcebergPartitionUpdate {

    private static final String S3_ENDPOINT = "http://minio:9000";
    private static final String S3_WAREHOUSE = "s3://warehouse/catalogo/minio";
    private static final String CATALOG_URI = "http://rest:8181";
    private static final String AWS_REGION_KEY = "aws.region";
    private static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";
    private static final String AWS_SECRET_ACCESS_KEY = "aws.secretAccessKey";

    public static void main(String[] args) {
        Map<String, String> customCatalogConfig = createCustomCatalogConfig();

        ParameterTool parameters = ParameterTool.fromArgs(args);
        configureAwsEnvironment(parameters);

        CatalogLoader catalogLoader = loadCatalog(customCatalogConfig, new Configuration());

        updateTableToUsePartitions(catalogLoader.loadCatalog());
    }

    public static void updateTableToUsePartitions(Catalog catalog) {
        // Cargar la tabla
        Table table = catalog.loadTable(TableIdentifier.of("icebergminio", "usuarios_minio_streaming"));

        // Verificar si los campos de partición ya están definidos
        PartitionSpec currentSpec = table.spec();

        if (!hasPartitionField(currentSpec, "year", "tiempo_evento")) {
            table.updateSpec()
                    .addField(Expressions.year("tiempo_evento"))
                    .commit();
            System.out.println("Partición 'year' añadida.");
        } else {
            System.out.println("La partición 'year' ya existe.");
        }

        if (!hasPartitionField(currentSpec, "month", "tiempo_evento")) {
            table.updateSpec()
                    .addField(Expressions.month("tiempo_evento"))
                    .commit();
            System.out.println("Partición 'month' añadida.");
        } else {
            System.out.println("La partición 'month' ya existe.");
        }
    }

    private static boolean hasPartitionField(PartitionSpec spec, String transform, String sourceName) {
        for (PartitionField field : spec.fields()) {
            // Compara la transformación y el nombre de la fuente
            if (field.transform().toString().equalsIgnoreCase(transform)
                    && spec.schema().findField(field.sourceId()).name().equalsIgnoreCase(sourceName)) {
                return true;
            }
        }
        return false;
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
}