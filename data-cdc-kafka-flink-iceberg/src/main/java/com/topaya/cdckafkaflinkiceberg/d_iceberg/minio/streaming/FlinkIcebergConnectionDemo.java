package com.topaya.cdckafkaflinkiceberg.d_iceberg.minio.streaming;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkIcebergConnectionDemo {
    private static final String AWS_REGION_KEY = "aws.region";
    private static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";
    private static final String AWS_SECRET_ACCESS_KEY = "aws.secretAccessKey";

    public static void main(String[] args) {

        // Crear el entorno de ejecución de Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameters = ParameterTool.fromArgs(args);
        configureAwsEnvironment(parameters);

        // Configurar TableEnvironment con las características de Streaming
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Agregar configuración del catálogo de Iceberg
        tEnv.executeSql(
                "CREATE CATALOG iceberg_minio WITH (" +
                        "  'type' = 'iceberg', " +
                        "  'catalog-type' = 'rest', " +
                        "  'uri' = 'http://rest:8181', " +
                        "  'warehouse' = 's3://warehouse/catalogo/minio', " +
                        "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO', " +
                        "  's3.endpoint' = 'http://minio:9000', " +
                        "  's3.path-style-access' = 'true'" +
                        ")"
        );

        // Usar el catálogo recién creado
        tEnv.executeSql("USE CATALOG iceberg_minio");

        // Consultar bases de datos existentes en el catálogo
        tEnv.executeSql("SHOW DATABASES").print();

        // Ejemplo: Usando una base de datos y mostrando las tablas
        tEnv.executeSql("USE demo_db_acid");
        tEnv.executeSql("SHOW TABLES").print();

        // Consulta a una tabla específica
        tEnv.executeSql("SELECT * FROM orders_iceberg_acid").print();
    }

    private static void configureAwsEnvironment(ParameterTool parameters) {
        System.setProperty(AWS_REGION_KEY, parameters.get("AWS_REGION", "us-east-1"));
        System.setProperty(AWS_ACCESS_KEY_ID, parameters.get("AWS_ACCESS_KEY_ID", "admin"));
        System.setProperty(AWS_SECRET_ACCESS_KEY, parameters.get("AWS_SECRET_ACCESS_KEY", "password"));
    }
}