/* (C)2025 */
package com.topaya.cdckafkaflinkiceberg.d_iceberg.minio.streaming;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.File;
import java.math.BigDecimal;
import java.util.*;

public class IcebergInsertIncrementalDemo {
    private static final String S3_ENDPOINT = "http://minio:9000";
    private static final String S3_WAREHOUSE = "s3://warehouse/catalogo/minio";
    private static final String CATALOG_URI = "http://rest:8181";
    private static final String AWS_REGION_KEY = "aws.region";
    private static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";
    private static final String AWS_SECRET_ACCESS_KEY = "aws.secretAccessKey";
    private static final String PARTITION_STATUS_NEW = "NEW";

    public static void main(String[] args) throws Exception {
        Map<String, String> catalogConfig = createCustomCatalogConfig();
        ParameterTool parameters = ParameterTool.fromArgs(args);
        configureAwsEnvironment(parameters);

        Catalog catalog = loadCatalog(catalogConfig, new Configuration()).loadCatalog();
        TableIdentifier tableId = TableIdentifier.of("demo_db_acid", "orders_iceberg_acid");

        // Asegúrate de que exista la tabla
        createTableIfNotExists(catalog, tableId);
        Table table = catalog.loadTable(tableId);

        // Crear registros simulados
        List<Record> recordsToInsert = createSampleRecords(table);

        // Realizar inserciones y actualizaciones
        insertOrUpdateRecords(table, recordsToInsert);

        System.out.println("Operaciones completadas con éxito.");
        mostrarSnapshots(table);
    }

    private static void insertOrUpdateRecords(Table table, List<Record> newRecords) throws Exception {
        // Escribir nuevos registros
        OutputFile newParquetFile = writeBatchToParquet(table, newRecords);
        DataFile newDataFile = commitParquetToTable(table, newParquetFile, newRecords.size());
        table.newAppend().appendFile(newDataFile).commit();
    }

    private static void createTableIfNotExists(Catalog catalog, TableIdentifier tableId) {
        if (catalog.tableExists(tableId)) {
            System.out.println("La tabla ya existe: " + tableId);
            return; // Evitar recrear la tabla
        }
        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "order_id", Types.LongType.get()),
                        Types.NestedField.optional(2, "status", Types.StringType.get()),
                        Types.NestedField.optional(3, "amount", Types.DecimalType.of(10, 2)));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("status").build();
        catalog.createTable(tableId, schema, spec);
        System.out.println("Tabla creada: " + tableId);
    }

    private static List<Record> createSampleRecords(Table table) {
        // Inicializamos cada registro usando el esquema de la tabla.
        Record record1 = GenericRecord.create(table.schema());
        record1.setField("order_id", 3L);
        record1.setField("status", PARTITION_STATUS_NEW);
        record1.setField("amount", BigDecimal.valueOf(77.00).setScale(2));

        Record record2 = GenericRecord.create(table.schema());
        record2.setField("order_id", 4L);
        record2.setField("status", PARTITION_STATUS_NEW);
        record2.setField("amount", BigDecimal.valueOf(288.00).setScale(2));

        return Arrays.asList(record1, record2);
    }

    private static OutputFile writeBatchToParquet(Table table, List<Record> records)
            throws Exception {
        PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
        partitionKey.partition(
                records.get(0)); // Aplicación de clave de partición al primer registro.

        OutputFile outputFile =
                table.io()
                        .newOutputFile(
                                table.location()
                                        + "/data-batch-"
                                        + System.currentTimeMillis()
                                        + ".parquet");

        // Compatibilidad con Java 11: uso de DataWriter definido explícitamente.
        DataWriter<Record> parquetWriter =
                Parquet.writeData(outputFile)
                        .schema(table.schema())
                        .withSpec(table.spec())
                        .withPartition(partitionKey)
                        .createWriterFunc(
                                org.apache.iceberg.data.parquet.GenericParquetWriter::buildWriter)
                        .build();

        try {
            for (Record record : records) {
                parquetWriter.write(record);
            }
        } finally {
            parquetWriter.close();
        }
        return outputFile;
    }

    private static DataFile commitParquetToTable(Table table, OutputFile file, int recordCount) {
        return DataFiles.builder(table.spec())
                .withPath(file.location())
                .withFormat(FileFormat.PARQUET)
                .withRecordCount(recordCount)
                .withFileSizeInBytes(getFileSize(file.location()))
                .build();
    }

    private static long getFileSize(String filePath) {
        try {
            File file = new File(new java.net.URI(filePath).getPath());
            return file.length();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error al calcular el tamaño del archivo: " + e.getMessage(), e);
        }
    }

    private static void mostrarSnapshots(Table table) {
        System.out.println("Snapshots de la tabla:");
        for (Snapshot snapshot : table.snapshots()) {
            System.out.println(
                    "- Snapshot ID: "
                            + snapshot.snapshotId()
                            + ", Timestamp: "
                            + new Date(snapshot.timestampMillis()));
        }
    }

    private static void configureAwsEnvironment(ParameterTool parameters) {
        System.setProperty(AWS_REGION_KEY, parameters.get("AWS_REGION", "us-east-1"));
        System.setProperty(AWS_ACCESS_KEY_ID, parameters.get("AWS_ACCESS_KEY_ID", "admin"));
        System.setProperty(
                AWS_SECRET_ACCESS_KEY, parameters.get("AWS_SECRET_ACCESS_KEY", "password"));
    }

    private static Map<String, String> createCustomCatalogConfig() {
        Map<String, String> catalogConfig = new HashMap<>();
        catalogConfig.put("uri", CATALOG_URI);
        catalogConfig.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogConfig.put("warehouse", S3_WAREHOUSE);
        catalogConfig.put("s3.endpoint", S3_ENDPOINT);
        return catalogConfig;
    }

    private static CatalogLoader loadCatalog(
            Map<String, String> customCatalogConfig, Configuration hadoopConfig) {
        return CatalogLoader.custom(
                "iceberg-minio-acid",
                customCatalogConfig,
                hadoopConfig,
                "org.apache.iceberg.rest.RESTCatalog");
    }
}
