# Caso de Uso

- Apache Kafka lectura de topicos con Schema registry
- Apache Flink CDC Postgres
- Apache Flink CDC Postgres, Apache Flink Join, Apache Flink Sum, Apache Flink Count, Apache Kafka
- Apache FLink y Apache Iceberg
- Apache Iceberg y Catalogo HDFS
- Apache Iceberg y Catalogo Hive

# Construir Proyecto

## Descargar proyecto
```bash
$ git clone https://github.com/davisusanibar/data-evolutionary.git
```

## Generar Modelos Avro
```bash
$ cd data-evolutionary/data-cdc-kafka-flink-iceberg
$ mvn clean compime
$ ls -1 target/generated-sources/avro/com/topaya/cdckafkaflinkiceberg/model/avro
|_ Customer.java
|_ CustomerTotalCount.java
|_ CustomerTotalPrice.java
|_ OrderEnrichment.java
|_ Orders.java
|_ o_totalprice.java
```

## Linter Codigo
```bash
$ cd data-evolutionary/data-cdc-kafka-flink-iceberg
$ mvn spotless:check
$ mvn spotless:apply
...
[INFO] Spotless.Java is keeping 24 files clean - 2 were changed to be clean, 22 were already clean, 0 were skipped because caching determined they were already clean
...
```

## Crear paquete de despliegue para cargar a Flink
```bash
$ cd data-evolutionary/data-cdc-kafka-flink-iceberg
$ mvn clean package
$ ls -1 target/data-cdckafkaflinkiceberg-1.0-SNAPSHOT-shaded.jar
|_ target/data-cdckafkaflinkiceberg-1.0-SNAPSHOT-shaded.jar
```

