/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.utilitarios;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class UtilitarioPoblarDataKafka {
    public static final String slotName = System.getenv().getOrDefault("slotname", "kcdconf01");

    public static void escribirTipoDatoAvroAKafka(
            String topico,
            String archivo,
            Properties properties,
            String clave,
            Integer registrosInsertar) {
        try {

            Path resourcePath =
                    Paths.get(
                            UtilitarioPoblarDataKafka.class
                                    .getClassLoader()
                                    .getResource(archivo)
                                    .toURI());
            DataFileReader<GenericRecord> reader =
                    new DataFileReader<>(resourcePath.toFile(), new GenericDatumReader<>());
            try (Producer<String, GenericRecord> producer =
                    UtilitarioPoblarDataKafka.construirProductorKafka(
                            KafkaAvroSerializer.class, properties)) {
                int numeroRegistrosLeidos = 1;
                for (GenericRecord record : reader) {
                    if ((registrosInsertar != null)
                            && (numeroRegistrosLeidos > registrosInsertar)) {
                        break;
                    }
                    System.out.println("Enviando data al topico de: " + topico);
                    if (clave != null && !clave.equals("")) {
                        try {
                            System.out.println("Se configurara como clave a: " + record.get(clave));
                        } catch (Exception e) {
                            System.out.println("Se configurara como clave a: null");
                            clave = null;
                        }
                    }
                    ProducerRecord<String, GenericRecord> producerRecord =
                            new ProducerRecord<>(topico, clave, record);
                    producer.send(producerRecord);
                    numeroRegistrosLeidos++;
                }
            }
        } catch (IOException e) {
            System.out.println("error---->" + e.getMessage());
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void leerTipoDatoAvroDeKafka(String topico, Integer numRegistros) {
        try (Consumer<String, GenericRecord> consumer =
                UtilitarioPoblarDataKafka.construirConsumidorKafka(StringDeserializer.class)) {
            consumer.subscribe(Collections.singletonList(topico));
            ConsumerRecords<String, GenericRecord> records =
                    consumer.poll(Duration.ofMillis(10000));
            consumer.seekToBeginning(consumer.assignment());
            int numeroRegistrosLeidos = 1;
            for (ConsumerRecord<String, GenericRecord> record : records) {
                if (numRegistros != null && numeroRegistrosLeidos > numRegistros) break;
                System.out.println("Leyendo data de topico: " + topico);
                System.out.println(
                        "La data (" + numeroRegistrosLeidos + ") recuperada es: " + record);
                numeroRegistrosLeidos++;
            }
        }
    }

    private static <T> Producer<String, T> construirProductorKafka(
            Class<? extends Serializer> valueSerializerClass, Properties properties) {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass);

        if (valueSerializerClass == KafkaAvroSerializer.class) {
            properties.put("schema.registry.url", "http://schema-registry:8081");
        }
        return new KafkaProducer<>(properties);
    }

    private static <T> Consumer<String, T> construirConsumidorKafka(
            Class<? extends Deserializer> valueDeserializerClass) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kcdlima-read");
        if (valueDeserializerClass == KafkaAvroDeserializer.class) {
            properties.put("schema.registry.url", "http://schema-registry:8081");
        }
        return new KafkaConsumer<>(properties);
    }

    public static void eliminarTopicos(List<String> topicos) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
        String topicoEliminar = null;
        try (Admin admin = AdminClient.create(properties)) {
            for (String topico : topicos) {
                topicoEliminar = topico;
                System.out.println("Enviando eliminar topico: " + topicoEliminar);
                admin.deleteTopics(Arrays.asList(topico)).all().get();
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
