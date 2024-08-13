/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.utilitarios;

import java.util.Properties;

public class IngestaDataHaciaTopicosKafka {
    public static void main(String[] args) throws InterruptedException {
        // leerDataAvroYEscribirEnTopico("orders", 1);
        leerDataAvroYEscribirEnTopico("orders", 10);
    }

    public static void leerDataAvroYEscribirEnTopico(String topico, Integer registros)
            throws InterruptedException {
        /******** 01 - Eliminar topico ********/
        // UtilitarioKCD.eliminarTopicos(Arrays.asList(topico));
        // Thread.sleep(5000);

        /******** 02 - Crear topicos ********/
        UtilitarioPoblarDataKafka.escribirTipoDatoAvroAKafka(
                topico, "data/" + topico + ".avro", new Properties(), "o_orderkey", registros);

        /******** 03 - Leer data de topicos ********/
        UtilitarioPoblarDataKafka.leerTipoDatoAvroDeKafka("orders", registros);
    }

    public static void leerDataAvroYEscribirEnTopico(String topico) throws InterruptedException {
        leerDataAvroYEscribirEnTopico(topico, null);

        IngestaDataHaciaTopicosKafka.leerDataAvroYEscribirEnTopico(
                /*topico*/ "orders", /*cantidad*/ 1);
    }
}
