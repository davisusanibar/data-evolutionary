/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.c_flink.basico;

import com.github.javafaker.Faker;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobTransformaciones {
    private static Faker faker = new Faker();
    private static final Logger logger = LoggerFactory.getLogger(JobTransformaciones.class);

    public static void main(String[] args) {
        final List<Persona> personas = devolverListaPersonas();
        final List<String> nombres = devolverListaNombres();
        //        personas.forEach(System.out::println);
        //        nombres.forEach(System.out::println);

        try (
        //                        final StreamExecutionEnvironment env =
        //                        StreamExecutionEnvironment.createRemoteEnvironment("localhost",
        // 8081);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {

            logger.info(
                    "Runtime.getRuntime().availableProcessors(): "
                            + Runtime.getRuntime().availableProcessors());

            logger.info("Valor env.getParallelism(): " + env.getParallelism());

            final DataStreamSource<String> source =
                    env.fromData(devolverListaNombres()).setParallelism(3);

            // source.print().name("print-nombres-nativo-3-parallel").setParallelism(3);
            source.print().name("print-nombres-nativo-default");

            //            source.map(
            //                    nombre -> {
            //                        System.out.println("nombre es: " + nombre);
            //                        return nombre;
            //                    });
            //
            //            source.map(
            //                            (MapFunction<String, String>)
            //                                    s -> {
            //                                        logger.info("Imprimiendo con el Hilo: " + s);
            //                                        return s;
            //                                    })
            //                    .print()
            //                    .name("print-nombres-custom")
            //                    .setParallelism(3); // 20

            env.execute("Demo-Ejemplo-Basico-Transformaciones");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<Persona> devolverListaPersonas() {
        return Stream.generate(
                        () ->
                                new Persona(
                                        faker.name().name(), faker.number().numberBetween(18, 60)))
                .limit(100)
                .collect(Collectors.toList());
    }

    public static List<String> devolverListaNombres() {
        return Stream.generate(() -> faker.name().name()).limit(100).collect(Collectors.toList());
    }
}

class Persona {
    private String nombre;
    private int edad;

    public Persona() {}

    public Persona(String nombre, int edad) {
        this.nombre = nombre;
        this.edad = edad;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public int getEdad() {
        return edad;
    }

    public void setEdad(int edad) {
        this.edad = edad;
    }

    @Override
    public String toString() {
        return "Persona{" + "nombre='" + nombre + '\'' + ", edad=" + edad + '}';
    }
}
