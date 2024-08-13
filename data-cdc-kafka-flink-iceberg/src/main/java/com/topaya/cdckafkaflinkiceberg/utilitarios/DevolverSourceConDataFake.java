/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.utilitarios;

import com.github.javafaker.Faker;
import java.io.Serializable;
import java.util.function.Supplier;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

public class DevolverSourceConDataFake {
    public static class RandomTupleSupplier
            implements Supplier<Tuple5<String, String, String, Integer, String>>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<String, String, String, Integer, String> get() {
            Faker fake = new Faker();
            String usuario = fake.name().username();
            String nombreCompleto = fake.name().fullName();
            String direccion = fake.address().fullAddress();
            int edad = fake.number().numberBetween(18, 60);
            String foto = fake.avatar().image();
            return new Tuple5<>(usuario, nombreCompleto, direccion, edad, foto);
        }
    }

    public static class RandomRowDataSupplier implements Supplier<RowData>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public RowData get() {
            Faker fake = new Faker();
            String usuario = fake.name().username();
            String nombreCompleto = fake.name().fullName();
            int edad = fake.number().numberBetween(18, 60);
            String direccion = fake.address().fullAddress();
            String foto = fake.avatar().image();
            GenericRowData rowData = new GenericRowData(5);
            rowData.setField(0, StringData.fromString(usuario));
            rowData.setField(1, StringData.fromString(nombreCompleto));
            rowData.setField(2, edad);
            rowData.setField(3, StringData.fromString(direccion));
            rowData.setField(4, StringData.fromString(foto));
            return rowData;
        }
    }
}
