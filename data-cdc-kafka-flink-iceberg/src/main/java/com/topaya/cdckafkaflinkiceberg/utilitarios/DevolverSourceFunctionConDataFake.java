/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.utilitarios;

import com.github.javafaker.Faker;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/**
 * FIXME! Se debe usar {@link org.apache.flink.api.connector.source.Source}, tal como lo ha
 * implementado {@link com.topaya.cdckafkaflinkiceberg.utilitarios.CreadorGenericoDeSource}
 */
public class DevolverSourceFunctionConDataFake extends RichParallelSourceFunction<RowData> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        Faker fake = new Faker();
        while (isRunning) {
            ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());
            ZonedDateTime fiveHundredYearsAgo = now.minusYears(500);
            long randomTimestamp =
                    ThreadLocalRandom.current()
                            .nextLong(
                                    fiveHundredYearsAgo.toInstant().toEpochMilli(),
                                    now.toInstant().toEpochMilli());
            TimestampData tiempoDelEvento = TimestampData.fromEpochMillis(randomTimestamp);
            String usuario = fake.name().username();
            String nombreCompleto = fake.name().fullName();
            int edad = fake.number().numberBetween(18, 60);
            String direccion = fake.address().fullAddress();
            String foto = fake.avatar().image();
            GenericRowData dataFakeDeRowData = new GenericRowData(6);
            dataFakeDeRowData.setField(0, StringData.fromString(usuario));
            dataFakeDeRowData.setField(1, StringData.fromString(nombreCompleto));
            dataFakeDeRowData.setField(2, edad);
            dataFakeDeRowData.setField(3, StringData.fromString(direccion));
            dataFakeDeRowData.setField(4, StringData.fromString(foto));
            dataFakeDeRowData.setField(5, tiempoDelEvento);

            Thread.sleep(20000);

            // emitir evento fake
            ctx.collect(dataFakeDeRowData);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
