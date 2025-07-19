/* (C)2025 */
package com.topaya.cdckafkaflinkiceberg.utilitarios;

import com.github.javafaker.Faker;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

public class DevolverSourceFunctionConDataFakeDeterminista
        extends RichParallelSourceFunction<RowData> {
    private volatile boolean isRunning = true;

    // Configuración para habilitar datos determinísticos
    private final boolean useDeterministicData; // Modo de datos determinísticos
    private final List<RowData> deterministicRecords; // Registros predefinidos para pruebas

    public DevolverSourceFunctionConDataFakeDeterminista(boolean useDeterministicData) {
        this.useDeterministicData = useDeterministicData;
        this.deterministicRecords = createDeterministicRecords(); // Crear conjunto determinístico
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        Faker fake = new Faker();
        int deterministicIndex = 0; // Para rastrear qué registro determinístico emitir

        while (isRunning) {
            // Generar datos determinísticos o aleatorios
            RowData dataFakeDeRowData;
            if (useDeterministicData && deterministicIndex < deterministicRecords.size()) {
                // Emitir datos determinísticos predefinidos
                dataFakeDeRowData = deterministicRecords.get(deterministicIndex);
                deterministicIndex++;
            } else {
                // Generar datos aleatorios
                dataFakeDeRowData = generateRandomData(fake);
            }

            // Emitir evento
            ctx.collect(dataFakeDeRowData);

            // Simular espera antes de enviar el siguiente registro
            Thread.sleep(20000);

            // Reiniciar el índice de los registros determinísticos, si llegamos al final
            if (deterministicIndex >= deterministicRecords.size()) {
                deterministicIndex = 0; // Para probar repetidamente los casos determinísticos
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    /**
     * Genera una lista de datos determinísticos predefinidos.
     */
    private List<RowData> createDeterministicRecords() {
        List<RowData> records = new ArrayList<>();

        // Simular algunos registros determinísticos preconfigurados
        records.add(createRow("usuario_deterministico_1", "2024-01-01T10:00:00Z"));
        records.add(createRow("usuario_deterministico_2", "2024-01-02T14:30:00Z"));
        records.add(createRow("usuario_deterministico_3", "2024-01-03T18:45:00Z"));
        records.add(createRow("usuario_deterministico_4", "2024-01-04T22:10:00Z"));

        return records;
    }

    /**
     * Genera un registro basado en datos aleatorios.
     */
    private RowData generateRandomData(Faker fake) {
        ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());
        ZonedDateTime fiveHundredYearsAgo = now.minusYears(500);
        long randomTimestamp =
                ThreadLocalRandom.current()
                        .nextLong(
                                fiveHundredYearsAgo.toInstant().toEpochMilli(),
                                now.toInstant().toEpochMilli());
        TimestampData tiempoDelEvento = TimestampData.fromEpochMillis(randomTimestamp);

        String usuario = fake.name().username();
        return createRow(usuario, tiempoDelEvento);
    }

    /**
     * Crea un registro RowData a partir de un nombre de usuario y un tiempo.
     * @param usuario Nombre del usuario (String)
     * @param timestamp Tiempo del evento en formato ISO8601 o milisegundos
     * @return Fila RowData llena
     */
    private RowData createRow(String usuario, Object timestamp) {
        GenericRowData rowData = new GenericRowData(2);
        ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());
        ZonedDateTime fiveHundredYearsAgo = now.minusYears(500);
        long randomTimestamp =
                ThreadLocalRandom.current()
                        .nextLong(
                                fiveHundredYearsAgo.toInstant().toEpochMilli(),
                                now.toInstant().toEpochMilli());
        TimestampData tiempoDelEvento = TimestampData.fromEpochMillis(randomTimestamp);
        rowData.setField(0, StringData.fromString(usuario));
        rowData.setField(1, tiempoDelEvento);

        return rowData;
    }
}
