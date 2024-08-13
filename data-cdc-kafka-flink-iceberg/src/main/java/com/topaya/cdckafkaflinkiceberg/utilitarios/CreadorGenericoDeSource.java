/* (C)2024 */
package com.topaya.cdckafkaflinkiceberg.utilitarios;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import scala.Int;

public class CreadorGenericoDeSource<T> implements Source<T, NoSplitGenerico, Void> {

    private int numeroCiclosAGenerar = 0;
    private final Supplier<T> dataGenerator;

    public CreadorGenericoDeSource(int numeroCiclosAGenerar, Supplier<T> dataGenerator) {
        this.numeroCiclosAGenerar = numeroCiclosAGenerar;
        this.dataGenerator = dataGenerator;
    }

    public CreadorGenericoDeSource(Supplier<T> dataGenerator) {
        this.dataGenerator = dataGenerator;
        this.numeroCiclosAGenerar = Int.MaxValue();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, NoSplitGenerico> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new MiFuenteDeDatosGenerico<>(this.numeroCiclosAGenerar, this.dataGenerator);
    }

    // Rest of the implementation.
    // There's no need to change these unless your other serializer/checkpoint needs to know T

    @Override
    public SplitEnumerator<NoSplitGenerico, Void> createEnumerator(
            SplitEnumeratorContext<NoSplitGenerico> enumContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<NoSplitGenerico, Void> restoreEnumerator(
            SplitEnumeratorContext<NoSplitGenerico> enumContext, Void checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<NoSplitGenerico> getSplitSerializer() {
        return new NoSplitSerializerGenerico();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return null;
    }
}

class NoSplitGenerico implements SourceSplit {
    @Override
    public String splitId() {
        return "NO_SPLIT";
    }
}

class MiFuenteDeDatosGenerico<T> implements SourceReader<T, NoSplitGenerico> {
    private int numeroCiclosAGenerar = 0;
    private AtomicInteger contadorCiclosGenerados = new AtomicInteger(0);
    private final Supplier<T> dataGenerator;

    public MiFuenteDeDatosGenerico(int numeroCiclosAGenerar, Supplier<T> dataGenerator) {
        this.numeroCiclosAGenerar = numeroCiclosAGenerar;
        this.dataGenerator = dataGenerator;
    }

    public MiFuenteDeDatosGenerico(Supplier<T> dataGenerator) {
        this.dataGenerator = dataGenerator;
        this.numeroCiclosAGenerar = Int.MaxValue();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        final int actual = contadorCiclosGenerados.incrementAndGet();
        System.out.println("Ciclo actual de repeticion: " + actual);
        System.out.println("Ciclos maximos permitidos: " + numeroCiclosAGenerar);
        if (actual > numeroCiclosAGenerar) {
            return InputStatus.END_OF_INPUT;
        }
        T generatedData = dataGenerator.get();
        output.collect(generatedData);
        // agrega un tiempo de espera, aquí por ejemplo hemos configurado 10 segundos de espera.
        Thread.sleep(20000);
        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public void start() {}

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        SourceReader.super.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void addSplits(List<NoSplitGenerico> splits) {}

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        // Simula la espera de disponibilidad de los datos
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
    }

    @Override
    public List<NoSplitGenerico> snapshotState(long checkpointId) {
        return List.of();
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        SourceReader.super.pauseOrResumeSplits(splitsToPause, splitsToResume);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        SourceReader.super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        SourceReader.super.handleSourceEvents(sourceEvent);
    }

    @Override
    public void close() throws Exception {}
}

class NoSplitSerializerGenerico implements SimpleVersionedSerializer<NoSplitGenerico> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(NoSplitGenerico obj) throws IOException {
        return new byte[0];
    }

    @Override
    public NoSplitGenerico deserialize(int version, byte[] serialized) throws IOException {
        return null;
    }
}
