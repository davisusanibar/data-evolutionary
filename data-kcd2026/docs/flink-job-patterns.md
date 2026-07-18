# Patrones de implementación para jobs Flink en data-kcd2026

Implementación de [ADR-0002](../governance/ADR-0002-patrones-de-implementacion-jobs-flink.md).
Todo extracto de código de este documento es real, tomado de la feature 001
(`specs/001-revenue-ventana-cliente/`), no una descripción hipotética. Si una
feature futura se aparta de un patrón, decláralo y justifícalo en la sección
"Decisiones de diseño" o "Constitution Check" de su propio `plan.md` — este
documento fija la convención por defecto, no una regla que no admita
excepción razonada.

## 1. Contrato antes que código

Los tipos de entrada/salida se declaran como `.avsc` versionados en
`src/main/resources/model/`, y `avro-maven-plugin` genera las clases Java en
la fase `generate-sources` (ver `pom.xml`, y [ADR-0001](../governance/ADR-0001-empaquetado-jar-plano-maven-shade.md)
para el empaquetado). El job nunca declara una estructura ad-hoc paralela al
contrato — usa las clases generadas directamente.

Cuidado real encontrado en la feature 001: Avro genera `CharSequence` para
campos `string` y `java.time.Instant` para campos con `logicalType:
timestamp-millis`, no `String`/`long`. Revisa la clase generada
(`target/generated-sources/avro/...`) antes de asumir el tipo.

## 2. Transformación pura separada de la I/O

La lógica de negocio vive en un método estático que recibe y devuelve
`DataStream<T>`, sin conocer Kafka ni el Schema Registry. `main()` ensambla
fuente real + transformación + sink real; el test ensambla una fuente de
prueba + la misma transformación + collect:

```java
// RevenueVentanaClienteJob.java
public static DataStream<RevenueVentana> agregarRevenuePorVentana(DataStream<Orden> ordenes) {
    return ordenes
            .assignTimestampsAndWatermarks(/* ... */)
            .keyBy(orden -> orden.getClienteId().toString())
            .window(TumblingEventTimeWindows.of(Time.minutes(TAMANO_VENTANA_MINUTOS)))
            .allowedLateness(Time.seconds(TOLERANCIA_TARDIOS_SEGUNDOS))
            .aggregate(new RevenueAggregateFunction(), new RevenueWindowFunction());
}
```

`main()` llama a este método con un `DataStream` que viene de `KafkaSource`;
el test (patrón 5) le pasa un `DataStream` que viene de una fuente de
prueba. La lógica que se prueba es exactamente la que corre en producción —
no una reimplementación paralela del cálculo.

## 3. `AggregateFunction` + `ProcessWindowFunction` combinados

Nunca uno solo de los dos. `AggregateFunction` agrega de forma incremental
(no materializa la ventana completa en estado); `ProcessWindowFunction`
aporta la metadata de la ventana (inicio/fin) que un `AggregateFunction`
solo no puede emitir:

```java
private static class RevenueWindowFunction
        extends ProcessWindowFunction<RevenueAcumulador, RevenueVentana, String, TimeWindow> {
    @Override
    public void process(String clienteId, Context context,
            Iterable<RevenueAcumulador> acumuladores, Collector<RevenueVentana> out) {
        RevenueAcumulador acumulador = acumuladores.iterator().next();
        out.collect(RevenueVentana.newBuilder()
                .setClienteId(clienteId)
                .setInicioVentana(Instant.ofEpochMilli(context.window().getStart()))
                .setFinVentana(Instant.ofEpochMilli(context.window().getEnd()))
                .setRevenueAcumulado(acumulador.revenueAcumulado)
                .setNumeroOrdenes(acumulador.numeroOrdenes)
                .build());
    }
}
```

## 4. Watermark y tolerancia a tardíos como decisiones separadas

`WatermarkStrategy.forMonotonousTimestamps()` para el watermark;
`.allowedLateness(Time.seconds(N))` en la propia ventana para la tolerancia
a eventos tardíos. **No** usar `forBoundedOutOfOrderness` para la
tolerancia — la feature 001 empezó así y se corrigió durante el
`/speckit.implement` (ver `plan.md`): mezclar ambos mecanismos duplica la
tolerancia real y complica el razonamiento sobre cuándo cierra una ventana.

```java
.assignTimestampsAndWatermarks(
        WatermarkStrategy.<Orden>forMonotonousTimestamps()
                .withTimestampAssigner((orden, ts) -> orden.getTimestampEvento().toEpochMilli()))
/* ... */
.allowedLateness(Time.seconds(TOLERANCIA_TARDIOS_SEGUNDOS))
```

## 5. Test determinista de ventanas de event-time con fuente punctuated

Un `SourceFunction` de prueba que llama `ctx.collectWithTimestamp(elemento,
timestamp)` dentro de `ctx.getCheckpointLock()`, combinado con
`env.getConfig().setAutoWatermarkInterval(N)` bajo (10ms en la feature 001)
y una pausa corta (`Thread.sleep`) entre fases del fixture, para darle
tiempo al generador de watermarks periódico de avanzar entre cada fase:

```java
private static final class OrdenesFixtureSource implements SourceFunction<Orden> {
    private final List<Orden> ordenes;
    @Override
    public void run(SourceContext<Orden> ctx) throws Exception {
        for (Orden orden : ordenes) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collectWithTimestamp(orden, orden.getTimestampEvento().toEpochMilli());
            }
            Thread.sleep(50L);
        }
    }
    @Override public void cancel() { /* ... */ }
}
```

Evita depender del arnés interno de operadores
(`WindowOperatorBuilder`/`KeyedOneInputStreamOperatorTestHarness`) para el
caso común de probar una ventana con tolerancia a tardíos. Si ese nivel de
control no alcanza (por ejemplo, para verificar el estado interno del
operador entre fases), el arnés interno sigue siendo la alternativa correcta
— ver el gate script y la evaluación descartada en ADR-0002.

## 6. Empaquetado

Ver [ADR-0001](../governance/ADR-0001-empaquetado-jar-plano-maven-shade.md):
`maven-shade-plugin`, Flink en scope `provided`, conectores en `compile`.

## 7. Convención de paquete

Un subpaquete por feature bajo `com.topaya.kcd2026.<slug-de-la-feature>` —
aquí, `com.topaya.kcd2026.revenue`. No mezclar clases de features distintas
en el mismo paquete.

## Cómo citar este documento

Una spec futura que agregue un job Flink puede listar este archivo en su
`fuentes:` (frontmatter de `spec.md`) como referencia de convención de
implementación.
