# ADR-0002 — Patrones de implementación para jobs Flink en data-kcd2026

**Estado:** Aceptado
**Fecha:** 2026-07-18
**Owner:** romaria85
**Relaciona:** ADR-0001, specs/001-revenue-ventana-cliente

## Contexto

La feature 001 (`revenue por cliente en ventana`) es el primer job Flink de
`data-kcd2026`. Su implementación (`RevenueVentanaClienteJob.java`,
`RevenueVentanaClienteJobTest.java`, `orden.avsc`, `revenue-ventana.avsc`)
fijó, sin proponérselo como decisión explícita en su momento, un conjunto de
patrones técnicos que resolvieron problemas reales encontrados durante el
`/speckit.implement` (deserialización Avro, tipos generados, testeo
determinista de ventanas de event-time). Hoy esos patrones solo existen
implícitos en el código de una feature; nada obliga ni documenta que la
feature 002 (o cualquier feature futura del módulo) los repita, y cada nueva
feature corre el riesgo de redescubrir por su cuenta los mismos problemas
(p. ej. que Avro genera `CharSequence`/`Instant` en vez de `String`/`long`, o
que un test de ventana de event-time necesita un generador de watermark
punctuated para ser determinista).

Este ADR es distinto de ADR-0001 en diseño: ADR-0001 fijó **una** decisión de
build (empaquetado). Este ADR **cataloga y adopta como convención** un
conjunto de patrones de código y de prueba, para que se apliquen de forma
consistente en toda feature futura del módulo — es una decisión de
arquitectura de código, no de build.

## Decisión

Se adoptan como patrón estándar para jobs Flink en `data-kcd2026`, verificado
con la evidencia real de la feature 001:

1. **Contrato antes que código.** Los tipos de entrada/salida se declaran
   como `.avsc` versionados en `src/main/resources/model/`, y las clases Java
   se generan en `generate-sources` (`avro-maven-plugin`). El código del job
   nunca declara una estructura ad-hoc paralela al contrato.
2. **Transformación pura separada de la I/O.** La lógica de negocio vive en
   un método estático que recibe y devuelve `DataStream<T>`
   (`agregarRevenuePorVentana(DataStream<Orden>): DataStream<RevenueVentana>`),
   sin conocer Kafka ni el Schema Registry. `main()` ensambla fuente real +
   transformación + sink real; el test ensambla una fuente de prueba + la
   misma transformación + collect. La lógica que se prueba es exactamente la
   que corre en producción.
3. **`AggregateFunction` + `ProcessWindowFunction` combinados**, nunca solo
   uno de los dos, para agregar de forma incremental (sin materializar la
   ventana completa en estado) y aun así poder emitir metadata de la ventana
   (inicio/fin) en el resultado.
4. **Watermark y tolerancia a tardíos como decisiones separadas.**
   `WatermarkStrategy.forMonotonousTimestamps()` para el watermark;
   `.allowedLateness(Time.seconds(N))` en la propia ventana para la
   tolerancia. No se usa `forBoundedOutOfOrderness` para tolerancia — mezclar
   ambos mecanismos duplica la tolerancia real y complica el razonamiento
   (lección de la corrección hecha en la feature 001, ver `plan.md`).
5. **Test determinista de ventanas de event-time con fuente punctuated.**
   Un `SourceFunction` de prueba que llama
   `ctx.collectWithTimestamp(elemento, timestamp)` y controla el avance del
   watermark periódico con `env.getConfig().setAutoWatermarkInterval(N)` +
   pausas cortas (`Thread.sleep`) entre fases del fixture. Evita depender del
   arnés interno de operadores (`WindowOperatorBuilder`,
   `KeyedOneInputStreamOperatorTestHarness`) para el caso común.
6. **Empaquetado**: ADR-0001 (shade plugin, Flink `provided`, conectores
   `compile`) se reconfirma como parte de este catálogo, no se repite aquí.
7. **Convención de paquete**: un subpaquete por feature bajo
   `com.topaya.kcd2026.<slug-de-la-feature>` (aquí, `revenue`), sin mezclar
   clases de distintas features en el mismo paquete.

Los patrones 1–5 y 7 se documentan en un artefacto de referencia nuevo,
`data-kcd2026/docs/flink-job-patterns.md`, con extractos reales de código de
la feature 001 como ejemplo — no descripciones abstractas. Ese documento es
la **implementación** de este ADR: sin él, la decisión queda solo en este
archivo y no es descubrible por quien escriba la feature 002.

## Alternativas consideradas

- **No documentar nada; confiar en que se lea el código de la feature 001
  como referencia.** Rechazada: el código no explica el *porqué* (p. ej. por
  qué `forMonotonousTimestamps` y no `forBoundedOutOfOrderness`), y una
  feature futura puede copiar el síntoma sin entender la causa.
- **Encapsular los patrones en una librería compartida** (p. ej. una clase
  base `AbstractVentanaJob` o utilidades comunes en un paquete `common`).
  Rechazada por ahora: con una sola feature implementada, extraer una
  abstracción compartida es prematuro (arriesga una mala abstracción); se
  revisita si la feature 002 confirma que el patrón se repite igual.
- **Un checklist en el gate SDD** (`spec_kit_gate.py`) que verifique
  mecánicamente estos patrones. Rechazada por ahora: el gate valida
  metadata/contrato SDD, no AST de código Java; añadir esa verificación es un
  cambio de herramienta más grande, fuera de alcance de este ADR.

## Consecuencias

- Toda feature nueva en `data-kcd2026` que agregue un job Flink debe seguir
  estos patrones o declarar explícitamente por qué se aparta (en su propio
  `plan.md`, sección Constitution Check o Decisiones de diseño).
- `data-kcd2026/docs/flink-job-patterns.md` se convierte en una fuente citable
  (`fuentes:`) para specs futuras que impliquen un job Flink nuevo.
- Si la feature 002 revela que el patrón 2 (transformación pura) no escala
  (p. ej. porque el estado compartido entre features lo hace impráctico),
  este ADR se deroga o se enmienda con un ADR posterior — no se edita en
  silencio.

## Evidencia y frontera del claim

- Los patrones 1–5 y 7 están verificados en código real y ya mergeado:
  `data-kcd2026/src/main/java/com/topaya/kcd2026/revenue/RevenueVentanaClienteJob.java`,
  `RevenueVentanaClienteJobTest.java`, `orden.avsc`, `revenue-ventana.avsc` —
  no son propuestas hipotéticas, son la feature 001 ya cerrada
  (`specs/001-revenue-ventana-cliente/`, evidencia en su carpeta
  `evidence/`).
- Lo que **no** está probado todavía: que estos patrones sigan siendo
  correctos para una segunda feature con requisitos distintos (por ejemplo,
  un job con múltiples fuentes, o con estado que sobrevive más allá de una
  ventana). Esa validación queda pendiente de la feature 002.
- `data-kcd2026/docs/flink-job-patterns.md` implementa este ADR: creado tras
  la aprobación, con los mismos extractos de código citados arriba.

## Aprobación

Aprobado por el owner (`romaria85`) el 2026-07-18, en el chat de esta sesión
(Artículo VI). La implementación (`docs/flink-job-patterns.md`) comienza a
partir de esta aprobación.
