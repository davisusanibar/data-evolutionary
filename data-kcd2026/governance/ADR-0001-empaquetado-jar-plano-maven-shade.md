# ADR-0001 — Empaquetado del job Flink como jar plano vía maven-shade-plugin

**Estado:** Aceptado
**Fecha:** 2026-07-18
**Owner:** romaria85
**Relaciona:** specs/001-revenue-ventana-cliente

## Contexto

`data-kcd2026/pom.xml` se creó sin plugin de empaquetado. Para validar la
feature 001 end-to-end contra un cluster Flink real (no solo el MiniCluster
embebido del test), se necesitó un jar ejecutable que Flink pudiera someter
vía `flink run`. La imagen `flink:1.20.2-scala_2.12-java11` solo trae en
`/opt/flink/lib` las librerías core (`flink-streaming-java`, `flink-clients`,
etc.); las dependencias del connector Kafka, Avro, Avro-Confluent-Registry y
el cliente de Schema Registry no vienen en la imagen base y deben viajar
dentro del jar del job.

## Decisión

Se agrega `maven-shade-plugin` (v3.5.1) a `data-kcd2026/pom.xml`, ejecutado
en la fase `package`, con:

- `ManifestResourceTransformer` fijando `Main-Class:
  com.topaya.kcd2026.revenue.RevenueVentanaClienteJob`.
- `ServicesResourceTransformer` para fusionar correctamente los archivos
  `META-INF/services/*` (necesario para el descubrimiento de
  conectores/serializadores vía SPI en vez de que una dependencia pise el
  archivo de otra).
- `flink-streaming-java` y `flink-clients` en scope `provided` (ya están en
  el cluster; no se empaquetan).
- El resto de dependencias (`flink-connector-kafka`, `flink-avro`,
  `flink-avro-confluent-registry`, `kafka-avro-serializer`, `avro`)
  permanecen en scope `compile` por defecto y sí se empaquetan.

Resultado: `mvn -pl data-kcd2026 -am package` genera un jar de ~23M,
ejecutable directamente con `flink run`.

## Alternativas consideradas

- **maven-assembly-plugin** con descriptor `jar-with-dependencies`: más
  simple de configurar, pero no ofrece transformers (en particular
  `ServicesResourceTransformer`); archivos `META-INF/services` de distintas
  dependencias se pisan entre sí en vez de fusionarse — riesgo real para
  conectores Flink que dependen de SPI para registrarse.
- **Empaquetar también Flink core** (sin `provided`): jar más grande e
  innecesario, y puede chocar en classpath con las clases que ya trae el
  cluster (`ClassNotFoundException`/`NoSuchMethodError` por versiones
  duplicadas de Flink en el classpath).
- **No generar jar plano; usar `flink run -C` con classpath externo**: evita
  el jar grande, pero exige distribuir las dependencias por separado a cada
  nodo del cluster — más frágil operativamente para una demo de conferencia.

## Consecuencias

- Cualquier job nuevo que se agregue a `data-kcd2026` hereda este plugin
  automáticamente (está a nivel de módulo, no de clase): el patrón "Flink en
  `provided`, conectores en `compile`" aplica por defecto a features futuras
  del mismo módulo. Hay que recordarlo al agregar una dependencia nueva: si
  se necesita en el cluster en tiempo de ejecución, no debe quedar en
  `provided`.
- El jar de ~23M no se commitea (vive en `target/`, ya ignorado por el
  `.gitignore` raíz); se regenera con `mvn package` cuando se necesite.
- No se resolvió publicación de este jar en ningún artefacto compartido
  (registry, release) — sigue siendo un build manual, local.

## Evidencia y frontera del claim

- `data-kcd2026/pom.xml` (plugin real, no descrito de memoria).
- `specs/001-revenue-ventana-cliente/evidence/flink-job-detail.json` y
  `flink-jobs-overview.json`: el jar generado con este plugin corrió como job
  `revenue-por-cliente-en-ventana` en estado `RUNNING` sobre un cluster Flink
  1.20.2 real, con Kafka y Schema Registry reales.
- No se probó con más de un job simultáneo en el mismo jar, ni con
  actualizaciones incrementales (rolling upgrade) del cluster — fuera del
  alcance de esta decisión.

## Aprobación

Aprobado por el owner (`romaria85`) el 2026-07-18, en el chat de esta sesión
(Artículo VI — la decisión la firma el humano, no la IA).
