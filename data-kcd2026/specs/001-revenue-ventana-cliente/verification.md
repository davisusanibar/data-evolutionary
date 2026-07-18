# Matriz de verificación SC → evidencia — 001-revenue-ventana-cliente

Cada criterio de éxito declara cómo se verificó y qué evidencia dejó. Los archivos
bajo el directorio evidence/ son capturas reales de los comandos, no resúmenes
redactados a posteriori.

| Criterio | Cómo se verifica | Evidencia | Estado |
|---|---|---|---|
| SC-001 | Ejecutar el pipeline contra el fixture determinista y comparar cada fila contra la tabla de referencia calculada aparte. | `evidence/sc-001-revenue-vs-referencia.txt` | verificado |
| SC-002 | Compilar el módulo con los contratos íntegros y confirmar que las clases Avro se generan. | `evidence/sc-002-build-verde.txt` | verificado |
| SC-003 | Introducir la deriva y confirmar que el build falla identificando el campo, sin producir artefacto ejecutable. | `evidence/sc-003-build-rojo-deriva.txt` | verificado |
| SC-004 | Cronometrar el canary completo desde un único comando. | `evidence/sc-004-canary-cronometraje.txt` | verificado |
| SC-005 | Ejecutar el gate con perfil `consumer-release` y comprobar que degradar un artefacto lo devuelve a rojo. | `evidence/sc-005-gate-consumer-release.txt` | verificado |
| SC-006 | Levantar la infraestructura, producir el fixture en Avro, someter el job al cluster Flink y comparar la salida real contra la referencia. | `evidence/sc-006-end-to-end-infra-real.txt` | verificado |

## Resultados

- **SC-001** — 4 filas (2 clientes × 2 ventanas) coinciden exactamente con la
  referencia. El evento desordenado del fixture se imputa a su ventana por tiempo
  de evento: cliente 1 en la ventana 0 suma 160.00 sobre 3 órdenes.
- **SC-003** — la variante por renombrado falla con
  `symbol: method getTotalPrice()`; la variante por cambio de tipo falla con
  `incompatible types: java.lang.String cannot be converted to double`.
- **SC-004** — 4 s en la variante por renombrado y 5 s en la de tipo, frente al
  umbral declarado de 30 s.
- **SC-005** — 42/42 checks, exit 0.

## La prueba negativa

SC-001, SC-003 y SC-005 son criterios que solo significan algo si se les ha visto
fallar. Los tres se rompieron a propósito:

| Criterio | Perturbación introducida | Resultado |
|---|---|---|
| SC-001 | referencia alterada de 160.00 a 160.01 | test en rojo: `expected: <160.01> but was: <160.0>` |
| SC-003 | es en sí mismo la prueba negativa del contrato | build en rojo, restaurado a verde |
| SC-005 | pin de la constitución alterado un carácter; y FR-005 sin cobertura | 41/42 en cada caso, verde al restaurar |

## El resultado end-to-end

SC-006 cierra la brecha que quedó abierta en la primera pasada. El job se sometió
al cluster Flink 1.20.2 del repositorio, leyendo Avro desde Kafka a través del
Schema Registry y escribiendo Avro de vuelta:

| Cliente y ventana | Referencia | End-to-end |
|---|---|---|
| cliente 1 · ventana 0 | 160.00 sobre 3 órdenes | 160.0 sobre 3 |
| cliente 2 · ventana 0 | 75.25 sobre 1 orden | 75.25 sobre 1 |
| cliente 1 · ventana 1 | 200.75 sobre 2 órdenes | 200.75 sobre 2 |
| cliente 2 · ventana 1 | 30.00 sobre 3 órdenes | 30.0 sobre 3 |

La aritmética verificada en local es la misma que atraviesa el transporte real.

## Hallazgo de semántica: las ventanas no cierran solas

La verificación local y la end-to-end **no** son equivalentes, y la diferencia
importa para la demo en vivo.

Con un stream acotado, Flink emite un watermark final al terminar la entrada y
todas las ventanas pendientes cierran. Con una fuente Kafka **no acotada** eso no
ocurre: una ventana solo cierra cuando el watermark supera su fin, y el watermark
solo avanza si siguen llegando eventos. El fixture por sí solo deja la última
ventana abierta indefinidamente.

Por eso la entrada de SC-006 incluye dos eventos de avance (cliente 99, con
timestamp muy posterior) cuyo único propósito es empujar el watermark. No es un
truco de laboratorio: es la semántica que gobierna cualquier pipeline de ventanas
en streaming, y en una demostración en vivo conviene decirlo en voz alta.

## Cierre

El owner declaró el cierre el 2026-07-18. La declaración literal, su alcance y la
naturaleza de lo que constituye —un registro de trazabilidad, no una firma
verificable, porque EDAIOS no instala mecanismo de firma— están en
`evidence/cierre-owner.txt`.

## Lo que sigue sin verificarse

El comportamiento ante fallo y recuperación —checkpoints, reinicio del job,
exactly-once contra el sink— no se ejercitó. El sink está configurado como
`AT_LEAST_ONCE` y no se reclama nada más fuerte.
