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

## Lo que no se verificó

La ejecución extremo a extremo contra Kafka y Schema Registry **no** se ejecutó:
la infraestructura de `infra/dockercompose` no estaba levantada. El job compila y
está completo, pero su comportamiento en runtime contra el transporte real no
tiene evidencia aquí y no se reclama. SC-001 verifica la aritmética del caso, que
es lo que declaró verificar.
