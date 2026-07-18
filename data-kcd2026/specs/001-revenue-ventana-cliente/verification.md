# Matriz de verificación SC → evidencia — 001-revenue-ventana-cliente

Cada criterio de éxito declara cómo se verifica y qué evidencia deja al cerrar.
La evidencia se deposita bajo el directorio evidence/ de esta feature **en el
momento del cierre**; en fase `tasked` está declarada, no producida. Ningún
criterio se da por cumplido sin su artefacto correspondiente.

| Criterio | Cómo se verifica | Evidencia de cierre | Estado |
|---|---|---|---|
| SC-001 | Ejecutar el job contra el fixture determinista y comparar fila a fila el revenue por cliente y ventana contra la tabla de referencia calculada aparte. | evidence/sc-001-revenue-vs-referencia.txt | pendiente |
| SC-002 | Ejecutar la compilación del módulo con los contratos íntegros y confirmar que las clases Avro se generan. | evidence/sc-002-build-verde.txt | pendiente |
| SC-003 | Introducir la deriva en el contrato de entrada, compilar y confirmar que el build falla identificando el campo derivado y que no se produce artefacto ejecutable. | evidence/sc-003-build-rojo-deriva.txt | pendiente |
| SC-004 | Cronometrar el canary completo — romper, compilar, observar el fallo y revertir — desde un único comando. | evidence/sc-004-canary-cronometraje.txt | pendiente |
| SC-005 | Ejecutar el gate SDD con perfil `consumer-release` sobre la feature y comprobar además que degradar un artefacto lo devuelve a rojo. | evidence/sc-005-gate-consumer-release.txt | pendiente |

## Nota sobre la prueba negativa

SC-003 y SC-005 son criterios que se demuestran **fallando a propósito**. Una
evidencia que solo muestre el caso verde no los satisface: cada uno exige el par
completo, verde y rojo, porque lo que se está verificando es precisamente que la
puerta cierra cuando debe cerrar.
