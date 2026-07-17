# Plan técnico — Revenue por cliente en ventana temporal

**Feature:** KCD2026-ORDERS-REVENUE-WINDOW
**Spec:** `data-kcd2026/specs/001-orders-revenue-window/spec.md`
**Owner:** Data & AI Lead

## Constitution Check

Verificado contra los 7 artículos de la Constitución EDAIOS. Un `VIOLA` detiene
el plan; el camino sería un ADR, no una excepción.

| # | Artículo | Veredicto | Evidencia |
|---|---|---|---|
| I | El conocimiento manda | PASS | El contrato vive en `feature.spec.yaml` y `spec.md`; el `.avsc` y el job los consumen. `contract_check.py` falla si el esquema pretende mandar sobre la spec. |
| II | Spec antes que artefacto | PASS | La spec declara FR-001..FR-007 y SC-001..SC-006 antes del job. El contrato tipado precede al `.avsc`. |
| III | El canon crece por decisión | N/A | La feature no introduce frontera estructural en EDAIOS Core. Es un consumer que aplica el gobierno existente. |
| IV | Cero cifras sin fuente | PASS | La única cifra del pipeline (`sum_o_totalprice`) declara su fuente (tópico `orders`), su alcance (ventana de N segundos) y su límite (Clarification 2: no es exactitud fiscal). |
| V | Una fuente, muchas vistas | PASS | El contrato tiene una fuente (`feature.spec.yaml`) y tres representaciones (prosa FR-002, `.avsc`, job). El gate verifica que no deriven. |
| VI | La IA consume; el humano firma | PASS | El job lo genera un agente desde la spec; la aceptación de la feature y el cierre exigen firma del owner. Ningún gate acepta por su cuenta. |
| VII | Privacidad por diseño | PASS | T0 declarado. `orders` es TPC-H sintético: no hay PII. Ningún campo del contrato de salida identifica a una persona; `o_custkey` es una clave sintética. |

**Constitución verificada:** 1.0.0 · pin no aplicable — este consumer no vive en
el árbol de Core y no puede resolver `constitution.md` localmente. La
verificación es manual y se registra aquí. Esa es la frontera honesta: el
consumer aplica los artículos, no los verifica mecánicamente.

## Gate Impact

| Gate | Efecto |
|---|---|
| `contract_check.py` | **Nuevo.** Verifica que `feature.spec.yaml`, FR-002 y el `.avsc` declaren los mismos campos. Falla cerrado ante deriva. |
| `mvn -pl data-kcd2026 test` | **Nuevo.** Verifica SC-002, SC-003, SC-004 y SC-006 sin cluster. |
| Compose local | No es un gate. Levantar el entorno demuestra que corre, no que es correcto. |

## Enfoque técnico

1. **El contrato primero.** `feature.spec.yaml` declara los cinco campos; el
   `.avsc` los materializa; `contract_check.py` los compara. El esquema no puede
   cambiar en silencio.
2. **Lógica separada del wiring.** La agregación y el descarte son funciones
   puras sobre `GenericRecord` (`RevenueVentana`), de modo que SC-002..SC-004 se
   verifican con JUnit y no levantando Docker. Un criterio que solo se puede
   comprobar con un cluster arriba no es falsable en CI.
3. **Sin esquema embebido.** FR-001 exige el registry; el `.avsc` del classpath
   solo resuelve el reader schema. `contract_check.py` busca literales de
   esquema en el fuente y falla si aparecen.
4. **Ventana de processing time.** Clarification 1: `o_orderdate` es una fecha
   sin hora y no sirve como marca de evento. Event time exigiría decidir
   watermarks y datos tardíos; esta feature no toma esa decisión.

## Alternativas consideradas

- **Event time con watermarks:** rechazada; el origen no tiene marca temporal
  utilizable y la decisión sobre datos tardíos excede la demo.
- **Conservar `decimal` en la salida:** rechazada para la demo por costo de
  serialización Avro decimal; el precio es explícito en Clarification 2 y en el
  `doc` del campo. Un pipeline de facturación no podría aceptar este trade-off.
- **Generar clases Avro con `avro-maven-plugin` (como el módulo CDC):**
  rechazada; `GenericRecord` mantiene el esquema como dato en tiempo de
  ejecución y hace observable la deriva. Con clases generadas, el contrato se
  congela en el bytecode y el gate pierde su objeto.
- **Verificar solo con el compose:** rechazada; sería una demo que corre, no una
  feature con criterios falsables.

## Frontera del claim del plan

El plan puede sostener que el contrato es verificable y que la lógica es
testeable sin cluster. No sostiene que el job esté probado en un cluster real,
ni rendimiento, ni tolerancia a fallos, ni corrección ante datos desordenados.
