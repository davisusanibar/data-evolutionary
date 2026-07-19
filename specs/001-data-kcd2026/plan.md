# Plan · Submódulo `data-kcd2026`

## Enfoque

El módulo se agrega como unidad aislada del reactor: hereda el parent, fija sus
dependencias con versión explícita y no altera el resto de los módulos. La
verificación se apoya en el propio ciclo de Maven, sin infraestructura externa.

## Secuencia

1. Declarar `data-kcd2026` en `<modules>` del `pom.xml` raíz.
2. Crear `data-kcd2026/pom.xml` con parent `com.topaya:oss:1.0-SNAPSHOT`,
   coordenadas propias y packaging jar.
3. Fijar las dependencias Flink con versión explícita.
4. Crear `com.topaya.kcd2026.KafkaHello` como superficie mínima de arranque.
5. Verificar con el ciclo de Maven y registrar evidencia local.

## Riesgos y límites

- La mediación de dependencias de Flink puede arrastrar versiones transitivas
  distintas de las fijadas; se verifica con `dependency:list`, no se asume.
- La evidencia es una observación local de este workspace sobre Maven 3.9.8; no
  es un assessment de producción ni una promesa de comportamiento en un cluster.
- Las versiones de Flink quedan duplicadas entre `data-kcd2025` y
  `data-kcd2026`; unificarlas exigiría una decisión posterior sobre
  `dependencyManagement` en el parent, fuera del alcance de ADR-0001.

## Constitution Check

| Principio | Veredicto | Evidencia |
|---|---|---|
| I. El conocimiento manda | PASS | ADR-0001, spec tipada y matriz de verificación preceden a la aceptación del módulo. |
| II. Spec antes que artefacto | PASS | FR-001..FR-004 y SC-001..SC-004 quedan versionados junto al código del módulo. |
| III. El canon crece por decisión | PASS | El registro del módulo en el reactor se respalda en ADR-0001 y no en configuración tácita. |
| IV. Cero cifras sin fuente | PASS | Flink 1.20.1 y conector 3.3.0-1.20 provienen de data-kcd2026/pom.xml, trazados en evidence/sources.md. |
| V. Una fuente, muchas vistas | PASS | Las coordenadas del módulo se derivan del parent com.topaya:oss y no se duplican en otra vista. |
| VI. La IA consume; el humano firma | PASS | La aceptación queda pendiente de firma de David Dali Susanibar Arce; el gate sólo verifica artefactos. |
| VII. Privacidad por diseño | PASS | Tramo T0: el módulo no procesa datos personales y la evidencia local no contiene payloads. |

Constitución verificada: sha256:392b1518ce87fc2464634b8447aebe8bf69107a4a8f44bd510b1393cc3a4d77c

## Gate Impact

- `spec_kit_gate.py --profile consumer-release`: valida el frontmatter, la spec
  tipada, la cobertura FR en tareas y la matriz SC de esta feature.
- Frontera del claim: el gate acredita la existencia y coherencia del contrato,
  no el despliegue del módulo ni su comportamiento en un cluster.
