---
description: Reduce ambiguedades de una especificacion activa antes de tomar decisiones tecnicas.
---

<!-- GENERADO desde .specify/commands; no editar a mano. -->

# Aclarar

## Entrada

```text
$ARGUMENTS
```

1. Resolver la feature con `python3 tools/operations/feature_context.py resolve --path-only`; un argumento explicito tiene precedencia sobre `.specify/feature.local.json` y el handoff canonico `.specify/feature.json`.
2. Leer su `spec.md` y contrato tipado.
3. Revisar cobertura de alcance, owner, valor, datos, privacidad, seguridad, errores y criterios de exito.
4. Formular como maximo cinco preguntas de alto impacto; proponer una recomendacion y sus implicancias.
5. Registrar las respuestas confirmadas en `## Clarifications` dentro de `spec.md` y actualizar requisitos afectados.
6. No convertir decisiones tecnicas en requisitos de negocio ni completar verdad de dominio sin su owner.
7. Cambiar `fase` a `clarified` solo cuando no queden ambiguedades bloqueantes y volver a ejecutar el gate Spec Kit.
