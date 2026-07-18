# Procedencia de `spec_kit_gate.py` (vendorizado)

Este gate **no** se distribuye por el wheel `edaios-core` ni por el bundle Spec Kit:
vive en `tools/validation/` del repo de EDAIOS Core y se copia (vendoriza) al consumer.
Este archivo registra de dónde salió la copia, para que el pin sea trazable.

| Campo | Valor |
|---|---|
| Repo fuente | `edaiosv` (`git@bitbucket.org:data_and_ia/edaiosv.git`) |
| Rama | `feature/consumer-release-profile` |
| Commit | `fc579e70edeab84253f251ed5f3aecdd7225fc32` |
| Motivo | Soporta `--profile consumer-release` (ADR-0016). `main` aún no lo tiene. |

## Re-sincronización

**No re-copiar desde `main` hasta que el PR de ADR-0016 mergee**: `main` no conoce
`--profile consumer-release` y el gate del consumer volvería a fallar.

Tras el merge del PR:

1. Re-copiar desde `main`:
   `cp <edaiosv>/tools/validation/spec_kit_gate.py tools/validation/spec_kit_gate.py`
2. Actualizar la fila `Commit` de esta tabla con el sha de `main`.
3. Re-instalar el bundle para pinear los comandos:
   `specify bundle install <edaiosv>/core/framework/extensions/sdd-adapter/spec-kit/bundle --offline`

## Deuda conocida (seguimiento)

El gate debería **shipearse** (como componente del bundle o del wheel) para que los
consumers dejen de vendorizar una copia cruda. Mientras no ocurra, este sidecar es el
único pin posible. Ver ADR-0016 § Decisión 6 (seguimiento).
