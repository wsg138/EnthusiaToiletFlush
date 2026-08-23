# Enthusia deployment status

`EnthusiaToiletFlush` is the Velocity + Paper restart coordinator whose internal codename remains `queue-restart`.

## Canonical source

The canonical owner repository is `BadgersMC/EnthusiaToiletFlush`. This `wsg138` repository is a fork and currently contains additional commits not present on the canonical main branch, so future documentation must not silently assume either branch exactly matches the production JAR without artifact/provenance evidence.

## What production evidence proves

The latest Enthusia SMP backend JAR manifest contains **two Paper companion copies at version 9.8.7**:

- `EnthusiaToiletFlush-PaperCompanion-9.8.7.jar`
- `EnthusiaToiletFlush-Paper-Companion-9.8.7.jar`

Therefore the earlier statement that ToiletFlush was entirely deployment-unconfirmed was too broad. The **Paper/backend companion is installed on the SMP backend**.

The duplicate companion files are an operational cleanup item; filename presence alone does not prove which duplicate the server loaded.

## What the SMP snapshot does not prove

`enthusia-server-state` mirrors the Minecraft backend, not the full Velocity/proxy filesystem. It therefore does **not** prove that the authoritative Velocity coordinator is installed/running or reveal the current proxy/network restart schedule.

Without separate proxy evidence, do not claim as current Enthusia behavior that:

- scheduled proxy/full-network restarts are active,
- Pterodactyl restart execution is enabled,
- players are currently drained/rejoined through the complete queue workflow,
- CheckHacks rejoin gating is active,
- any specific recurring restart times are live.

Those features exist in the software, but the backend companion alone cannot provide the complete coordination flow.

## Player-visible behavior when the full coordinator is deployed

The software can provide:

- scheduled backend restart countdowns,
- graceful transfer/drain from a backend before shutdown,
- controlled post-restart rejoin ordering,
- public `/nextrestart`, `/restartschedule`, and `/lastrestart` information,
- proxy/full-network restart countdowns that disconnect players when needed,
- maintenance/access screens during restart windows.

Treat these as **implemented capabilities** until separate proxy deployment evidence confirms which are current on Enthusia.

## Documentation rule

For future Enthusia wiki automation:

- classify the **Paper companion as installed**,
- classify the **full Velocity-controlled service as deployment-unconfirmed from the SMP snapshot alone**,
- use `BadgersMC/EnthusiaToiletFlush` as canonical source ownership,
- use exact release/artifact provenance before choosing between canonical main and this diverged fork for version-specific implementation claims,
- publish only player-visible restart/countdown/rejoin behavior after the corresponding proxy deployment is confirmed,
- never publish Pterodactyl credentials, panel IDs, trusted topology, signing/control secrets, or internal recovery details.