# Enthusia deployment status

This repository is **related infrastructure but is not currently proven by the Enthusia SMP snapshot to be an active Enthusia production service**.

The implementation is a Velocity + Paper restart coordinator (internal codename `queue-restart`) with scheduled backend/proxy/network restarts, player draining/rejoin queues, maintenance mode, CheckHacks gating and Pterodactyl restart control. The existing README currently describes its running deployment as **BadgersMC**.

The normal `enthusia-server-state` snapshot mirrors the SMP backend and does not provide enough Velocity/network deployment evidence to override that repository documentation. Therefore future Enthusia wiki automation must classify this repository as **deployment-unconfirmed** unless separate proxy/network evidence shows an Enthusia deployment.

## Documentation rule

- Keep the main README/docs as the technical feature reference for the software.
- Do **not** list scheduled network restart/rejoin behavior as a current Enthusia SMP feature solely because this repository is under `wsg138` or carries the `EnthusiaToiletFlush` name.
- If it is deployed to Enthusia later (or already is and separate deployment evidence is added), update this file and `wsg138/enthusia-server-state/DOCUMENTATION_INVENTORY.md` with the current schedule/behavior.
- A public player wiki would only need user-visible restart scheduling/countdown/rejoin behavior; Pterodactyl credentials, panel IDs, trusted topology and internal recovery details remain operational documentation.