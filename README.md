# EnthusiaToiletFlush

Velocity-CTD plugin + Paper companion for graceful scheduled backend, proxy,
and full-network restarts on BadgersMC. Backend restarts drain players to a
hub and rejoin them in rank-weighted order behind a CheckHacks gate.

(Internal codename: `queue-restart` — Gradle subproject names, package paths,
plugin-message channel `qrestart:v1`, and SLP markers retain that identifier
for compatibility with running deployments.)

Built with the [SPEAR methodology](docs/) — every feature traces a `REQ-NNN`
in `docs/requirements.md`.

## Modules

| Module | Target | Role |
|---|---|---|
| `common/` | JVM 21 | Wire DTOs + `Codec` for channel `qrestart:v1`. No framework deps. |
| `velocity/` | Velocity-CTD 3.5.x | Authoritative scheduler, countdowns, backend drain/rejoin, maintenance mode, and Pterodactyl proxy/network restart actions. |
| `paper-companion/` | Paper 1.21.x | Backend agent: executes normal backend shutdown and bridges CheckHacks events. |

## Build

Java 21, Gradle Kotlin DSL. Bundled wrapper.

```bash
./gradlew check
./gradlew :velocity:konsistCheck
./gradlew :velocity:shadowJar
./gradlew :paper-companion:shadowJar
```

Snapshot outputs:

```text
velocity/build/libs/velocity-0.1.0-SNAPSHOT.jar
paper-companion/build/libs/paper-companion-0.1.0-SNAPSHOT.jar
```

## Install

1. Drop the Velocity jar into the proxy's `plugins/` directory.
2. Drop the companion jar into **every** backend's `plugins/` directory.
3. Start the proxy first, then the backends.
4. The default Velocity config materialises at
   `plugins/queue-restart/config.yml`; edit it and run `/qrestart reload`.

The Velocity jar does **not** go on backend servers. The Paper companion jar
does **not** go on Velocity. Keep both components installed: Velocity owns
schedules, maintenance mode, external restart actions, and the rejoin queue;
the companion performs a normal backend's local shutdown after Velocity drains
it.

## Configuration

The canonical sample is
[`velocity/src/main/resources/config.yml`](velocity/src/main/resources/config.yml).
See [`docs/network-restarts.md`](docs/network-restarts.md) for Pterodactyl
setup, dry-run testing, migration, recovery, and troubleshooting.

## Commands

| Command | Permission | Purpose |
|---|---|---|
| `/schedrestart <minutes> [server]` | `queuerestart.command.schedrestart` | Arm an ad-hoc backend restart |
| `/schedrestart <server> <duration> [--silent] [reason...]` | `queuerestart.command.schedrestart` | Schedule a configured backend by name |
| `/schedrestart proxy <duration> [--silent] [reason...]` | `queuerestart.command.schedrestart` | Restart only Velocity and disconnect everyone |
| `/schedrestart network <duration> [--silent] [reason...]` | `queuerestart.command.schedrestart` | Restart configured network members, then Velocity |
| `/schedrestart at server <server> <HH:mm> [--silent] [reason...]` | `queuerestart.command.schedrestart` | Schedule a backend at a clock time |
| `/schedrestart at proxy\|network <HH:mm> [--silent] [reason...]` | `queuerestart.command.schedrestart` | Schedule a proxy or full-network restart at a clock time |
| `/schedrestart cancel [server]` | `queuerestart.command.schedrestart` | Cancel an armed backend restart |
| `/schedrestart cancel <plan-id\|proxy\|network>` | `queuerestart.command.schedrestart` | Cancel a network restart plan |
| `/schedrestart status` | `queuerestart.command.schedrestart` | Inspect coordinator states |
| `/nextrestart` | none | Show the next public restart concisely |
| `/restartschedule` | none | Show public recurring restarts concisely |
| `/lastrestart` | none | Show when the proxy and configured backends last restarted |
| `/qrestart reload` | `queuerestart.command.admin` | Reload Velocity configuration |
| `/qrestart trigger <name>` | `queuerestart.command.admin` | Run a named configured schedule on demand |

Per-player bypass permissions and rank-ladder details are documented in
[`docs/permissions.md`](docs/permissions.md).

## Network-wide restarts

`network-restart` in the Velocity configuration is the authoritative source
for daily and weekly schedules and Pterodactyl target mappings.

`DRY_RUN` is non-disruptive: it validates scheduling and countdown behavior but
does not move or disconnect players, enable maintenance mode, contact
Pterodactyl, or issue power actions. Before real restarts, set
`PTERODACTYL_API_KEY` in the Velocity process environment, verify every panel
identifier, change the executor to `PTERODACTYL`, reload, and test one
non-critical backend before testing Velocity or the full network.

Automatic schedules run only when `network-restart.enabled: true`. Real proxy
and network actions require `executor: PTERODACTYL`, a valid API key, and valid
panel identifiers for the proxy and every configured network member.

## CheckHacks integration

The companion translates `me.branduzzo.checkHacks.api.CheckCompletedEvent`
into `CheckHacksResult` plugin messages. CheckHacks remains a soft dependency;
the plugin enables without it. The additive integration proposal is documented
in [`docs/checkhacks-fork-pr.md`](docs/checkhacks-fork-pr.md).

## Verification

```bash
./gradlew check shadowJar --no-daemon
```

This runs unit tests, Pterodactyl executor tests, network restart failure-path
tests, and Konsist architecture rules, then packages both deployable jars.
Pull requests and release builds run the same check before jars are published.

End-to-end procedures are in [`docs/e2e-runbook.md`](docs/e2e-runbook.md).

## Architecture

```text
domain  ← application ← infrastructure
            ↑              ↑
         (ports)       (adapters)
```

Konsist (`velocity/src/test/kotlin/architecture/LayerRulesTest.kt`) enforces
layer direction and keeps domain code free of Velocity, Paper, Adventure,
Configurate, and cron implementation dependencies.

## Docs

- [`docs/tech-stack.md`](docs/tech-stack.md)
- [`docs/requirements.md`](docs/requirements.md)
- [`docs/implementation.md`](docs/implementation.md)
- [`docs/tasks.md`](docs/tasks.md)
- [`docs/permissions.md`](docs/permissions.md)
- [`docs/e2e-runbook.md`](docs/e2e-runbook.md)
- [`docs/network-restarts.md`](docs/network-restarts.md)
- [`docs/checkhacks-fork-pr.md`](docs/checkhacks-fork-pr.md)

## License

GNU Affero General Public License v3.0. See [`LICENSE`](LICENSE).
