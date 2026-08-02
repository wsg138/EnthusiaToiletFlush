# EnthusiaToiletFlush

Velocity-CTD plugin + Paper companion for graceful, scheduled backend
restarts on BadgersMC. Drains players to the hub, restarts the backend,
auto-rejoins them in rank-weighted order behind a CheckHacks gate. It can
also perform a proxy-wide countdown and cleanly stop Velocity so the process
supervisor can relaunch it.

(Internal codename: `queue-restart` — Gradle subproject names, package
paths, plugin-message channel `qrestart:v1`, and SLP markers retain
that identifier for compatibility with running deployments.)

Built with the [SPEAR methodology](docs/) — every feature traces a
`REQ-NNN` in `docs/requirements.md`.

## Modules

| Module | Target | Role |
|---|---|---|
| `common/` | JVM 21 | Wire DTOs + `Codec` for channel `qrestart:v1`. No framework deps. |
| `velocity/` | Velocity-CTD 3.5.x | Proxy plugin: countdown, drain, backend restart trigger, proxy shutdown, rejoin queue. Hexagonal: domain ← application ← infrastructure. |
| `paper-companion/` | Paper 1.21.x | Backend agent: executes restart, bridges CheckHacks events. |

## Build

Java 21, Gradle Kotlin DSL. Bundled wrapper.

```bash
./gradlew check          # unit tests + Konsist layer rules
./gradlew :velocity:konsistCheck   # explicit layer-rules gate (CI)
./gradlew :velocity:shadowJar      # plugin jar
./gradlew :paper-companion:shadowJar
```

Outputs:

```
velocity/build/libs/queue-restart-velocity-0.1.0-SNAPSHOT.jar
paper-companion/build/libs/queue-restart-paper-companion-0.1.0-SNAPSHOT.jar
```

## Install

1. Drop the velocity jar into the proxy's `plugins/` directory.
2. Drop the companion jar into **every** backend's `plugins/` directory.
3. Start the proxy first, then the backends.
4. Default config materialises at `plugins/queue-restart/config.yml` —
   edit and run `/qrestart reload`.
5. Enable automatic process restart for both Paper and Velocity in the panel
   or service manager. The plugin performs clean shutdowns; it cannot start a
   new JVM after the current process exits.

## Configuration

Canonical sample lives at
[`velocity/src/main/resources/config.yml`](velocity/src/main/resources/config.yml).
See `docs/implementation.md` §8 for the field reference. Sound volumes
above 0.8 emit a startup warning; above 1.0 are rejected (REQ-006).

## Commands

| Command | Permission | Purpose |
|---|---|---|
| `/schedrestart <minutes> [server]` | `queuerestart.command.schedrestart` | Arm an ad-hoc backend restart. Use the reserved target `proxy` to restart Velocity. |
| `/schedrestart cancel [server]` | `queuerestart.command.schedrestart` | Cancel an armed backend or proxy countdown. |
| `/schedrestart status` | `queuerestart.command.schedrestart` | Inspect coordinator states, including an active proxy countdown. |
| `/qrestart reload` | `queuerestart.command.admin` | Reload config + cron. |
| `/qrestart trigger <name>` | `queuerestart.command.admin` | Run named backend schedule on demand. |

Proxy example:

```text
/schedrestart 5 proxy
/schedrestart cancel proxy
```

A proxy restart disconnects all connected players at T-0 with a reconnect
message. There is no drain or automatic rejoin phase because the process
hosting every proxy connection is the component being restarted.

Per-player bypass perms and rank-ladder details: see
[`docs/permissions.md`](docs/permissions.md).

## CheckHacks integration

The companion translates `me.branduzzo.checkHacks.api.CheckCompletedEvent`
into `CheckHacksResult` plugin messages. CheckHacks remains a
soft-depend — the plugin enables fine without it. To wire it up, install
the additive PR described in [`docs/checkhacks-fork-pr.md`](docs/checkhacks-fork-pr.md).

## Verification

Unit suite: `./gradlew test` — covers wire codec, rank ladder, countdown
schedule, restart state machine, drain planner, rejoin service, check gate,
schedule service, hub fallback, plugin-message adapter, queue adapter,
configurate adapter, cron-utils scheduler, both command handlers, proxy
restart lifecycle, restart executor, CheckHacks bridge.

End-to-end runbook: [`docs/e2e-runbook.md`](docs/e2e-runbook.md).

## Architecture

```
domain  ← application ← infrastructure
            ↑              ↑
         (ports)       (adapters)
```

Konsist (`velocity/src/test/kotlin/architecture/LayerRulesTest.kt`)
enforces:

- `domain.*` imports nothing from application or infrastructure.
- `application.*` depends only on domain.
- `domain.*` is free of `com.velocitypowered`, `com.velocityctd`,
  `org.bukkit`, `io.papermc`, `net.kyori`, `org.spongepowered.configurate`,
  `com.cronutils`.

`./gradlew :velocity:konsistCheck` is the layer-violation gate for CI.

## Docs

- [`docs/tech-stack.md`](docs/tech-stack.md) — languages, libraries, versions.
- [`docs/requirements.md`](docs/requirements.md) — EARS spec (REQ-001..REQ-090).
- [`docs/implementation.md`](docs/implementation.md) — architecture blueprint.
- [`docs/tasks.md`](docs/tasks.md) — task ledger with evidence per item.
- [`docs/permissions.md`](docs/permissions.md) — perms + LuckPerms tracks.
- [`docs/e2e-runbook.md`](docs/e2e-runbook.md) — manual verification.
- [`docs/checkhacks-fork-pr.md`](docs/checkhacks-fork-pr.md) — additive
  CheckHacks PR (separate repo).

## Licence

Internal BadgersMC project. Contact Badger before redistributing.
