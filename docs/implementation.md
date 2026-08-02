# Implementation Blueprint

## 1. Architecture overview

Two-module Gradle project + a shared `common/` for the wire protocol.

```
queue-restart/
  common/                shared message DTOs + codec (no framework deps)
  velocity/              proxy-side plugin (the brain)
  paper-companion/       backend-side agent (restart + CheckHacks bridge)
```

Velocity plugin is hexagonal:

- domain         — pure logic, no Velocity / Adventure / Configurate / cron / IO
- application    — orchestration, depends only on domain
- infrastructure — adapters: Velocity API, plugin messaging, config IO, sounds, cron

## 2. Layer Dependency Rules

domain <- application <- infrastructure.

- `domain.*` MUST NOT import from `application.*` or `infrastructure.*`.
- `application.*` MAY import from `domain.*` only.
- `infrastructure.*` MAY import from `domain.*` and `application.*`.
- Tests MAY import freely from any layer.
- The `common/` module is treated as `domain` for layering purposes (pure DTOs + codec, no framework imports).

Konsist enforces these via `velocity/src/test/kotlin/architecture/LayerRulesTest.kt`
substituting `__BASE_PACKAGE__` = `com.badgersmc.queuerestart.velocity`.

## 3. Forbidden Domain Annotations

```yaml
forbidden: []
```

Velocity / Paper plugins use no framework annotations inside domain — kept
empty intentionally. Future entries go here if a DI/framework annotation
ever leaks toward domain.

## 4. Package layout (Velocity module)

```
com.badgersmc.queuerestart.velocity
  ├── domain
  │   ├── coordinator/  RestartCoordinator (state machine), RestartState
  │   ├── cohort/       Cohort, CohortMember
  │   ├── rank/         RankLadder, RankWeight
  │   ├── countdown/    CountdownSchedule, MarkSecond, SoundCue
  │   └── id/           ServerId, PlayerId
  ├── application
  │   ├── schedule/     ScheduleService, RestartService
  │   ├── drain/        DrainPlanner, RejoinService
  │   ├── gate/         CheckGate
  │   └── ports/        OUTBOUND interfaces:
  │                       ProxyPort, MessagingPort, AudiencePort, ClockPort,
  │                       ConfigPort, QueuePort
  └── infrastructure
      ├── velocity/     ProxyAdapter, QueueAdapter
      ├── messaging/    PluginMessageAdapter, ChannelHandler
      ├── audience/     AdventureAudienceAdapter, MiniMessageRenderer
      ├── config/       ConfigurateConfigAdapter
      ├── schedule/     CronUtilsScheduler
      ├── clock/        SystemClockAdapter
      └── command/      SchedRestartCommand, QRestartAdminCommand
```

Paper companion is flat:

```
com.badgersmc.queuerestart.paper
  ├── CompanionPlugin
  ├── ProxyMessageListener
  ├── RestartExecutor
  └── CheckHacksBridge   (subscribes to CheckCompletedEvent — see §7)
```

## 5. State machine

`RestartCoordinator` (one instance per target server):

```
IDLE
  └─arm(reqMinutes,cohort)──► ARMED
                                └─tick──► COUNTDOWN
                                            └─T-0──► DRAINING
                                                     └─empty | disconnect+timeout──► RESTART_SENT
                                                                                    └─ping fail──► SERVER_DOWN
                                                                                                    └─ping ok──► REJOIN_RELEASE
                                                                                                                  └─cohort empty──► IDLE
cancel() valid in: ARMED, COUNTDOWN
```

Lives in domain. Time and side effects come in via `ClockPort` ticks and
application-level commands. No Velocity types in this class.

## 6. Wire protocol (`common/`)

Channel: `qrestart:v1`. Frame: `[u8 type][payload]`.

| Type | Direction | Payload |
|---|---|---|
| 0x01 | proxy→backend | DrainRequest (advisory; no body) |
| 0x02 | backend→proxy | DrainAck `i32 remainingPlayers` |
| 0x10 | proxy→backend | RestartNow `u8 mode` `string arg` |
| 0x20 | backend→proxy | CheckHacksResult `uuid playerId` `u8 outcome` |

`Codec` provides symmetric encode/decode used by both modules — single
source of truth, no duplicate parsing.

## 7. CheckHacks integration (additive change to D:/CheckHacks)

Inspected `D:/CheckHacks-fork/src/main/java/me/branduzzo/checkHacks/managers/CheckManager.java:329`
(`finishCheck(UUID)`). It already aggregates `anyDetected` / `anyProtected` /
`allClean` (lines 346–348) — but exposes nothing publicly.

Add (in CheckHacks-fork, separate PR — tracked here as T-042):

- `me.branduzzo.checkHacks.api.CheckCompletedEvent extends Event`
  carrying `UUID playerId`, `boolean clean = allClean`,
  `boolean detected = anyDetected`, `boolean protected_ = anyProtected`,
  `Set<String> detectedHacks` (filtered from `allHacks` where
  `data.getResults().get(h.getId()) == HackResult.DETECTED`).
- Fire site: end of `finishCheck`, after the existing
  `cfg.isCommandIfCleanEnabled()` branch (~line 414).

Companion subscribes via Bukkit listener and forwards a
`CheckHacksResult` plugin message. CheckHacks stays a soft-depend.

## 8. Configuration (Velocity `plugins/queue-restart/config.yml`)

See `velocity/src/main/resources/config.yml` for the canonical default.

## 9. Commands & permissions

| Command                              | Perm                                | Purpose                                  |
|--------------------------------------|-------------------------------------|------------------------------------------|
| `/schedrestart <minutes> [server]`   | `queuerestart.command.schedrestart` | Ad-hoc arm                               |
| `/schedrestart cancel [server]`      | `queuerestart.command.schedrestart` | Cancel armed/counting-down               |
| `/schedrestart status`               | `queuerestart.command.schedrestart` | Inspect coordinator states               |
| `/qrestart reload`                   | `queuerestart.command.admin`        | Reload config + cron                     |
| `/qrestart trigger <scheduleName>`   | `queuerestart.command.admin`        | Run named schedule on demand             |
| (perm) `queuerestart.bypass.drain`        | — | Excluded from drain & cohort                  |
| (perm) `queuerestart.bypass.checkhacks`   | — | Skip CheckHacks gate after restart            |

## 10. Sound design

Caps in code: hard-cap volume at 1.0; warn on load above 0.8 (REQ-006).
Default suite chosen for audibility without piercing — note-block + UI clicks
under 0.7 volume. All keys/volumes/pitches user-overridable.
