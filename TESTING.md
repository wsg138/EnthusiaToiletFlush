# EnthusiaToiletFlush testing guide

This repository owns its handwritten restart/control regression tests. Sentinel Sim is a separate built-plugin/runtime compatibility layer. It must not become the storage location for these unit and integration tests.

## Test-hardening additions

### `RestartPlanStateTest`
Exhaustively classifies every `PlanState` against three safety questions:

- is the plan still active?
- may an operator cancel it?
- must it block an overlapping future restart?

The reviewed enum set is explicit, so adding a future state forces a deliberate policy decision. `NEEDS_REVIEW` is specifically required to remain non-active/non-cancellable while still fencing new restarts; this prevents unresolved destructive state from being silently bypassed.

### `CompanionRegistryTest`
Protects authenticated Paper-companion heartbeat eligibility:

- missing heartbeat is incompatible;
- missing required capability bits are incompatible;
- exactly-at-timeout is still accepted by the current contract;
- older-than-timeout is rejected;
- extra future capability bits remain forward-compatible;
- a newer authenticated heartbeat replaces the prior boot identity/freshness.

These are safety-critical because restart orchestration uses companion freshness/capabilities before destructive actions.

### `FullFeatureCoverageContractTest`
Inventory guard connecting the major production surfaces to concrete tests: wire encoding, authenticated control, Paper delivery/execution, companion identity, plan state, drain/rejoin, preflight, network handoff/recovery, countdown/orchestration, commands/schedules, domain rules, persistence/config/scheduling, Pterodactyl execution, plugin messaging, proxy/queue adapters and architecture boundaries.

This is not a substitute for behavior assertions. Add a real regression test first; update the inventory second.

## Running tests

Use the checked-in Gradle wrapper.

Full repository validation:

```bash
./gradlew clean test build
```

Windows:

```powershell
.\gradlew.bat clean test build
```

Focused Velocity tests:

```bash
./gradlew :velocity:test --tests 'com.badgersmc.queuerestart.velocity.domain.plan.RestartPlanStateTest'
./gradlew :velocity:test --tests 'com.badgersmc.queuerestart.velocity.application.companion.CompanionRegistryTest'
./gradlew :velocity:test --tests 'com.badgersmc.queuerestart.velocity.FullFeatureCoverageContractTest'
```

Common/Paper companion suites can be run independently with `:common:test` and `:paper-companion:test`.

## Result locations

Per-module Gradle reports include:

- `common/build/reports/tests/test/index.html`
- `paper-companion/build/reports/tests/test/index.html`
- `velocity/build/reports/tests/test/index.html`

JUnit XML is under each module's `build/test-results/test/` directory. Hosted GitHub Actions evidence must be tied to the exact PR head SHA.

## How to review failures

1. **State-policy failure** — treat a changed active/cancellable/blocking result as a restart-safety change. Confirm the intended transition before changing the expected set.
2. **Companion compatibility failure** — inspect authentication, boot identity, capability bits and timeout semantics. Never make stale/incomplete companions eligible merely to obtain green CI.
3. **Coverage inventory failure** — locate the real replacement regression evidence or add the missing behavior test; do not point to unrelated files.
4. **Build/harness failure** — fix Gradle/dependency/test infrastructure without weakening product safety rules.
5. **Sentinel boundary** — built plugin lifecycle/simulation failure belongs to Sentinel evidence and may require real Paper when MockBukkit cannot model the behavior.
6. **Zero-step/no-runner CI** — infrastructure failure, not a pass and not a reason to modify product code.

## What new restart behavior should test

Depending on the change, cover:

- duplicate/replayed authenticated messages;
- stale heartbeat and wrong boot identity;
- missing capability bits;
- restart plan state and cancellation boundaries;
- overlapping restart fencing;
- drain/transfer failure and rollback/recovery;
- player rejoin behavior;
- schedule/time crossing and countdown warnings;
- crash/restart persistence and `NEEDS_REVIEW` recovery;
- queue and proxy provider failures;
- Pterodactyl/control-plane failure;
- plugin-message acknowledgement/idempotency;
- shutdown/closed-resource behavior.

Do not use live production Pterodactyl credentials, control secrets, server IDs tied to private infrastructure, or production player data in tests. Generate fake identities and use temporary files/fakes.

## Sentinel and live-server boundary

Repository tests prove deterministic logic and adapters. Sentinel can prove built artifact/lifecycle compatibility. Real Paper/Velocity or the documented E2E harness is required for behavior that depends on actual proxy/backend networking, server process replacement, real CheckHacks integration, or production-like control-plane timing.

## Worker coordination

Reconcile live `main`, open PRs and changed paths before editing. This hardening branch is test/documentation-only. Product defects discovered here should be repaired in the appropriate owning branch, not mixed into unrelated test-hardening changes.
