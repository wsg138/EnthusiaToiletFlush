from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    if old not in text:
        raise SystemExit(f"expected block not found in {path}: {old[:120]!r}")
    file.write_text(text.replace(old, new, 1))


replace_once(
    "velocity/src/main/kotlin/com/badgersmc/queuerestart/velocity/application/ports/NetworkRestartPorts.kt",
    "interface ExternalRestartExecutor {\n    val name: String\n",
    "interface ExternalRestartExecutor {\n    val name: String\n\n    /** False for validation-only executors that must not move or disconnect players. */\n    val performsPowerActions: Boolean get() = true\n",
)

replace_once(
    "velocity/src/main/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/executor/PterodactylRestartExecutor.kt",
    "class DryRunRestartExecutor : ExternalRestartExecutor {\n    override val name: String = \"DRY_RUN\"\n",
    "class DryRunRestartExecutor : ExternalRestartExecutor {\n    override val name: String = \"DRY_RUN\"\n    override val performsPowerActions: Boolean = false\n",
)
replace_once(
    "velocity/src/main/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/executor/PterodactylRestartExecutor.kt",
    "    override val name: String get() = delegate().name\n\n    override fun preflight",
    "    override val name: String get() = delegate().name\n    override val performsPowerActions: Boolean get() = delegate().performsPowerActions\n\n    override fun preflight",
)

service = "velocity/src/main/kotlin/com/badgersmc/queuerestart/velocity/application/network/NetworkRestartService.kt"
replace_once(
    service,
    """        val ids = when (plan.type) {
            PlanType.PROXY -> listOf(cfg.proxyServerId)
            PlanType.NETWORK -> cfg.members.map { cfg.serverIds.getValue(it) } + cfg.proxyServerId
            PlanType.SERVER -> emptyList()
        }
        CompletableFuture.allOf(*ids.map { id ->
""",
    """        val ids = when (plan.type) {
            PlanType.PROXY -> listOf(cfg.proxyServerId)
            PlanType.NETWORK -> cfg.members.map { cfg.serverIds.getValue(it) } + cfg.proxyServerId
            PlanType.SERVER -> emptyList()
        }
        if (!executor.performsPowerActions) {
            completeDryRun(plan)
            return
        }
        CompletableFuture.allOf(*ids.map { id ->
""",
)
replace_once(
    service,
    """    private fun restartBatch(plan: RestartPlan, targets: List<ServerId>): CompletionStage<Void> {
        val cfg = config()
        val groups = targets.chunked(cfg.maxConcurrentActions)
        var stage: CompletionStage<Void> = CompletableFuture.completedFuture(null)
        for (group in groups) stage = stage.thenCompose {
            CompletableFuture.allOf(*group.map { target ->
                dispatch(plan, "${plan.id}:${target.value}", cfg.serverIds.getValue(target)).toCompletableFuture()
                    .thenAccept { result -> plan.targetResults[target.value] = if (result.accepted) result.detail else "FAILED: ${result.detail}" }
            }.toTypedArray()).thenRun(::save)
        }
        return stage
    }
""",
    """    private fun restartBatch(plan: RestartPlan, targets: List<ServerId>): CompletionStage<Void> {
        val cfg = config()
        val groups = targets.chunked(cfg.maxConcurrentActions)
        var stage: CompletionStage<Void> = CompletableFuture.completedFuture(null)
        for (group in groups) stage = stage.thenCompose {
            val requests = group.associateWith { target ->
                dispatch(plan, "${plan.id}:${target.value}", cfg.serverIds.getValue(target)).toCompletableFuture()
            }
            CompletableFuture.allOf(*requests.values.toTypedArray()).thenRun {
                val rejected = mutableListOf<String>()
                requests.forEach { (target, future) ->
                    val result = future.join()
                    plan.targetResults[target.value] = if (result.accepted) result.detail else "FAILED: ${result.detail}"
                    if (!result.accepted) rejected += "${target.value}: ${result.detail}"
                }
                save()
                check(rejected.isEmpty()) { "restart rejected for ${rejected.joinToString()}" }
            }
        }
        return stage
    }
""",
)
replace_once(
    service,
    """    private fun complete(plan: RestartPlan, target: String, detail: String) {
        plan.targetResults[target] = detail
        plan.completedAt = Instant.now()
        plan.state = PlanState.COMPLETED
        control.setMaintenance(false, Duration.ZERO)
        plan.maintenanceEnabled = false
        audit(plan, "completed via ${executor.name}")
        save()
    }
""",
    """    private fun completeDryRun(plan: RestartPlan) {
        val cfg = config()
        when (plan.type) {
            PlanType.PROXY -> plan.targetResults["proxy"] = "dry-run: no power action sent"
            PlanType.NETWORK -> {
                cfg.members.forEach { plan.targetResults[it.value] = "dry-run: no power action sent" }
                plan.targetResults["proxy"] = "dry-run: no power action sent"
            }
            PlanType.SERVER -> Unit
        }
        plan.completedAt = Instant.now()
        plan.state = PlanState.COMPLETED
        plan.maintenanceEnabled = false
        audit(plan, "completed via ${executor.name} without player disruption")
        save()
    }

    private fun complete(plan: RestartPlan, target: String, detail: String) {
        plan.targetResults[target] = detail
        plan.completedAt = Instant.now()
        plan.state = PlanState.COMPLETED
        // Keep the login gate active until this process actually exits. The
        // replacement proxy starts with a fresh control adapter, and if the
        // accepted panel action does not stop this process the gate expires
        // naturally after maintenance-failure-expiry-seconds.
        plan.maintenanceEnabled = false
        audit(plan, "completed via ${executor.name}")
        save()
    }
""",
)

test = "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/network/NetworkRestartServiceTest.kt"
replace_once(
    test,
    """        assertThat(events).containsSubsequence("disconnect", "restart:proxy")
        assertThat(control.maintenanceDisables).isGreaterThanOrEqualTo(2)
    }
""",
    """        assertThat(events).containsSubsequence("disconnect", "restart:proxy")
        assertThat(control.maintenanceEnables).isEqualTo(1)
        assertThat(control.maintenanceDisables).isEqualTo(1)
    }
""",
)
replace_once(
    test,
    """    @Test
    fun `last completed restart queries include proxy network and target history`() {
""",
    """    @Test
    fun `dry run completes without transfers disconnects maintenance or power requests`() {
        val control = FakeControl()
        val executor = NonDestructiveExecutor()
        val service = service(control, executor)
        val now = Instant.now()
        val plan = service.createManual(
            PlanType.NETWORK,
            setOf(hub, smp),
            now.plusSeconds(1),
            now,
            "validation",
            "console",
            false,
        )

        service.tick(now.plusSeconds(2))

        assertThat(plan.state).isEqualTo(PlanState.COMPLETED)
        assertThat(control.transfers).isZero()
        assertThat(control.disconnects).isZero()
        assertThat(control.maintenanceEnables).isZero()
        assertThat(executor.calls).isZero()
    }

    @Test
    fun `rejected backend restart aborts before hub disconnect and proxy restart`() {
        val control = FakeControl()
        val executor = SelectiveExecutor(rejectedPanelId = "smp1234")
        val service = service(control, executor)
        val now = Instant.now()
        val plan = service.createManual(
            PlanType.NETWORK,
            setOf(hub, smp),
            now.plusSeconds(1),
            now,
            "failure test",
            "console",
            false,
        )

        service.tick(now.plusSeconds(2))

        assertThat(plan.state).isEqualTo(PlanState.FAILED)
        assertThat(executor.restartIds).containsExactly("smp1234")
        assertThat(control.disconnects).isZero()
        assertThat(plan.failure).contains("SMP").contains("rejected")
    }

    @Test
    fun `last completed restart queries include proxy network and target history`() {
""",
)
replace_once(
    test,
    """    private class FailingExecutor : ExternalRestartExecutor {
        override val name = "failing"
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(false, "rejected"))
    }

    private fun nextDailyWarningStart(): Instant {
""",
    """    private class FailingExecutor : ExternalRestartExecutor {
        override val name = "failing"
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(false, "rejected"))
    }

    private class NonDestructiveExecutor : ExternalRestartExecutor {
        override val name = "DRY_RUN"
        override val performsPowerActions = false
        var calls = 0
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> {
            calls++
            return CompletableFuture.completedFuture(PowerActionResult(true, "unexpected"))
        }
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
            calls++
            return CompletableFuture.completedFuture(PowerActionResult(true, "unexpected"))
        }
    }

    private class SelectiveExecutor(private val rejectedPanelId: String) : ExternalRestartExecutor {
        override val name = "selective"
        val restartIds = mutableListOf<String>()
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
            restartIds += panelServerId
            val accepted = panelServerId != rejectedPanelId
            return CompletableFuture.completedFuture(PowerActionResult(accepted, if (accepted) "ok" else "rejected"))
        }
    }

    private fun nextDailyWarningStart(): Instant {
""",
)
replace_once(
    test,
    """        val broadcasts = mutableListOf<RestartNotice>()
        var maintenanceEnables = 0
        var maintenanceDisables = 0
        override fun broadcast(notice: RestartNotice) { broadcasts += notice }
        override fun disconnectAll(notice: RestartNotice) { events += "disconnect" }
        override fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary> =
            CompletableFuture.completedFuture(TransferSummary(0, 0, 0))
""",
    """        val broadcasts = mutableListOf<RestartNotice>()
        var maintenanceEnables = 0
        var maintenanceDisables = 0
        var disconnects = 0
        var transfers = 0
        override fun broadcast(notice: RestartNotice) { broadcasts += notice }
        override fun disconnectAll(notice: RestartNotice) { disconnects++; events += "disconnect" }
        override fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary> {
            transfers++
            return CompletableFuture.completedFuture(TransferSummary(0, 0, 0))
        }
""",
)

replace_once(
    "velocity/src/main/resources/config.yml",
    "  executor: DRY_RUN # DRY_RUN or PTERODACTYL\n",
    "  executor: DRY_RUN # DRY_RUN is non-disruptive; PTERODACTYL performs real restarts\n",
)
replace_once(
    ".github/workflows/release.yml",
    "          ./gradlew shadowJar --no-daemon -PreleaseVersion=\"$RELEASE_VERSION\"\n",
    "          ./gradlew check shadowJar --no-daemon -PreleaseVersion=\"$RELEASE_VERSION\"\n",
)

Path(".github/workflows/ci.yml").write_text("""name: checks

on:
  pull_request:
  push:
    branches:
      - main
      - agent/**

permissions:
  contents: read

jobs:
  check:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    steps:
      - uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683
      - uses: actions/setup-java@3a4f6e1af504cf6a31855fa899c6aa5355ba6c12
        with:
          distribution: temurin
          java-version: 21
          cache: gradle
      - name: Run tests, architecture checks, and package jars
        run: |
          chmod +x ./gradlew
          ./gradlew check shadowJar --no-daemon
""")
