from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    if old not in text:
        raise SystemExit(f"expected block not found in {path}: {old[:120]!r}")
    file.write_text(text.replace(old, new, 1))


replace_once(
    "velocity/src/main/kotlin/com/badgersmc/queuerestart/velocity/application/ports/NetworkRestartPorts.kt",
    """    /** False for validation-only executors that must not move or disconnect players. */
    val performsPowerActions: Boolean get() = true
    fun preflight(panelServerId: String): CompletionStage<PowerActionResult>
""",
    """    /** False for validation-only executors that must not move or disconnect players. */
    val performsPowerActions: Boolean get() = true

    /** Stable executor instance for one destructive plan execution. */
    fun snapshot(): ExternalRestartExecutor = this

    fun preflight(panelServerId: String): CompletionStage<PowerActionResult>
""",
)

replace_once(
    "velocity/src/main/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/executor/PterodactylRestartExecutor.kt",
    """    override val name: String get() = delegate().name
    override val performsPowerActions: Boolean get() = delegate().performsPowerActions

    override fun preflight""",
    """    override val name: String get() = delegate().name
    override val performsPowerActions: Boolean get() = delegate().performsPowerActions
    override fun snapshot(): ExternalRestartExecutor = delegate()

    override fun preflight""",
)

service = "velocity/src/main/kotlin/com/badgersmc/queuerestart/velocity/application/network/NetworkRestartService.kt"
replace_once(
    service,
    """        .filter { it.state == PlanState.COMPLETED && it.completedAt != null && it.type in setOf(PlanType.PROXY, PlanType.NETWORK) }
""",
    """        .filter {
            it.state == PlanState.COMPLETED && it.completedAt != null &&
                it.type in setOf(PlanType.PROXY, PlanType.NETWORK) && !it.isDryRunCompletion()
        }
""",
)
replace_once(
    service,
    """                it.completedAt != null && it.type in setOf(PlanType.SERVER, PlanType.NETWORK)
""",
    """                it.completedAt != null && it.type in setOf(PlanType.SERVER, PlanType.NETWORK) &&
                !it.isDryRunCompletion()
""",
)
replace_once(
    service,
    """        val cfg = config()
        val ids = when (plan.type) {
            PlanType.PROXY -> listOf(cfg.proxyServerId)
            PlanType.NETWORK -> cfg.members.map { cfg.serverIds.getValue(it) } + cfg.proxyServerId
            PlanType.SERVER -> emptyList()
        }
        if (!executor.performsPowerActions) {
            completeDryRun(plan)
            return
        }
        CompletableFuture.allOf(*ids.map { id ->
            executor.preflight(id).toCompletableFuture().thenAccept { result ->
                if (!result.accepted) throw IllegalStateException(result.detail)
            }
        }.toTypedArray())
            .whenComplete { _, error ->
                if (error != null) fail(plan, "preflight failed: ${rootMessage(error)}")
                else if (plan.type == PlanType.PROXY) executeProxy(plan) else executeNetwork(plan)
            }
""",
    """        val cfg = config()
        val executionExecutor = executor.snapshot()
        val ids = when (plan.type) {
            PlanType.PROXY -> listOf(cfg.proxyServerId)
            PlanType.NETWORK -> cfg.members.map { cfg.serverIds.getValue(it) } + cfg.proxyServerId
            PlanType.SERVER -> emptyList()
        }
        if (!executionExecutor.performsPowerActions) {
            completeDryRun(plan, cfg, executionExecutor.name)
            return
        }
        CompletableFuture.allOf(*ids.map { id ->
            executionExecutor.preflight(id).toCompletableFuture().thenAccept { result ->
                if (!result.accepted) throw IllegalStateException(result.detail)
            }
        }.toTypedArray())
            .whenComplete { _, error ->
                if (error != null) fail(plan, "preflight failed: ${rootMessage(error)}")
                else if (plan.type == PlanType.PROXY) executeProxy(plan, cfg, executionExecutor)
                else executeNetwork(plan, cfg, executionExecutor)
            }
""",
)
replace_once(
    service,
    """    private fun executeProxy(plan: RestartPlan) {
        val cfg = config()
""",
    """    private fun executeProxy(
        plan: RestartPlan,
        cfg: NetworkRestartConfig,
        executionExecutor: ExternalRestartExecutor,
    ) {
""",
)
replace_once(
    service,
    """        dispatch(plan, "${plan.id}:proxy", cfg.proxyServerId).whenComplete { result, error ->
            if (error != null || !result.accepted) fail(plan, error?.let(::rootMessage) ?: result.detail)
            else complete(plan, "proxy", result.detail)
        }
""",
    """        dispatch(plan, "${plan.id}:proxy", cfg.proxyServerId, executionExecutor).whenComplete { result, error ->
            if (error != null || !result.accepted) fail(plan, error?.let(::rootMessage) ?: result.detail)
            else complete(plan, "proxy", result.detail, executionExecutor.name)
        }
""",
)
replace_once(
    service,
    """    private fun executeNetwork(plan: RestartPlan) {
        val cfg = config()
""",
    """    private fun executeNetwork(
        plan: RestartPlan,
        cfg: NetworkRestartConfig,
        executionExecutor: ExternalRestartExecutor,
    ) {
""",
)
replace_once(
    service,
    """                restartBatch(plan, nonHubs)
""",
    """                restartBatch(plan, nonHubs, cfg, executionExecutor)
""",
)
replace_once(
    service,
    """                restartBatch(plan, cfg.hubServers)
            }
            .thenCompose { dispatch(plan, "${plan.id}:proxy", cfg.proxyServerId) }
""",
    """                restartBatch(plan, cfg.hubServers, cfg, executionExecutor)
            }
            .thenCompose { dispatch(plan, "${plan.id}:proxy", cfg.proxyServerId, executionExecutor) }
""",
)
replace_once(
    service,
    """                    else complete(plan, "network", "all configured actions accepted")
""",
    """                    else complete(plan, "network", "all configured actions accepted", executionExecutor.name)
""",
)
replace_once(
    service,
    """    private fun restartBatch(plan: RestartPlan, targets: List<ServerId>): CompletionStage<Void> {
        val cfg = config()
""",
    """    private fun restartBatch(
        plan: RestartPlan,
        targets: List<ServerId>,
        cfg: NetworkRestartConfig,
        executionExecutor: ExternalRestartExecutor,
    ): CompletionStage<Void> {
""",
)
replace_once(
    service,
    """                dispatch(plan, "${plan.id}:${target.value}", cfg.serverIds.getValue(target)).toCompletableFuture()
""",
    """                dispatch(
                    plan,
                    "${plan.id}:${target.value}",
                    cfg.serverIds.getValue(target),
                    executionExecutor,
                ).toCompletableFuture()
""",
)
replace_once(
    service,
    """    private fun dispatch(plan: RestartPlan, actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
        if (!plan.dispatchedActionKeys.add(actionKey)) {
            return CompletableFuture.completedFuture(PowerActionResult(false, "duplicate action blocked"))
        }
        save()
        return executor.restart(actionKey, panelServerId)
    }

    private fun completeDryRun(plan: RestartPlan) {
        val cfg = config()
""",
    """    private fun dispatch(
        plan: RestartPlan,
        actionKey: String,
        panelServerId: String,
        executionExecutor: ExternalRestartExecutor,
    ): CompletionStage<PowerActionResult> {
        if (!plan.dispatchedActionKeys.add(actionKey)) {
            return CompletableFuture.completedFuture(PowerActionResult(false, "duplicate action blocked"))
        }
        save()
        return executionExecutor.restart(actionKey, panelServerId)
    }

    private fun completeDryRun(
        plan: RestartPlan,
        cfg: NetworkRestartConfig,
        executorName: String,
    ) {
""",
)
replace_once(
    service,
    """        audit(plan, "completed via ${executor.name} without player disruption")
""",
    """        audit(plan, "completed via $executorName without player disruption")
""",
)
replace_once(
    service,
    """    private fun complete(plan: RestartPlan, target: String, detail: String) {
""",
    """    private fun complete(
        plan: RestartPlan,
        target: String,
        detail: String,
        executorName: String,
    ) {
""",
)
replace_once(
    service,
    """        audit(plan, "completed via ${executor.name}")
""",
    """        audit(plan, "completed via $executorName")
""",
)
replace_once(
    service,
    """    @Synchronized private fun save() = store.save(plans.values)
    private fun rootMessage(error: Throwable): String = generateSequence(error) { it.cause }.last().message ?: error.javaClass.simpleName
""",
    """    private fun RestartPlan.isDryRunCompletion(): Boolean =
        targetResults.isNotEmpty() && targetResults.values.all { it.startsWith("dry-run:") }

    @Synchronized private fun save() = store.save(plans.values)
    private fun rootMessage(error: Throwable): String = generateSequence(error) { it.cause }.last().message ?: error.javaClass.simpleName
""",
)

test = "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/network/NetworkRestartServiceTest.kt"
replace_once(
    test,
    """        assertThat(control.maintenanceEnables).isZero()
        assertThat(executor.calls).isZero()
    }

    @Test
    fun `rejected backend restart aborts before hub disconnect and proxy restart`() {
""",
    """        assertThat(control.maintenanceEnables).isZero()
        assertThat(executor.calls).isZero()
        assertThat(service.lastCompletedProxyRestart()).isNull()
        assertThat(service.lastCompletedServerRestart(smp)).isNull()
    }

    @Test
    fun `executor snapshot stays stable when configuration changes during preflight`() {
        val control = FakeControl()
        val executor = ReloadDuringPreflightExecutor()
        val service = service(control, executor)
        val now = Instant.now()
        val plan = service.createManual(
            PlanType.PROXY,
            emptySet(),
            now.plusSeconds(1),
            now,
            "reload race",
            "console",
            false,
        )

        service.tick(now.plusSeconds(2))

        assertThat(plan.state).isEqualTo(PlanState.COMPLETED)
        assertThat(executor.realRestartCalls).isEqualTo(1)
        assertThat(executor.dryRunRestartCalls).isZero()
    }

    @Test
    fun `rejected backend restart aborts before hub disconnect and proxy restart`() {
""",
)
replace_once(
    test,
    """    private class SelectiveExecutor(private val rejectedPanelId: String) : ExternalRestartExecutor {
""",
    """    private class ReloadDuringPreflightExecutor : ExternalRestartExecutor {
        var realRestartCalls = 0
        var dryRunRestartCalls = 0

        private val dryRun = object : ExternalRestartExecutor {
            override val name = "DRY_RUN"
            override val performsPowerActions = false
            override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
                CompletableFuture.completedFuture(PowerActionResult(true, "dry-run"))
            override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
                dryRunRestartCalls++
                return CompletableFuture.completedFuture(PowerActionResult(true, "dry-run"))
            }
        }

        private val real = object : ExternalRestartExecutor {
            override val name = "PTERODACTYL"
            override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> {
                current = dryRun
                return CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
            }
            override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
                realRestartCalls++
                return CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
            }
        }

        @Volatile private var current: ExternalRestartExecutor = real
        override val name: String get() = current.name
        override val performsPowerActions: Boolean get() = current.performsPowerActions
        override fun snapshot(): ExternalRestartExecutor = current
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> = current.preflight(panelServerId)
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
            current.restart(actionKey, panelServerId)
    }

    private class SelectiveExecutor(private val rejectedPanelId: String) : ExternalRestartExecutor {
""",
)

replace_once(
    "docs/network-restarts.md",
    """`DRY_RUN` is intentionally non-disruptive. It validates scheduling and
countdown behavior, records a completed dry-run plan, and performs no player or
server lifecycle actions.
""",
    """`DRY_RUN` is intentionally non-disruptive. It validates scheduling and
countdown behavior, records a clearly marked dry-run result, and performs no
player or server lifecycle actions. Dry-run records are excluded from
`/lastrestart` history.
""",
)
replace_once(
    "docs/network-restarts.md",
    """If preflight fails, no power actions are sent. If a non-hub backend restart is
rejected after preflight, the sequence stops before hubs are disconnected or
Velocity is restarted. Any earlier accepted action is recorded and is never
retried automatically. After an accepted proxy restart request, the old proxy
keeps its login maintenance gate until it exits; if it does not exit, the gate
expires after `maintenance-failure-expiry-seconds`.
""",
    """If preflight fails, no power actions are sent. Executor and target mappings are
snapshotted when execution begins, so `/qrestart reload` cannot switch an
in-flight plan between DRY_RUN and PTERODACTYL or redirect later actions. If a
non-hub backend restart is rejected after preflight, the sequence stops before
hubs are disconnected or Velocity is restarted. Any earlier accepted action is
recorded and is never retried automatically. After an accepted proxy restart
request, the old proxy keeps its login maintenance gate until it exits; if it
does not exit, the gate expires after `maintenance-failure-expiry-seconds`.
""",
)
