package com.badgersmc.queuerestart.velocity.application.network

import com.badgersmc.queuerestart.velocity.application.ports.ConfiguredRestartSchedule
import com.badgersmc.queuerestart.velocity.application.ports.ExternalRestartExecutor
import com.badgersmc.queuerestart.velocity.application.ports.NetworkControlPort
import com.badgersmc.queuerestart.velocity.application.ports.NetworkRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.PowerActionResult
import com.badgersmc.queuerestart.velocity.application.ports.RestartNotice
import com.badgersmc.queuerestart.velocity.application.ports.RestartPlanStore
import com.badgersmc.queuerestart.velocity.application.schedule.SchedCommandResult
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.plan.PlanState
import com.badgersmc.queuerestart.velocity.domain.plan.PlanType
import com.badgersmc.queuerestart.velocity.domain.plan.RestartPlan
import com.badgersmc.queuerestart.velocity.domain.plan.RestartTimes
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class NetworkRestartService(
    private val config: () -> NetworkRestartConfig,
    private val schedules: () -> List<ConfiguredRestartSchedule>,
    private val executor: ExternalRestartExecutor,
    private val control: NetworkControlPort,
    private val store: RestartPlanStore,
    private val backendArm: (ServerId, Int, Boolean) -> SchedCommandResult,
    private val backendCancel: (ServerId) -> Unit,
    private val audit: (RestartPlan, String) -> Unit,
) {
    private val plans = ConcurrentHashMap<UUID, RestartPlan>()

    init { recover() }

    fun createManual(
        type: PlanType,
        targets: Set<ServerId>,
        executionAt: Instant,
        warningAt: Instant,
        reason: String,
        creator: String,
        silent: Boolean,
    ): RestartPlan = schedule(
        RestartPlan(
            type = type,
            targets = targets,
            createdAt = Instant.now(),
            executionAt = executionAt,
            warningAt = warningAt,
            reason = reason,
            creator = creator,
            silent = silent,
        ),
    )

    fun triggerConfiguredSchedule(name: String): RestartPlan? {
        val definition = schedules().firstOrNull { it.name == name && it.enabled } ?: return null
        val now = Instant.now()
        val type = PlanType.valueOf(definition.type)
        val targets = if (type == PlanType.NETWORK) config().members.toSet() else definition.targets.toSet()
        return createManual(
            type = type,
            targets = targets,
            executionAt = now.plusSeconds(definition.warningWindowSeconds),
            warningAt = now,
            reason = definition.reason,
            creator = "TRIGGER:$name",
            silent = definition.silent,
        )
    }

    @Synchronized fun schedule(plan: RestartPlan): RestartPlan {
        validate(plan)
        val conflict = plans.values.firstOrNull { it.active() && conflicts(it, plan) }
        if (conflict != null) {
            if (plan.automaticKey != null || conflict.automaticKey == null || conflict.state !in setOf(PlanState.SCHEDULED, PlanState.COUNTING_DOWN)) {
                throw IllegalArgumentException("conflicts with active plan ${conflict.id}")
            }
            cancel(conflict, "automatic plan replaced by manual plan ${plan.id}")
        }
        plans[plan.id] = plan
        save()
        audit(plan, "created by ${plan.creator}")
        return plan
    }

    fun activePublicPlans(): List<RestartPlan> = plans.values
        .filter { it.active() && !it.silent }
        .sortedBy(RestartPlan::executionAt)

    fun allPlans(): List<RestartPlan> = plans.values.sortedBy(RestartPlan::executionAt)

    /** Most recent completed restart that included the Velocity proxy. */
    fun lastCompletedProxyRestart(): RestartPlan? = plans.values
        .asSequence()
        .filter {
            it.state == PlanState.COMPLETED && it.completedAt != null &&
                it.type in setOf(PlanType.PROXY, PlanType.NETWORK) && !it.isDryRunCompletion()
        }
        .maxByOrNull { it.completedAt!! }

    /** Most recent completed restart that included [target]. */
    fun lastCompletedServerRestart(target: ServerId): RestartPlan? = plans.values
        .asSequence()
        .filter {
            it.state == PlanState.COMPLETED &&
                target in it.targets &&
                it.completedAt != null && it.type in setOf(PlanType.SERVER, PlanType.NETWORK) &&
                !it.isDryRunCompletion()
        }
        .maxByOrNull { it.completedAt!! }

    @Synchronized fun cancel(prefix: String): Boolean {
        val plan = plans.values.firstOrNull { it.cancellable() && it.id.toString().startsWith(prefix) } ?: return false
        cancel(plan)
        return true
    }

    @Synchronized fun cancel(type: PlanType): Boolean {
        val plan = plans.values.firstOrNull { it.cancellable() && it.type == type } ?: return false
        cancel(plan)
        return true
    }

    @Synchronized fun cancel(target: ServerId): Boolean {
        val plan = plans.values.firstOrNull {
            it.cancellable() && it.type == PlanType.SERVER && target in it.targets
        } ?: return false
        cancel(plan)
        return true
    }

    private fun cancel(plan: RestartPlan, auditEvent: String = "cancelled") {
        plan.state = PlanState.CANCELLED
        if (plan.type == PlanType.SERVER) plan.targets.firstOrNull()?.let(backendCancel)
        if (!plan.silent) cancellation(plan)
        audit(plan, auditEvent)
        save()
    }

    fun tick(now: Instant) {
        createAutomaticPlans(now)
        plans.values.filter(RestartPlan::active).forEach { plan ->
            try {
                refreshMaintenance(plan)
                tickPlan(plan, now)
            } catch (error: Exception) {
                fail(plan, error.message ?: error.javaClass.simpleName)
            }
        }
    }

    private fun tickPlan(plan: RestartPlan, now: Instant) {
        if (now.isBefore(plan.warningAt)) return
        if (plan.state == PlanState.SCHEDULED) {
            if (plan.type == PlanType.SERVER) {
                val seconds = Duration.between(now, plan.executionAt).seconds.coerceAtLeast(1).toInt()
                when (val result = backendArm(plan.targets.single(), seconds, plan.silent)) {
                    is SchedCommandResult.Rejected -> return fail(plan, result.reason)
                    else -> {}
                }
            } else if (!plan.silent) {
                val remaining = Duration.between(now, plan.executionAt).seconds.coerceAtLeast(0)
                plan.announcedSeconds += config().announcementPointsSeconds.filter { remaining <= it }
                announcement(plan, remaining)
            }
            plan.state = PlanState.COUNTING_DOWN
            audit(plan, "countdown started")
            save()
        }
        val remaining = Duration.between(now, plan.executionAt).seconds.coerceAtLeast(0)
        if (plan.type != PlanType.SERVER && !plan.silent) announceDue(plan, remaining)
        if (remaining == 0L && plan.type == PlanType.SERVER) {
            plan.completedAt = now
            plan.state = PlanState.COMPLETED
            save()
        } else if (remaining == 0L) {
            execute(plan)
        }
    }

    private fun announceDue(plan: RestartPlan, remaining: Long) {
        val cfg = config()
        if (remaining in 1..cfg.finalCountdownSeconds.toLong() && plan.announcedSeconds.add(remaining)) {
            control.broadcast(notice(plan, remaining, urgent = true))
            save()
            return
        }
        val due = cfg.announcementPointsSeconds.filter { remaining <= it && it !in plan.announcedSeconds }
        if (due.isNotEmpty()) {
            plan.announcedSeconds += due
            announcement(plan, remaining)
            save()
        }
    }

    private fun announcement(plan: RestartPlan, remaining: Long) = control.broadcast(notice(plan, remaining, false))

    private fun notice(plan: RestartPlan, remaining: Long, urgent: Boolean): RestartNotice {
        val time = RestartTimes.format(Duration.ofSeconds(remaining))
        return when (plan.type) {
            PlanType.PROXY -> if (urgent) {
                RestartNotice("NETWORK", "NETWORK RESTART", "Disconnecting everyone in $time...", "", plan.reason, true)
            } else {
                RestartNotice("NETWORK", "NETWORK RESTART", "The network proxy will restart in $time.", "All players will be fully disconnected.", plan.reason)
            }
            PlanType.NETWORK -> if (urgent) {
                RestartNotice("NETWORK", "FULL NETWORK RESTART", "Full network restart in $time...", "", plan.reason, true)
            } else {
                RestartNotice("NETWORK", "FULL NETWORK RESTART", "The entire network will restart in $time.", "Servers may be temporarily unavailable.", plan.reason)
            }
            PlanType.SERVER -> RestartNotice("SERVER", "SCHEDULED RESTART", "${plan.targets.single().value} restarts in $time.", "Players will be moved to the hub.", plan.reason, urgent)
        }
    }

    @Synchronized private fun execute(plan: RestartPlan) {
        if (plan.actionStarted || plan.state != PlanState.COUNTING_DOWN) return
        plan.actionStarted = true
        plan.state = PlanState.PREFLIGHT
        save()
        val cfg = config()
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
                try {
                    if (error != null) fail(plan, "preflight failed: ${rootMessage(error)}")
                    else if (plan.type == PlanType.PROXY) executeProxy(plan, cfg, executionExecutor)
                    else executeNetwork(plan, cfg, executionExecutor)
                } catch (dispatchError: Exception) {
                    fail(plan, rootMessage(dispatchError))
                }
            }
    }

    private fun executeProxy(
        plan: RestartPlan,
        cfg: NetworkRestartConfig,
        executionExecutor: ExternalRestartExecutor,
    ) {
        enableMaintenance(plan, cfg)
        plan.state = PlanState.DISPATCHING
        save()
        if (!plan.silent) control.broadcast(RestartNotice("NETWORK", "RESTARTING NOW", "The Velocity proxy is restarting.", "All players are being disconnected.", plan.reason))
        control.disconnectAll(RestartNotice("NETWORK", "Network restarting", "The Velocity proxy is restarting.", "Please reconnect shortly.", plan.reason, true))
        dispatch(plan, "${plan.id}:proxy", cfg.proxyServerId, executionExecutor).whenComplete { result, error ->
            if (error != null || !result.accepted) fail(plan, error?.let(::rootMessage) ?: result.detail)
            else complete(plan, "proxy", result.detail, executionExecutor.name)
        }
    }

    private fun executeNetwork(
        plan: RestartPlan,
        cfg: NetworkRestartConfig,
        executionExecutor: ExternalRestartExecutor,
    ) {
        enableMaintenance(plan, cfg)
        plan.state = PlanState.TRANSFERRING
        save()
        val nonHubs = cfg.members.filterNot(cfg.hubServers::contains)
        val transfers = nonHubs.map { control.transferAll(it, cfg.hubServers).toCompletableFuture() }
        CompletableFuture.allOf(*transfers.toTypedArray())
            .orTimeout(cfg.transferTimeoutSeconds, TimeUnit.SECONDS)
            .thenCompose {
                plan.state = PlanState.DISPATCHING; save()
                restartBatch(plan, nonHubs, cfg, executionExecutor)
            }
            .thenCompose {
                CompletableFuture.runAsync({}, CompletableFuture.delayedExecutor(cfg.backendHeadStartSeconds, TimeUnit.SECONDS))
            }
            .thenCompose {
                if (!plan.silent) control.broadcast(RestartNotice("NETWORK", "RESTARTING NOW", "The entire network is restarting.", "All players are being disconnected.", plan.reason))
                control.disconnectAll(RestartNotice("NETWORK", "Full network restart", "The entire Minecraft network is restarting.", "Please reconnect shortly.", plan.reason, true))
                restartBatch(plan, cfg.hubServers, cfg, executionExecutor)
            }
            .thenCompose { dispatch(plan, "${plan.id}:proxy", cfg.proxyServerId, executionExecutor) }
            .whenComplete { proxyResult, error ->
                if (error != null) fail(plan, rootMessage(error))
                else {
                    plan.targetResults["proxy"] = proxyResult.detail
                    if (plan.targetResults.values.any { it.startsWith("FAILED") } || !proxyResult.accepted) fail(plan, "one or more restart actions failed")
                    else complete(plan, "network", "all configured actions accepted", executionExecutor.name)
                }
            }
    }

    private fun restartBatch(
        plan: RestartPlan,
        targets: List<ServerId>,
        cfg: NetworkRestartConfig,
        executionExecutor: ExternalRestartExecutor,
    ): CompletionStage<Void> {
        val groups = targets.chunked(cfg.maxConcurrentActions)
        var stage: CompletionStage<Void> = CompletableFuture.completedFuture(null)
        for (group in groups) stage = stage.thenCompose {
            val requests = group.associateWith { target ->
                dispatch(
                    plan,
                    "${plan.id}:${target.value}",
                    cfg.serverIds.getValue(target),
                    executionExecutor,
                ).toCompletableFuture()
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

    private fun enableMaintenance(plan: RestartPlan, cfg: NetworkRestartConfig) {
        control.setMaintenance(true, Duration.ofSeconds(cfg.maintenanceFailureExpirySeconds))
        plan.maintenanceEnabled = true
        save()
    }

    private fun refreshMaintenance(plan: RestartPlan) {
        if (!plan.maintenanceEnabled || plan.state !in setOf(PlanState.PREFLIGHT, PlanState.TRANSFERRING, PlanState.DISPATCHING)) return
        control.setMaintenance(true, Duration.ofSeconds(config().maintenanceFailureExpirySeconds))
    }

    private fun dispatch(
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
        when (plan.type) {
            PlanType.PROXY -> plan.targetResults["proxy"] = "dry-run: no power action sent"
            PlanType.NETWORK -> {
                cfg.members.forEach { plan.targetResults[it.value] = "dry-run: no power action sent" }
                plan.targetResults["proxy"] = "dry-run: no power action sent"
            }
            PlanType.SERVER -> Unit
        }
        plan.dryRun = true
        plan.completedAt = Instant.now()
        plan.state = PlanState.COMPLETED
        plan.maintenanceEnabled = false
        audit(plan, "completed via $executorName without player disruption")
        save()
    }

    private fun complete(
        plan: RestartPlan,
        target: String,
        detail: String,
        executorName: String,
    ) {
        plan.targetResults[target] = detail
        plan.completedAt = Instant.now()
        plan.state = PlanState.COMPLETED
        // Keep the login gate active until this process actually exits. The
        // replacement proxy starts with a fresh control adapter, and if the
        // accepted panel action does not stop this process the gate expires
        // naturally after maintenance-failure-expiry-seconds.
        plan.maintenanceEnabled = false
        audit(plan, "completed via $executorName")
        save()
    }

    private fun fail(plan: RestartPlan, detail: String) {
        plan.failure = detail
        plan.state = PlanState.FAILED
        control.setMaintenance(false, Duration.ZERO)
        plan.maintenanceEnabled = false
        audit(plan, "failed: $detail")
        save()
    }

    private fun cancellation(plan: RestartPlan) = control.broadcast(
        RestartNotice(if (plan.type == PlanType.SERVER) "SERVER" else "NETWORK", "RESTART CANCELLED", "The scheduled ${plan.type.name.lowercase()} restart was cancelled.", "", ""),
    )

    private fun createAutomaticPlans(now: Instant) {
        if (!config().enabled) return
        schedules().filter { it.enabled }.forEach { def ->
            val execution = nextOccurrence(def, now)
            val key = "${def.name}@$execution"
            if (plans.values.any { it.automaticKey == key }) return@forEach
            val warning = execution.minusSeconds(def.warningWindowSeconds)
            if (now.isBefore(warning) || !now.isBefore(execution)) return@forEach
            val type = PlanType.valueOf(def.type)
            try {
                schedule(RestartPlan(type = type, targets = if (type == PlanType.NETWORK) config().members.toSet() else def.targets.toSet(), createdAt = now, executionAt = execution, warningAt = warning, reason = def.reason, creator = "AUTOMATIC:${def.name}", automaticKey = key, silent = def.silent))
            } catch (_: IllegalArgumentException) { /* conflict intentionally suppresses this occurrence */ }
        }
    }

    fun nextOccurrence(def: ConfiguredRestartSchedule, now: Instant): Instant {
        val zone = ZoneId.of(def.timezone)
        val time = LocalTime.parse(def.time)
        val today = LocalDate.ofInstant(now, zone)
        for (offset in 0..7) {
            val candidate = ZonedDateTime.of(today.plusDays(offset.toLong()), time, zone)
            if (candidate.toInstant().isAfter(now) && (def.days.isEmpty() || candidate.dayOfWeek.name in def.days)) return candidate.toInstant()
        }
        throw IllegalArgumentException("schedule '${def.name}' has no upcoming occurrence")
    }

    private fun validate(plan: RestartPlan) {
        require(plan.executionAt.isAfter(Instant.now())) { "execution time must be in the future" }
        val cfg = config()
        require(cfg.enabled) { "network-restart is disabled in config.yml" }
        when (plan.type) {
            PlanType.SERVER -> require(plan.targets.size == 1 && cfg.serverIds.containsKey(plan.targets.single())) {
                "server restart requires one configured target"
            }
            PlanType.PROXY -> require(cfg.proxyServerId.isNotBlank()) { "proxy panel identifier is missing" }
            PlanType.NETWORK -> {
                require(cfg.members.isNotEmpty()) { "full-network members are empty" }
                require(cfg.members.all(cfg.serverIds::containsKey)) { "full-network mapping is incomplete" }
                require(cfg.proxyServerId.isNotBlank()) { "proxy panel identifier is missing" }
            }
        }
    }

    private fun conflicts(left: RestartPlan, right: RestartPlan): Boolean =
        if (left.type != PlanType.SERVER || right.type != PlanType.SERVER) true else left.targets.any(right.targets::contains)

    private fun recover() {
        val now = Instant.now()
        store.load().forEach { plan ->
            if (plan.state in setOf(PlanState.PREFLIGHT, PlanState.TRANSFERRING, PlanState.DISPATCHING) || plan.actionStarted) {
                plan.state = PlanState.NEEDS_REVIEW
                plan.failure = "execution was interrupted; destructive actions were not replayed"
            } else if (plan.active() && !plan.executionAt.isAfter(now)) {
                plan.state = PlanState.MISSED
            }
            plans[plan.id] = plan
        }
        control.setMaintenance(false, Duration.ZERO)
        save()
    }

    private fun RestartPlan.isDryRunCompletion(): Boolean = dryRun

    @Synchronized private fun save() = store.save(plans.values)
    private fun rootMessage(error: Throwable): String = generateSequence(error) { it.cause }.last().message ?: error.javaClass.simpleName
}
