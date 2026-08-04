package com.badgersmc.queuerestart.velocity.application.network

import com.badgersmc.queuerestart.velocity.application.ports.ConfiguredRestartSchedule
import com.badgersmc.queuerestart.velocity.application.ports.ExternalRestartExecutor
import com.badgersmc.queuerestart.velocity.application.ports.NetworkControlPort
import com.badgersmc.queuerestart.velocity.application.ports.NetworkRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.PowerActionResult
import com.badgersmc.queuerestart.velocity.application.ports.RestartNotice
import com.badgersmc.queuerestart.velocity.application.ports.RestartPlanStore
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
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

/**
 * Persistent authority for configured server, proxy, and full-network plans.
 *
 * A panel/API acceptance is not completion. Destructive plans remain in
 * [PlanState.DISPATCHING] until authenticated process identities prove that
 * every expected JVM was replaced. Any ambiguous interruption is fail-closed
 * as [PlanState.NEEDS_REVIEW] and is never replayed automatically.
 */
class NetworkRestartService(
    private val config: () -> NetworkRestartConfig,
    private val schedules: () -> List<ConfiguredRestartSchedule>,
    private val executor: ExternalRestartExecutor,
    private val control: NetworkControlPort,
    private val store: RestartPlanStore,
    private val backendArm: (ServerId, Int, Boolean) -> SchedCommandResult,
    private val backendCancel: (ServerId) -> Unit,
    private val audit: (RestartPlan, String) -> Unit,
    private val serverCancellationOwner: ((ServerId, Boolean) -> Unit)? = null,
    private val soundResolver: (Long) -> SoundCue? = { null },
    private val backendIdentity: (ServerId) -> UUID? = { null },
    private val prepareBackendHandoff: (ServerId, UUID) -> Boolean = { _, _ -> false },
    private val currentProxyBootId: UUID = UUID.randomUUID(),
    private val executionTimeout: () -> Duration = { Duration.ofMinutes(10) },
    private val serverReviewResolver: (ServerId) -> Unit = {},
) {
    private val plans = ConcurrentHashMap<UUID, RestartPlan>()

    init {
        recover()
    }

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

    @Synchronized
    fun schedule(plan: RestartPlan): RestartPlan {
        validate(plan)
        val conflict = plans.values.firstOrNull { it.blocksScheduling() && conflicts(it, plan) }
        if (conflict != null) {
            if (
                plan.automaticKey != null ||
                conflict.automaticKey == null ||
                conflict.state !in setOf(PlanState.SCHEDULED, PlanState.COUNTING_DOWN)
            ) {
                throw IllegalArgumentException("conflicts with unresolved plan ${conflict.id} (${conflict.state})")
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

    fun lastCompletedProxyRestart(): RestartPlan? = plans.values.asSequence()
        .filter {
            it.state == PlanState.COMPLETED && it.completedAt != null &&
                it.type in setOf(PlanType.PROXY, PlanType.NETWORK) && !it.dryRun
        }
        .maxByOrNull { it.completedAt!! }

    fun lastCompletedServerRestart(target: ServerId): RestartPlan? = plans.values.asSequence()
        .filter {
            it.state == PlanState.COMPLETED && target in it.targets && it.completedAt != null &&
                it.type in setOf(PlanType.SERVER, PlanType.NETWORK) && !it.dryRun
        }
        .maxByOrNull { it.completedAt!! }

    @Synchronized
    fun cancel(prefix: String): Boolean {
        val matches = plans.values.filter { it.cancellable() && it.id.toString().startsWith(prefix) }
        if (matches.size != 1) return false
        cancel(matches.single())
        return true
    }

    @Synchronized
    fun cancel(type: PlanType): Boolean {
        val matches = plans.values.filter { it.cancellable() && it.type == type }
        if (matches.size != 1) return false
        cancel(matches.single())
        return true
    }

    @Synchronized
    fun cancel(target: ServerId): Boolean {
        val matches = plans.values.filter {
            it.cancellable() && it.type == PlanType.SERVER && target in it.targets
        }
        if (matches.size != 1) return false
        cancel(matches.single())
        return true
    }

    private fun cancel(plan: RestartPlan, auditEvent: String = "cancelled") {
        plan.state = PlanState.CANCELLED
        if (plan.type == PlanType.SERVER) {
            plan.targets.singleOrNull()?.let { target ->
                val owner = serverCancellationOwner
                if (owner != null) {
                    owner(target, plan.silent)
                } else {
                    backendCancel(target)
                    if (!plan.silent) cancellation(plan)
                }
            }
        } else if (!plan.silent) {
            cancellation(plan)
        }
        audit(plan, auditEvent)
        save()
    }

    @Synchronized
    fun tick(now: Instant) {
        createAutomaticPlans(now)
        plans.values.filter { it.active() || it.state == PlanState.NEEDS_REVIEW }.forEach { plan ->
            try {
                refreshMaintenance(plan)
                tickPlan(plan, now)
            } catch (error: Exception) {
                fail(plan, error.message ?: error.javaClass.simpleName)
            }
        }
    }

    private fun tickPlan(plan: RestartPlan, now: Instant) {
        if (plan.state == PlanState.NEEDS_REVIEW) return
        if (plan.state == PlanState.DISPATCHING) {
            monitorExecution(plan, now)
            return
        }
        if (now.isBefore(plan.warningAt)) return

        val remaining = remainingSeconds(now, plan.executionAt)
        if (plan.state == PlanState.SCHEDULED) {
            if (plan.type == PlanType.SERVER) {
                if (remaining == 0L) {
                    miss(plan, "server countdown elapsed before its backend coordinator was restored")
                    return
                }
                when (val result = backendArm(plan.targets.single(), remaining.toInt(), plan.silent)) {
                    is SchedCommandResult.Armed -> plan.backendArmAccepted = true
                    is SchedCommandResult.Rejected -> {
                        fail(plan, result.reason)
                        return
                    }
                    else -> {
                        fail(plan, "backend arm did not return an armed result")
                        return
                    }
                }
            } else if (!plan.silent && remaining > 0L) {
                announcement(plan, remaining, urgent = false)
                if (remaining in countdownMarks(config())) plan.announcedSeconds += remaining
                soundResolver(remaining)?.let(control::playSound)
            }
            plan.lastObservedRemainingSeconds = remaining
            plan.state = PlanState.COUNTING_DOWN
            audit(plan, "countdown started")
            save()
        }

        val currentRemaining = remainingSeconds(now, plan.executionAt)
        if (plan.type != PlanType.SERVER && !plan.silent && currentRemaining > 0L) {
            announceDue(plan, currentRemaining)
        }
        if (plan.type == PlanType.SERVER) {
            if (currentRemaining == 0L && plan.state == PlanState.COUNTING_DOWN) {
                prepareServerExecution(plan, now)
            }
            if (plan.state == PlanState.DISPATCHING) monitorExecution(plan, now)
        } else if (currentRemaining == 0L) {
            executeExternal(plan, now)
        }
    }

    private fun prepareServerExecution(plan: RestartPlan, now: Instant) {
        if (!plan.backendArmAccepted) {
            fail(plan, "server execution time arrived without an accepted backend coordinator")
            return
        }
        val target = plan.targets.single()
        val baseline = backendIdentity(target)
        if (baseline == null || !prepareBackendHandoff(target, baseline)) {
            serverCancellationOwner?.invoke(target, plan.silent) ?: backendCancel(target)
            fail(plan, "no fresh authenticated companion identity was available at T-0")
            return
        }
        plan.baselineBootIds.clear()
        plan.baselineBootIds[target] = baseline
        plan.executionDeadlineAt = now.plus(executionTimeout())
        plan.state = PlanState.DISPATCHING
        plan.actionStarted = false
        audit(plan, "backend restart prepared with authenticated boot baseline $baseline")
        save()
    }

    /** Called synchronously by the orchestrator after publishing the idempotent control delivery. */
    @Synchronized
    fun markBackendHandoffPublished(target: ServerId, baselineBootId: UUID): Boolean {
        val plan = plans.values.firstOrNull {
            it.type == PlanType.SERVER && target in it.targets && it.state == PlanState.DISPATCHING
        } ?: return false
        if (plan.baselineBootIds[target] != baselineBootId) {
            requireReview(plan, "published backend handoff did not match the persisted boot baseline")
            return false
        }
        plan.actionStarted = true
        plan.targetResults[target.value] = "authenticated restart delivery published"
        audit(plan, "backend restart delivery published")
        save()
        return true
    }

    private fun monitorExecution(plan: RestartPlan, now: Instant) {
        if (!plan.actionStarted) {
            if (deadlineElapsed(plan, now)) {
                requireReview(plan, "restart handoff was prepared but publication was not durably confirmed")
            }
            return
        }

        if (plan.type != PlanType.SERVER) {
            val missing = expectedActionKeys(plan) - plan.acceptedActionKeys
            if (missing.isNotEmpty()) {
                if (deadlineElapsed(plan, now)) {
                    requireReview(
                        plan,
                        "restart action acceptance was not durably recorded for ${missing.sorted().joinToString()}",
                    )
                }
                return
            }
        }

        when (plan.type) {
            PlanType.SERVER -> {
                val target = plan.targets.single()
                val baseline = plan.baselineBootIds[target]
                    ?: return requireReview(plan, "server execution has no persisted boot baseline")
                val current = backendIdentity(target)
                if (current != null && current != baseline) {
                    completeVerified(plan, "authenticated backend boot identity changed")
                    return
                }
            }
            PlanType.PROXY -> {
                val baseline = plan.proxyBaselineBootId
                    ?: return requireReview(plan, "proxy execution has no persisted boot baseline")
                if (currentProxyBootId != baseline) {
                    completeVerified(plan, "Velocity process identity changed after accepted proxy action")
                    return
                }
            }
            PlanType.NETWORK -> {
                val proxyBaseline = plan.proxyBaselineBootId
                    ?: return requireReview(plan, "network execution has no persisted proxy boot baseline")
                val completeBackendBaseline = plan.baselineBootIds.keys == plan.targets
                val allBackendsChanged = completeBackendBaseline && plan.baselineBootIds.all { (target, baseline) ->
                    backendIdentity(target)?.let { it != baseline } == true
                }
                if (currentProxyBootId != proxyBaseline && allBackendsChanged) {
                    completeVerified(plan, "Velocity and every authenticated backend process identity changed")
                    return
                }
            }
        }

        if (deadlineElapsed(plan, now)) {
            requireReview(plan, "restart did not produce every expected authenticated boot identity before ${plan.executionDeadlineAt}")
        }
    }

    private fun deadlineElapsed(plan: RestartPlan, now: Instant): Boolean =
        plan.executionDeadlineAt?.let { !now.isBefore(it) } == true

    private fun announceDue(plan: RestartPlan, remaining: Long) {
        val previous = plan.lastObservedRemainingSeconds
        if (previous == null) {
            plan.lastObservedRemainingSeconds = remaining
            save()
            return
        }
        if (remaining >= previous) return

        val crossed = countdownMarks(config())
            .filter { it < previous && it >= remaining && it !in plan.announcedSeconds }
        plan.lastObservedRemainingSeconds = remaining
        if (crossed.isEmpty()) {
            save()
            return
        }

        plan.announcedSeconds += crossed
        val due = crossed.minOrNull() ?: return
        announcement(plan, due, due <= config().finalCountdownSeconds)
        soundResolver(due)?.let(control::playSound)
        save()
    }

    private fun announcement(plan: RestartPlan, remaining: Long, urgent: Boolean) =
        control.broadcast(notice(plan, remaining, urgent))

    private fun countdownMarks(cfg: NetworkRestartConfig): Set<Long> =
        (cfg.announcementPointsSeconds + (1..cfg.finalCountdownSeconds).map(Int::toLong)).toSet()

    private fun remainingSeconds(now: Instant, executionAt: Instant): Long {
        val millis = Duration.between(now, executionAt).toMillis()
        return if (millis <= 0L) 0L else (millis + 999L) / 1000L
    }

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
            PlanType.SERVER -> RestartNotice(
                "SERVER",
                "SCHEDULED RESTART",
                "${plan.targets.single().value} restarts in $time.",
                "Players will be moved to the hub.",
                plan.reason,
                urgent,
            )
        }
    }

    @Synchronized
    private fun executeExternal(plan: RestartPlan, now: Instant) {
        if (plan.actionStarted || plan.state != PlanState.COUNTING_DOWN) return
        val cfg = config()
        val executionExecutor = executor.snapshot()
        if (!executionExecutor.performsPowerActions) {
            completeDryRun(plan, executionExecutor.name)
            return
        }

        plan.state = PlanState.PREFLIGHT
        plan.proxyBaselineBootId = currentProxyBootId
        plan.executionDeadlineAt = now.plus(executionTimeout())
        if (plan.type == PlanType.NETWORK) {
            plan.baselineBootIds.clear()
            for (target in plan.targets) {
                plan.baselineBootIds[target] = backendIdentity(target)
                    ?: throw IllegalStateException("no fresh authenticated companion identity for ${target.value}")
            }
        }
        save()

        val ids = when (plan.type) {
            PlanType.PROXY -> listOf(cfg.proxyServerId)
            PlanType.NETWORK -> plan.targets.map { cfg.serverIds.getValue(it) } + cfg.proxyServerId
            PlanType.SERVER -> emptyList()
        }
        CompletableFuture.allOf(*ids.map { id ->
            executionExecutor.preflight(id).toCompletableFuture().thenAccept { result ->
                check(result.accepted) { result.detail }
            }
        }.toTypedArray()).whenComplete { _, error ->
            try {
                when {
                    error != null -> fail(plan, "preflight failed: ${rootMessage(error)}")
                    plan.type == PlanType.PROXY -> executeProxy(plan, cfg, executionExecutor)
                    else -> executeNetwork(plan, cfg, executionExecutor)
                }
            } catch (dispatchError: Exception) {
                fail(plan, rootMessage(dispatchError))
            }
        }
    }

    @Synchronized
    private fun executeProxy(
        plan: RestartPlan,
        cfg: NetworkRestartConfig,
        executionExecutor: ExternalRestartExecutor,
    ) {
        enableMaintenance(plan, cfg)
        plan.state = PlanState.DISPATCHING
        save()
        finalAnnouncement(
            plan,
            RestartNotice("NETWORK", "RESTARTING NOW", "The Velocity proxy is restarting.", "All players are being disconnected.", plan.reason),
        )
        control.disconnectAll(
            RestartNotice("NETWORK", "Network restarting", "The Velocity proxy is restarting.", "Please reconnect shortly.", plan.reason, true),
        )
        dispatch(plan, "${plan.id}:proxy", cfg.proxyServerId, executionExecutor).whenComplete { result, error ->
            onProxyDispatchCompleted(plan, result, error, executionExecutor.name)
        }
    }

    @Synchronized
    private fun onProxyDispatchCompleted(
        plan: RestartPlan,
        result: PowerActionResult?,
        error: Throwable?,
        executorName: String,
    ) {
        if (error != null || result == null) {
            requireReview(plan, error?.let(::rootMessage) ?: "missing proxy restart result")
            return
        }
        if (!result.accepted) {
            failRejected(plan, result.detail)
            return
        }
        plan.targetResults["proxy"] = result.detail
        audit(plan, "proxy action accepted by $executorName; awaiting authenticated process replacement")
        save()
    }

    @Synchronized
    private fun executeNetwork(
        plan: RestartPlan,
        cfg: NetworkRestartConfig,
        executionExecutor: ExternalRestartExecutor,
    ) {
        enableMaintenance(plan, cfg)
        plan.state = PlanState.TRANSFERRING
        save()
        val nonHubs = plan.targets.filterNot(cfg.hubServers::contains)
        val transfers = nonHubs.map { control.transferAll(it, cfg.hubServers).toCompletableFuture() }
        CompletableFuture.allOf(*transfers.toTypedArray())
            .orTimeout(cfg.transferTimeoutSeconds, TimeUnit.SECONDS)
            .thenCompose {
                synchronized(this) {
                    plan.state = PlanState.DISPATCHING
                    save()
                }
                restartBatch(plan, nonHubs, cfg, executionExecutor)
            }
            .thenCompose {
                if (cfg.backendHeadStartSeconds == 0L) {
                    CompletableFuture.completedFuture<Void>(null)
                } else {
                    CompletableFuture.runAsync(
                        {},
                        CompletableFuture.delayedExecutor(cfg.backendHeadStartSeconds, TimeUnit.SECONDS),
                    )
                }
            }
            .thenCompose {
                synchronized(this) {
                    finalAnnouncement(
                        plan,
                        RestartNotice("NETWORK", "RESTARTING NOW", "The entire network is restarting.", "All players are being disconnected.", plan.reason),
                    )
                    control.disconnectAll(
                        RestartNotice("NETWORK", "Full network restart", "The entire Minecraft network is restarting.", "Please reconnect shortly.", plan.reason, true),
                    )
                }
                restartBatch(plan, plan.targets.filter(cfg.hubServers::contains), cfg, executionExecutor)
            }
            .thenCompose { dispatch(plan, "${plan.id}:proxy", cfg.proxyServerId, executionExecutor) }
            .whenComplete { proxyResult, error ->
                onNetworkDispatchCompleted(plan, proxyResult, error, executionExecutor.name)
            }
    }

    @Synchronized
    private fun onNetworkDispatchCompleted(
        plan: RestartPlan,
        proxyResult: PowerActionResult?,
        error: Throwable?,
        executorName: String,
    ) {
        if (error != null || proxyResult == null) {
            fail(plan, error?.let(::rootMessage) ?: "missing network restart result")
            return
        }
        plan.targetResults["proxy"] = if (proxyResult.accepted) proxyResult.detail else "FAILED: ${proxyResult.detail}"
        if (!proxyResult.accepted || plan.targetResults.values.any { it.startsWith("FAILED") }) {
            fail(plan, "one or more network restart actions were rejected or failed")
            return
        }
        val missing = expectedActionKeys(plan) - plan.acceptedActionKeys
        if (missing.isNotEmpty()) {
            requireReview(plan, "accepted restart actions were not durably recorded for ${missing.sorted().joinToString()}")
            return
        }
        plan.targetResults["network"] = "all actions accepted; awaiting authenticated boot replacements"
        audit(plan, "network actions accepted by $executorName; awaiting authenticated boot replacements")
        save()
    }

    private fun restartBatch(
        plan: RestartPlan,
        targets: List<ServerId>,
        cfg: NetworkRestartConfig,
        executionExecutor: ExternalRestartExecutor,
    ): CompletionStage<Void> {
        val groups = targets.chunked(cfg.maxConcurrentActions)
        var stage: CompletionStage<Void> = CompletableFuture.completedFuture(null)
        for (group in groups) {
            stage = stage.thenCompose {
                val requests = group.associateWith { target ->
                    dispatch(
                        plan,
                        "${plan.id}:${target.value}",
                        cfg.serverIds.getValue(target),
                        executionExecutor,
                    ).toCompletableFuture()
                }
                CompletableFuture.allOf(*requests.values.toTypedArray()).thenRun {
                    synchronized(this) {
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
            }
        }
        return stage
    }

    @Synchronized
    private fun enableMaintenance(plan: RestartPlan, cfg: NetworkRestartConfig) {
        control.setMaintenance(true, Duration.ofSeconds(cfg.maintenanceFailureExpirySeconds))
        plan.maintenanceEnabled = true
        save()
    }

    private fun refreshMaintenance(plan: RestartPlan) {
        if (
            !plan.maintenanceEnabled ||
            plan.state !in setOf(PlanState.PREFLIGHT, PlanState.TRANSFERRING, PlanState.DISPATCHING, PlanState.NEEDS_REVIEW)
        ) return
        control.setMaintenance(true, Duration.ofSeconds(config().maintenanceFailureExpirySeconds))
    }

    @Synchronized
    private fun dispatch(
        plan: RestartPlan,
        actionKey: String,
        panelServerId: String,
        executionExecutor: ExternalRestartExecutor,
    ): CompletionStage<PowerActionResult> {
        if (!plan.dispatchedActionKeys.add(actionKey)) {
            return CompletableFuture.completedFuture(PowerActionResult(false, "duplicate action blocked"))
        }
        plan.actionStarted = true
        save()
        return executionExecutor.restart(actionKey, panelServerId).whenComplete { result, error ->
            synchronized(this) {
                if (error == null && result != null) {
                    plan.targetResults[actionKey] = if (result.accepted) result.detail else "FAILED: ${result.detail}"
                    if (result.accepted) plan.acceptedActionKeys += actionKey
                }
                save()
            }
        }
    }

    @Synchronized
    private fun completeDryRun(plan: RestartPlan, executorName: String) {
        when (plan.type) {
            PlanType.PROXY -> plan.targetResults["proxy"] = "dry-run: no power action sent"
            PlanType.NETWORK -> {
                plan.targets.forEach { plan.targetResults[it.value] = "dry-run: no power action sent" }
                plan.targetResults["proxy"] = "dry-run: no power action sent"
            }
            PlanType.SERVER -> Unit
        }
        plan.dryRun = true
        plan.completedAt = Instant.now()
        plan.state = PlanState.COMPLETED
        plan.maintenanceEnabled = false
        plan.executionDeadlineAt = null
        audit(plan, "completed via $executorName without player disruption")
        reconcileMaintenance()
        save()
    }

    @Synchronized
    private fun completeVerified(plan: RestartPlan, detail: String) {
        plan.targetResults["verification"] = detail
        plan.completedAt = Instant.now()
        plan.state = PlanState.COMPLETED
        plan.failure = ""
        plan.maintenanceEnabled = false
        plan.executionDeadlineAt = null
        audit(plan, "completed after verified lifecycle: $detail")
        reconcileMaintenance()
        save()
    }

    @Synchronized
    private fun fail(plan: RestartPlan, detail: String) {
        val unresolvedDispatch = plan.dispatchedActionKeys.any { it !in plan.targetResults }
        val destructiveUncertainty =
            (plan.type == PlanType.SERVER && plan.actionStarted) ||
                plan.acceptedActionKeys.isNotEmpty() ||
                unresolvedDispatch
        if (destructiveUncertainty) {
            requireReview(plan, detail)
            return
        }
        plan.failure = detail
        plan.state = PlanState.FAILED
        plan.maintenanceEnabled = false
        audit(plan, "failed: $detail")
        reconcileMaintenance()
        save()
    }

    @Synchronized
    private fun failRejected(plan: RestartPlan, detail: String) {
        if (plan.acceptedActionKeys.isNotEmpty()) {
            requireReview(plan, "restart rejected after another action was accepted: $detail")
            return
        }
        plan.failure = detail
        plan.state = PlanState.FAILED
        plan.maintenanceEnabled = false
        audit(plan, "restart rejected before any accepted power action: $detail")
        reconcileMaintenance()
        save()
    }

    @Synchronized
    private fun requireReview(plan: RestartPlan, detail: String) {
        plan.failure = detail
        plan.state = PlanState.NEEDS_REVIEW
        if (plan.type != PlanType.SERVER) {
            control.setMaintenance(true, Duration.ofSeconds(config().maintenanceFailureExpirySeconds))
            plan.maintenanceEnabled = true
        } else {
            plan.maintenanceEnabled = false
        }
        audit(plan, "requires operator review: $detail")
        save()
    }

    @Synchronized
    fun resolveReview(prefix: String): Boolean {
        val matches = plans.values.filter {
            it.state == PlanState.NEEDS_REVIEW && it.id.toString().startsWith(prefix)
        }
        if (matches.size != 1) return false
        val plan = matches.single()
        if (plan.type == PlanType.SERVER) plan.targets.forEach(serverReviewResolver)
        plan.state = PlanState.FAILED
        plan.failure = "operator reconciled: ${plan.failure}"
        plan.maintenanceEnabled = false
        audit(plan, "operator resolved review state")
        reconcileMaintenance()
        save()
        return true
    }

    fun blocksBackendAccess(target: ServerId): Boolean = plans.values.any {
        it.type == PlanType.SERVER && target in it.targets &&
            it.state in setOf(PlanState.DISPATCHING, PlanState.NEEDS_REVIEW)
    }

    @Synchronized
    fun markBackendNeedsReview(target: ServerId, reason: String): Boolean {
        val plan = plans.values.firstOrNull {
            it.type == PlanType.SERVER && target in it.targets && it.state == PlanState.DISPATCHING
        } ?: return false
        requireReview(plan, reason)
        return true
    }

    @Synchronized
    private fun finalAnnouncement(plan: RestartPlan, notice: RestartNotice) {
        if (plan.silent || !plan.announcedSeconds.add(0L)) return
        control.broadcast(notice)
        soundResolver(0L)?.let(control::playSound)
        save()
    }

    private fun cancellation(plan: RestartPlan) = control.broadcast(
        RestartNotice(
            if (plan.type == PlanType.SERVER) "SERVER" else "NETWORK",
            "RESTART CANCELLED",
            "The scheduled ${plan.type.name.lowercase()} restart was cancelled.",
            "",
            "",
        ),
    )

    @Synchronized
    private fun miss(plan: RestartPlan, detail: String) {
        plan.failure = detail
        plan.state = PlanState.MISSED
        audit(plan, "missed: $detail")
        save()
    }

    private fun createAutomaticPlans(now: Instant) {
        if (!config().enabled) return
        schedules().filter { it.enabled }.forEach { definition ->
            val execution = nextOccurrence(definition, now)
            val key = "${definition.name}@$execution"
            if (plans.values.any { it.automaticKey == key }) return@forEach
            val warning = execution.minusSeconds(definition.warningWindowSeconds)
            if (now.isBefore(warning) || !now.isBefore(execution)) return@forEach
            val type = PlanType.valueOf(definition.type)
            try {
                schedule(
                    RestartPlan(
                        type = type,
                        targets = if (type == PlanType.NETWORK) config().members.toSet() else definition.targets.toSet(),
                        createdAt = now,
                        executionAt = execution,
                        warningAt = warning,
                        reason = definition.reason,
                        creator = "AUTOMATIC:${definition.name}",
                        automaticKey = key,
                        silent = definition.silent,
                    ),
                )
            } catch (_: IllegalArgumentException) {
                // An overlapping manual or unresolved plan intentionally suppresses this occurrence.
            }
        }
    }

    fun nextOccurrence(definition: ConfiguredRestartSchedule, now: Instant): Instant {
        val zone = ZoneId.of(definition.timezone)
        val time = LocalTime.parse(definition.time)
        val today = LocalDate.ofInstant(now, zone)
        for (offset in 0..7) {
            val candidate = ZonedDateTime.of(today.plusDays(offset.toLong()), time, zone)
            if (
                candidate.toInstant().isAfter(now) &&
                (definition.days.isEmpty() || candidate.dayOfWeek.name in definition.days)
            ) return candidate.toInstant()
        }
        throw IllegalArgumentException("schedule '${definition.name}' has no upcoming occurrence")
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
                require(plan.targets == cfg.members.toSet()) { "full-network plan targets must match configured members" }
                require(cfg.members.isNotEmpty()) { "full-network members are empty" }
                require(cfg.members.all(cfg.serverIds::containsKey)) { "full-network mapping is incomplete" }
                require(cfg.proxyServerId.isNotBlank()) { "proxy panel identifier is missing" }
            }
        }
    }

    private fun conflicts(left: RestartPlan, right: RestartPlan): Boolean =
        if (left.type != PlanType.SERVER || right.type != PlanType.SERVER) true else left.targets.any(right.targets::contains)

    private fun isLegacyRecoveryRegression(plan: RestartPlan): Boolean =
        plan.type in setOf(PlanType.PROXY, PlanType.NETWORK) &&
            plan.actionStarted &&
            plan.failure == LEGACY_INTERRUPTED_FAILURE &&
            plan.completedAt == null &&
            plan.executionDeadlineAt == null &&
            plan.dispatchedActionKeys.isEmpty() &&
            plan.acceptedActionKeys.isEmpty() &&
            plan.baselineBootIds.isEmpty() &&
            plan.proxyBaselineBootId == null &&
            plan.targetResults.isEmpty()

    private fun recover() {
        val now = Instant.now()
        val loaded = store.load()
        for (plan in loaded) {
            when {
                plan.state in setOf(
                    PlanState.COMPLETED,
                    PlanState.CANCELLED,
                    PlanState.FAILED,
                    PlanState.MISSED,
                ) -> {
                    // actionStarted is durable history, not evidence that a
                    // terminal plan became active again after proxy startup.
                    plan.maintenanceEnabled = false
                    plan.executionDeadlineAt = null
                }
                plan.state == PlanState.NEEDS_REVIEW && plan.completedAt != null -> {
                    // Affected builds could overwrite a verified COMPLETED plan
                    // solely because actionStarted remained true. completedAt is
                    // written by the terminal transition, so restore it once.
                    plan.state = PlanState.COMPLETED
                    plan.failure = ""
                    plan.maintenanceEnabled = false
                    plan.executionDeadlineAt = null
                    plan.targetResults.putIfAbsent(
                        "recovery",
                        "restored completed plan after legacy recovery regression",
                    )
                }
                plan.state == PlanState.NEEDS_REVIEW && isLegacyRecoveryRegression(plan) -> {
                    // The known regression also produced records with the exact
                    // legacy failure but none of the durable evidence written by
                    // a real dispatch path. Close only that impossible state.
                    plan.state = PlanState.FAILED
                    plan.failure = "legacy recovery regression reconciled"
                    plan.maintenanceEnabled = false
                    plan.executionDeadlineAt = null
                }
                plan.state == PlanState.NEEDS_REVIEW -> {
                    plan.maintenanceEnabled = plan.type != PlanType.SERVER
                }
                plan.state == PlanState.DISPATCHING -> recoverDispatching(plan, now)
                plan.state == PlanState.PREFLIGHT && !plan.actionStarted && plan.acceptedActionKeys.isEmpty() -> {
                    plan.state = PlanState.FAILED
                    plan.failure = "preflight was interrupted before any power action"
                    plan.maintenanceEnabled = false
                }
                plan.state == PlanState.TRANSFERRING && !plan.actionStarted && plan.acceptedActionKeys.isEmpty() -> {
                    plan.state = PlanState.FAILED
                    plan.failure = "player transfer was interrupted before any power action"
                    plan.maintenanceEnabled = false
                }
                plan.state in setOf(PlanState.PREFLIGHT, PlanState.TRANSFERRING) ||
                    (plan.active() && plan.actionStarted) -> {
                    plan.state = PlanState.NEEDS_REVIEW
                    plan.failure = LEGACY_INTERRUPTED_FAILURE
                    plan.maintenanceEnabled = plan.type != PlanType.SERVER
                }
                plan.active() && !plan.executionAt.isAfter(now) -> {
                    plan.state = PlanState.MISSED
                    plan.failure = "execution time elapsed while the proxy was unavailable"
                    plan.maintenanceEnabled = false
                }
                plan.type == PlanType.SERVER && plan.state == PlanState.COUNTING_DOWN -> {
                    plan.state = PlanState.SCHEDULED
                    plan.backendArmAccepted = false
                    plan.lastObservedRemainingSeconds = null
                    plan.executionDeadlineAt = null
                    plan.baselineBootIds.clear()
                }
            }
            plans[plan.id] = plan
        }
        reconcileMaintenance()
        save()
    }

    private fun recoverDispatching(plan: RestartPlan, now: Instant) {
        val structurallyComplete = when (plan.type) {
            PlanType.SERVER -> plan.actionStarted && plan.baselineBootIds.keys == plan.targets && plan.executionDeadlineAt != null
            PlanType.PROXY -> plan.actionStarted && plan.proxyBaselineBootId != null && plan.executionDeadlineAt != null &&
                expectedActionKeys(plan).all(plan.acceptedActionKeys::contains)
            PlanType.NETWORK -> plan.actionStarted && plan.proxyBaselineBootId != null && plan.executionDeadlineAt != null &&
                plan.baselineBootIds.keys == plan.targets && expectedActionKeys(plan).all(plan.acceptedActionKeys::contains)
        }
        if (!structurallyComplete) {
            plan.state = PlanState.NEEDS_REVIEW
            plan.failure = "persisted execution is missing durable action acceptance or lifecycle verification state"
            plan.maintenanceEnabled = plan.type != PlanType.SERVER
            return
        }
        if (deadlineElapsed(plan, now)) {
            plan.state = PlanState.NEEDS_REVIEW
            plan.failure = "execution verification deadline elapsed while the proxy was unavailable"
            plan.maintenanceEnabled = plan.type != PlanType.SERVER
        } else {
            plan.maintenanceEnabled = plan.type != PlanType.SERVER
        }
    }

    private fun expectedActionKeys(plan: RestartPlan): Set<String> = when (plan.type) {
        PlanType.SERVER -> emptySet()
        PlanType.PROXY -> setOf("${plan.id}:proxy")
        PlanType.NETWORK -> plan.targets.mapTo(mutableSetOf()) { "${plan.id}:${it.value}" }.also {
            it += "${plan.id}:proxy"
        }
    }

    private fun reconcileMaintenance() {
        val required = plans.values.any {
            it.maintenanceEnabled && it.state in setOf(
                PlanState.PREFLIGHT,
                PlanState.TRANSFERRING,
                PlanState.DISPATCHING,
                PlanState.NEEDS_REVIEW,
            )
        }
        if (required) {
            control.setMaintenance(true, Duration.ofSeconds(config().maintenanceFailureExpirySeconds))
        } else {
            control.setMaintenance(false, Duration.ZERO)
        }
    }

    @Synchronized
    private fun save() {
        val terminal = plans.values
            .filter { !it.blocksScheduling() }
            .sortedByDescending { it.completedAt ?: it.executionAt }
        terminal.drop(MAX_TERMINAL_HISTORY).forEach { plan -> plans.remove(plan.id, plan) }
        store.save(plans.values)
    }

    private fun rootMessage(error: Throwable): String =
        generateSequence(error) { it.cause }.last().message ?: error.javaClass.simpleName

    companion object {
        private const val LEGACY_INTERRUPTED_FAILURE =
            "execution was interrupted after a destructive action may have started"
        private const val MAX_TERMINAL_HISTORY = 500
    }
}
