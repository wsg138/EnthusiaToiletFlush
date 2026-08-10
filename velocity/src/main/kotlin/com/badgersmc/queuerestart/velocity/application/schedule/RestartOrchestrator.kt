package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.common.schedule.PendingArm
import com.badgersmc.queuerestart.velocity.application.arm.PendingArmStore
import com.badgersmc.queuerestart.velocity.application.drain.DrainCandidate
import com.badgersmc.queuerestart.velocity.application.drain.DrainPlanner
import com.badgersmc.queuerestart.velocity.application.drain.HubResolver
import com.badgersmc.queuerestart.velocity.application.drain.RejoinService
import com.badgersmc.queuerestart.velocity.application.gate.CheckGate
import com.badgersmc.queuerestart.velocity.application.ports.AudiencePort
import com.badgersmc.queuerestart.velocity.application.ports.MessagingPort
import com.badgersmc.queuerestart.velocity.application.ports.ProxyPort
import com.badgersmc.queuerestart.velocity.application.ports.QueueRestartConfig
import com.badgersmc.queuerestart.velocity.domain.coordinator.RestartState
import com.badgersmc.queuerestart.velocity.domain.countdown.CountdownSchedule
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.rank.RankLadder
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

/**
 * REQ-001, REQ-010, REQ-012, REQ-020, REQ-040.
 *
 * Drives every coordinator's state machine forward on each tick.
 * Owns the per-target ephemeral state needed to translate elapsed-time
 * to the right transitions:
 *  - countdownStartedAt — when ARMED→COUNTDOWN happened
 *  - drainStartedAt — when COUNTDOWN→DRAINING happened
 *  - drainBatches / nextBatchAt — pending drain dispatch queue + cadence
 *
 * The orchestrator does NOT arm coordinators itself — that's the
 * `SchedRestartCommandHandler`'s job. Once a coordinator is in
 * [RestartState.ARMED], the next tick promotes it to COUNTDOWN and the
 * cycle proceeds automatically.
 */
class RestartOrchestrator(
    private val registry: CoordinatorRegistry,
    private val proxy: ProxyPort,
    private val messaging: MessagingPort,
    private val audience: AudiencePort,
    private val broadcaster: CountdownBroadcaster,
    private val planner: DrainPlanner,
    private val hubResolver: HubResolver,
    private val rejoin: RejoinService,
    private val gate: CheckGate,
    private val rankLadder: RankLadder,
    private val configSupplier: () -> QueueRestartConfig,
    private val restartMode: RestartMode = RestartMode.SHUTDOWN,
    private val restartArg: String = "",
    private val pendingArmStore: PendingArmStore = PendingArmStore(),
    private val options: BackendRestartOptions = BackendRestartOptions(),
    private val companionIdentity: (ServerId) -> UUID? = { null },
    private val onRestartPublished: (ServerId, UUID) -> Boolean = { _, _ -> true },
) {

    private data class TargetState(
        var countdownStartedAt: Instant? = null,
        var drainStartedAt: Instant? = null,
        var pendingBatches: ArrayDeque<List<PlayerId>> = ArrayDeque(),
        var nextBatchAt: Instant? = null,
        var fallbackDisconnectIssued: Boolean = false,
        val transferResults: MutableMap<PlayerId, CompletableFuture<Boolean>> = mutableMapOf(),
        val disconnectResults: MutableMap<PlayerId, CompletableFuture<Boolean>> = mutableMapOf(),
        var restartDeliveryId: UUID? = null,
        var preparedBaselineBootId: UUID? = null,
    )

    private val state = ConcurrentHashMap<ServerId, TargetState>()

    /** Wires the inbound subscriptions on [MessagingPort]. Call once. */
    fun start() {
        // REQ-040: route CheckHacks verdicts through the rejoin service
        // so a DETECTED player never reaches the queue. Routing directly
        // to gate.onResult (the prior wiring) updated gate state but
        // never blocked the enqueue path — dead-code anti-cheat bypass.
        messaging.onCheckHacksResult { source, player, outcome ->
            rejoin.onCheckHacksResult(source, player, outcome)
        }
    }

    /** Drive every coordinator forward. Caller supplies wall time. */
    @Synchronized
    fun tick(now: Instant) {
        val cfg = configSupplier()
        for ((target, coord) in registry.all()) {
            val s = state.getOrPut(target) { TargetState() }
            when (coord.state) {
                RestartState.ARMED -> tickArmed(target, coord.durationSeconds, cfg, s, now)
                RestartState.COUNTDOWN -> tickCountdown(target, coord.durationSeconds, cfg, s, now)
                RestartState.DRAINING -> tickDraining(target, cfg, s, now)
                else -> {} // RESTART_SENT/SERVER_DOWN/REJOIN_RELEASE handled by PingPoller
            }
        }
    }

    /** REQ-005. Direct legacy cancellation; inactive targets remain a no-op. */
    @Suppress("UNUSED_PARAMETER")
    @Synchronized
    fun cancel(target: ServerId, now: Instant = Instant.now()) {
        cancelInternal(
            target = target,
            silent = options.isSilent(target),
            announceWhenInactive = false,
        )
    }

    /**
     * Cancellation owner for persisted server plans. It also covers plans
     * cancelled before the ephemeral coordinator has been armed.
     */
    @Suppress("UNUSED_PARAMETER")
    @Synchronized
    fun cancelPlan(target: ServerId, silent: Boolean, now: Instant = Instant.now()) {
        cancelInternal(target, silent, announceWhenInactive = true)
    }

    private fun cancelInternal(
        target: ServerId,
        silent: Boolean,
        announceWhenInactive: Boolean,
    ) {
        val coord = registry.get(target)
        val active = coord.state == RestartState.ARMED || coord.state == RestartState.COUNTDOWN
        if (!active && !announceWhenInactive) return
        if (active) coord.cancel()
        broadcaster.cancel(target)
        if (!silent) {
            val cfg = configSupplier()
            audience.broadcast(target, cfg.countdown.cancelMessage, mapOf("server" to target.value))
        }
        options.clear(target)
        state.remove(target)
        // The backend is not armed before T-0. A cancellable plan therefore
        // has no shutdown task to revoke; clearing an unpublished local arm is
        // sufficient and avoids cancel/restart transport reordering races.
        pendingArmStore.clear(target)
    }

    private fun tickArmed(
        target: ServerId,
        durationSeconds: Int,
        cfg: QueueRestartConfig,
        s: TargetState,
        now: Instant,
    ) {
        val coord = registry.get(target)
        broadcaster.register(
            target,
            CountdownSchedule(cfg.countdown.marksSeconds),
            cfg.hubServer,
            options.isSilent(target),
            startingSeconds = durationSeconds,
        )
        s.countdownStartedAt = now
        coord.beginCountdown()
        // The backend is deliberately NOT armed here. The proxy owns the
        // countdown, starts draining at T-0, and only sends an immediate
        // shutdown after every transferable player has left the target.
        // This prevents players being moved to the hub tens of seconds
        // before the visible timer reaches zero.
        broadcaster.tick(target, durationSeconds)
        if (durationSeconds == 0) {
            coord.beginDrain()
            startDrain(target, cfg, s, now)
        }
    }

    private fun tickCountdown(
        target: ServerId,
        durationSeconds: Int,
        cfg: QueueRestartConfig,
        s: TargetState,
        now: Instant,
    ) {
        val coord = registry.get(target)
        val started = s.countdownStartedAt ?: return
        val elapsed = Duration.between(started, now).seconds.toInt()
        val remaining = (durationSeconds - elapsed).coerceAtLeast(0)
        broadcaster.tick(target, remaining)
        if (remaining == 0) {
            coord.beginDrain()
            startDrain(target, cfg, s, now)
        }
    }

    private fun startDrain(target: ServerId, cfg: QueueRestartConfig, s: TargetState, now: Instant) {
        s.drainStartedAt = now
        s.transferResults.clear()
        s.disconnectResults.clear()
        s.fallbackDisconnectIssued = false
        // Drain whoever's actually on the target right now. Cohort is the
        // arm-time roster (REQ-030) and only governs rejoin — using it for
        // drain would skip players who joined after arming.
        val candidates = proxy.playersOn(target).map { playerId ->
            val perms = proxy.permissionsOf(playerId)
            DrainCandidate(
                playerId = playerId,
                weight = rankLadder.resolve(perms),
                bypassDrain = "queuerestart.bypass.drain" in perms,
            )
        }
        val batches = planner.plan(candidates, cfg.drain.drainOrder, cfg.drain.batchSize)
        s.pendingBatches = ArrayDeque(batches)
        s.nextBatchAt = now
        // Dispatch first batch immediately so single-tick tests see progress.
        dispatchDueBatches(cfg, s, now)
        // A roster can become empty slightly before Velocity completes the
        // connection request. Do not restart underneath an in-flight transfer.
        if (proxy.playersOn(target).isEmpty() && s.transferResults.values.all { it.isDone }) {
            sendRestart(target, s, now)
        }
    }

    private fun tickDraining(target: ServerId, cfg: QueueRestartConfig, s: TargetState, now: Instant) {
        dispatchDueBatches(cfg, s, now)

        val remaining = proxy.playersOn(target)
        val started = s.drainStartedAt ?: now
        val elapsed = Duration.between(started, now).seconds
        val timedOut = elapsed >= cfg.drain.forceDrainTimeoutSeconds
        val transfersComplete = s.transferResults.values.all { it.isDone }

        if (remaining.isEmpty()) {
            if (transfersComplete || timedOut) {
                sendRestart(target, s, now)
            }
            return
        }

        val transferSettled = s.pendingBatches.isEmpty() &&
            transfersComplete &&
            s.nextBatchAt?.let { !now.isBefore(it) } == true

        // Do not infer transfer completion from elapsed time alone. Velocity's
        // connection request may still be resolving even after the batch cadence
        // elapsed; disconnecting at that point races the successful hub move.
        if ((transferSettled || timedOut) && !s.fallbackDisconnectIssued) {
            remaining.forEach { playerId ->
                s.disconnectResults[playerId] = audience.disconnectAndAwait(
                    playerId,
                    cfg.accessMessages.drainDisconnect,
                    mapOf("server" to target.value, "hub" to cfg.hubServer.value),
                ).toCompletableFuture().exceptionally { false }
            }
            s.fallbackDisconnectIssued = true
        }

        // Successful disconnect settlement is the normal restart path. The
        // configured force timeout remains a hard upper bound even if an
        // adapter returns a disconnect stage that never completes.
        if (s.fallbackDisconnectIssued) {
            val disconnectsSettled = s.disconnectResults.values.all { it.isDone && it.getNow(false) }
            if (disconnectsSettled || timedOut) {
                sendRestart(target, s, now)
            }
        }
    }

    private fun dispatchDueBatches(cfg: QueueRestartConfig, s: TargetState, now: Instant) {
        val intervalMillis = (cfg.drain.batchIntervalTicks * 50L) // 1 tick = 50ms
        while (s.pendingBatches.isNotEmpty() && (s.nextBatchAt == null || !now.isBefore(s.nextBatchAt))) {
            val batch = s.pendingBatches.removeFirst()
            val hub = hubResolver.resolve(cfg.hubServer, cfg.fallbackHubs)
            for (pid in batch) {
                s.transferResults[pid] = if (hub == null) {
                    CompletableFuture.completedFuture(false)
                } else {
                    proxy.transferPlayerAwaitable(pid, hub).toCompletableFuture().exceptionally { false }
                }
            }
            s.nextBatchAt = now.plusMillis(intervalMillis)
        }
    }

    /** Persisted plan authority calls this before T-0 side effects are allowed. */
    @Synchronized
    fun prepareRestartHandoff(target: ServerId, baselineBootId: UUID): Boolean {
        val coordinator = registry.get(target)
        if (coordinator.state != RestartState.COUNTDOWN) return false
        val targetState = state.getOrPut(target) { TargetState() }
        targetState.preparedBaselineBootId = baselineBootId
        return true
    }

    private fun sendRestart(target: ServerId, targetState: TargetState, now: Instant) {
        val coordinator = registry.get(target)
        if (coordinator.state != RestartState.DRAINING) return
        val preparedBaseline = targetState.preparedBaselineBootId
            ?: throw IllegalStateException("restart handoff for ${target.value} was not durably prepared")

        if (targetState.restartDeliveryId == null) {
            val currentIdentity = companionIdentity(target)
            check(currentIdentity == preparedBaseline) {
                "authenticated companion identity changed before restart delivery for ${target.value}"
            }
            val deliveryId = pendingArmStore.put(
                target,
                mode = restartMode,
                argument = restartArg,
                delaySeconds = 0,
                expectedBootId = preparedBaseline,
                now = now,
            )
            targetState.restartDeliveryId = deliveryId
            messaging.sendRestartNow(target, deliveryId, restartMode, restartArg, delaySeconds = 0)
        }

        // This callback persists that an idempotent delivery exists before the
        // ephemeral coordinator advances. If persistence fails, the next tick
        // retries the callback with the same delivery id rather than creating a
        // second independently executable restart.
        check(onRestartPublished(target, preparedBaseline)) {
            "persisted plan rejected restart publication for ${target.value}"
        }
        coordinator.restartSent(preparedBaseline)
        state.remove(target)
    }

    /** Operator-only reset after manually verifying an uncertain backend state. */
    @Synchronized
    fun resolveAfterManualReview(target: ServerId): Boolean {
        val coordinator = registry.get(target)
        if (coordinator.state == RestartState.IDLE) return false
        coordinator.forceResetAfterReview()
        state.remove(target)
        options.clear(target)
        pendingArmStore.clear(target)
        broadcaster.cancel(target)
        return true
    }

    /** Called by PingPoller after coord.serverUp(). */
    @Synchronized
    fun finishRejoin(target: ServerId, nowSeconds: Long) {
        val coord = registry.get(target)
        if (coord.state != RestartState.REJOIN_RELEASE) return
        coord.cohort?.let { rejoin.enqueueRejoin(target, it, nowSeconds) }
        coord.releaseComplete()
    }
}
