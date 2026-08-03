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
    @Suppress("unused") private val rankLadder: RankLadder,
    private val configSupplier: () -> QueueRestartConfig,
    private val restartMode: RestartMode = RestartMode.SHUTDOWN,
    private val restartArg: String = "",
    private val pendingArmStore: PendingArmStore = PendingArmStore(),
    private val options: BackendRestartOptions = BackendRestartOptions(),
) {

    private data class TargetState(
        var countdownStartedAt: Instant? = null,
        var drainStartedAt: Instant? = null,
        var pendingBatches: ArrayDeque<List<PlayerId>> = ArrayDeque(),
        var nextBatchAt: Instant? = null,
        var fallbackDisconnectIssued: Boolean = false,
    )

    private val state = mutableMapOf<ServerId, TargetState>()

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

    /** REQ-005. Cancels the countdown for [target] and broadcasts the cancel message. */
    @Synchronized
    fun cancel(target: ServerId, now: Instant = Instant.now()) {
        val coord = registry.all()[target] ?: return
        if (coord.state != RestartState.ARMED && coord.state != RestartState.COUNTDOWN) return
        val cfg = configSupplier()
        coord.cancel()
        broadcaster.cancel(target)
        if (!options.isSilent(target)) {
            audience.broadcast(target, cfg.countdown.cancelMessage, mapOf("server" to target.value))
        }
        options.clear(target)
        state.remove(target)
        // Clear any stale delivery from an older version or interrupted
        // attempt. New restarts are not armed on the backend until after T-0
        // drain completion, so a normal countdown cancellation has no pending
        // shutdown to abort.
        pendingArmStore.cancel(target, now)
        messaging.sendRestartCancel(target)
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
        // Drain whoever's actually on the target right now. Cohort is the
        // arm-time roster (REQ-030) and only governs rejoin — using it for
        // drain would skip players who joined after arming.
        val candidates = proxy.playersOn(target).map { playerId ->
            val perms = proxy.permissionsOf(playerId)
            DrainCandidate(
                playerId = playerId,
                weight = 0, // ordering only, not queue weight
                bypassDrain = "queuerestart.bypass.drain" in perms,
            )
        }
        val batches = planner.plan(candidates, cfg.drain.drainOrder, cfg.drain.batchSize)
        s.pendingBatches = ArrayDeque(batches)
        s.nextBatchAt = now
        // Dispatch first batch immediately so single-tick tests see progress
        dispatchDueBatches(target, cfg, s, now)
        // If the target is already empty, dispatch the immediate shutdown now.
        if (proxy.playersOn(target).isEmpty()) {
            sendRestart(target, now)
        }
    }

    private fun tickDraining(target: ServerId, cfg: QueueRestartConfig, s: TargetState, now: Instant) {
        dispatchDueBatches(target, cfg, s, now)

        val remaining = proxy.playersOn(target)
        if (remaining.isEmpty()) {
            sendRestart(target, now)
            return
        }

        val started = s.drainStartedAt ?: now
        val elapsed = Duration.between(started, now).seconds
        val timedOut = elapsed >= cfg.drain.forceDrainTimeoutSeconds
        val transferSettled = s.pendingBatches.isEmpty() &&
            s.nextBatchAt?.let { !now.isBefore(it) } == true

        // Once every transfer batch has had one full batch interval to settle,
        // disconnect only the players who are still stuck on the backend.
        // This covers a failed hub transfer and drain-bypass holders without
        // kicking players who are successfully moving between servers.
        if ((transferSettled || timedOut) && !s.fallbackDisconnectIssued) {
            remaining.forEach { playerId ->
                audience.disconnect(
                    playerId,
                    cfg.accessMessages.drainDisconnect,
                    mapOf("server" to target.value, "hub" to cfg.hubServer.value),
                )
            }
            s.fallbackDisconnectIssued = true
        }

        // The regular path waits for Velocity to observe an empty target on
        // the next tick. The force timeout remains the final safety valve: all
        // remaining disconnects are issued before the immediate restart arm.
        if (timedOut) sendRestart(target, now)
    }

    private fun dispatchDueBatches(target: ServerId, cfg: QueueRestartConfig, s: TargetState, now: Instant) {
        val intervalMillis = (cfg.drain.batchIntervalTicks * 50L) // 1 tick = 50ms
        while (s.pendingBatches.isNotEmpty() && (s.nextBatchAt == null || !now.isBefore(s.nextBatchAt))) {
            val batch = s.pendingBatches.removeFirst()
            val hub = hubResolver.resolve(cfg.hubServer, cfg.fallbackHubs) ?: cfg.hubServer
            for (pid in batch) proxy.transferPlayer(pid, hub)
            s.nextBatchAt = now.plusMillis(intervalMillis)
        }
    }

    private fun sendRestart(target: ServerId, now: Instant) {
        val coord = registry.get(target)
        if (coord.state != RestartState.DRAINING) return
        // Send through both delivery paths only after drain completion. The
        // plugin-message path is fast when a player is still present; the SLP
        // poll-back path works when the target is already empty. RestartExecutor
        // replaces duplicate deliveries, so both are safe and idempotent.
        messaging.sendRestartNow(target, restartMode, restartArg, delaySeconds = 0)
        pendingArmStore.put(
            target,
            PendingArm(0, restartMode, restartArg),
            now = now,
        )
        coord.restartSent()
        state.remove(target)
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
