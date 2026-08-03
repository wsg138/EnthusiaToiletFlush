package com.badgersmc.queuerestart.velocity.application.drain

import com.badgersmc.queuerestart.velocity.application.companion.CompanionRegistry
import com.badgersmc.queuerestart.velocity.application.schedule.CoordinatorRegistry
import com.badgersmc.queuerestart.velocity.domain.coordinator.RestartCoordinator
import com.badgersmc.queuerestart.velocity.domain.coordinator.RestartState
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * Proves a real backend lifecycle with authenticated companion boot identities.
 * Reachability alone is never considered proof of restart completion.
 */
class PingPoller(
    private val registry: CoordinatorRegistry,
    private val companions: CompanionRegistry,
    private val onReady: (ServerId, Long) -> Unit,
    private val heartbeatTimeout: () -> Duration,
    private val executionTimeout: () -> Duration,
    private val onTimeout: (ServerId, String) -> Unit = { _, _ -> },
) {
    private data class Observation(
        val startedAt: Instant,
        var timeoutReported: Boolean = false,
    )

    private val observations = ConcurrentHashMap<ServerId, Observation>()

    fun tick(now: Instant) {
        for ((target, coordinator) in registry.all()) {
            when (coordinator.state) {
                RestartState.RESTART_SENT -> observeRestartSent(target, coordinator, now)
                RestartState.SERVER_DOWN -> observeServerDown(target, coordinator, now)
                RestartState.IDLE -> observations.remove(target)
                else -> Unit
            }
        }
    }

    private fun observeRestartSent(target: ServerId, coordinator: RestartCoordinator, now: Instant) {
        val baseline = coordinator.restartBaselineBootId
        if (baseline == null) {
            reportTimeout(target, observation(target, now), "restart has no authenticated baseline boot id")
            return
        }

        val observation = observation(target, now)
        val heartbeat = companions.compatibleHeartbeat(target, now, heartbeatTimeout())
        when {
            heartbeat == null -> coordinator.serverDown()
            heartbeat.bootId != baseline -> {
                // A very fast restart can produce a new boot heartbeat before a
                // polling tick observes the stale/down interval. The identity
                // change itself is sufficient proof.
                coordinator.serverDown()
                coordinator.serverUp()
                observations.remove(target)
                onReady(target, now.epochSecond)
                return
            }
            else -> Unit
        }
        checkTimeout(target, observation, now)
    }

    private fun observeServerDown(target: ServerId, coordinator: RestartCoordinator, now: Instant) {
        val baseline = coordinator.restartBaselineBootId
        if (baseline == null) {
            reportTimeout(target, observation(target, now), "server-down state has no authenticated baseline boot id")
            return
        }

        val observation = observation(target, now)
        val heartbeat = companions.compatibleHeartbeat(target, now, heartbeatTimeout())
        if (heartbeat != null && heartbeat.bootId != baseline) {
            coordinator.serverUp()
            observations.remove(target)
            onReady(target, now.epochSecond)
            return
        }
        checkTimeout(target, observation, now)
    }

    private fun observation(target: ServerId, now: Instant): Observation =
        observations.computeIfAbsent(target) { Observation(now) }

    private fun checkTimeout(target: ServerId, observation: Observation, now: Instant) {
        if (Duration.between(observation.startedAt, now) >= executionTimeout()) {
            reportTimeout(
                target,
                observation,
                "backend did not present a new authenticated boot id before the execution timeout",
            )
        }
    }

    private fun reportTimeout(target: ServerId, observation: Observation, reason: String) {
        if (observation.timeoutReported) return
        observation.timeoutReported = true
        onTimeout(target, reason)
    }
}
