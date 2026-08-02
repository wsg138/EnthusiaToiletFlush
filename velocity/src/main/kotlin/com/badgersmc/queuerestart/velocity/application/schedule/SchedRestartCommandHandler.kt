package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.domain.cohort.Cohort
import com.badgersmc.queuerestart.velocity.domain.coordinator.RestartState
import com.badgersmc.queuerestart.velocity.domain.id.ServerId

/** Outcome of one `/schedrestart …` invocation. */
sealed interface SchedCommandResult {
    data class Armed(val server: ServerId, val durationSeconds: Int) : SchedCommandResult
    data class Cancelled(val server: ServerId) : SchedCommandResult
    data class Status(val states: Map<ServerId, RestartState>) : SchedCommandResult
    data class Rejected(val reason: String) : SchedCommandResult
}

/**
 * REQ-001, REQ-005, REQ-023, REQ-052, REQ-060, REQ-061, REQ-062.
 *
 * Pure command logic. The infrastructure-side Brigadier shim parses `/schedrestart`
 * argument trees and forwards to one of [arm] / [cancel] / [status]; the
 * shim is responsible for permission gates and message rendering.
 */
class SchedRestartCommandHandler(
    private val registry: CoordinatorRegistry,
    private val hubServer: ServerId,
    private val companionPresent: (ServerId) -> Boolean,
    private val cohortFor: (ServerId) -> Cohort,
    private val proxyRestart: ProxyRestartController? = null,
) {

    fun arm(target: ServerId, durationMinutes: Int): SchedCommandResult {
        if (durationMinutes <= 0) {
            return SchedCommandResult.Rejected("duration must be > 0 minutes")
        }

        val durationSeconds = durationMinutes * 60
        if (target == ProxyRestartService.TARGET) {
            val controller = proxyRestart
                ?: return SchedCommandResult.Rejected("proxy restart support is unavailable")
            if (!controller.arm(durationSeconds)) {
                return SchedCommandResult.Rejected(
                    "restart already armed for '${target.value}' (state=${controller.state})",
                )
            }
            return SchedCommandResult.Armed(target, durationSeconds)
        }

        if (target == hubServer) {
            return SchedCommandResult.Rejected("cannot restart the hub server '${target.value}'")
        }
        if (!companionPresent(target)) {
            return SchedCommandResult.Rejected(
                "no queue-restart companion plugin on '${target.value}'",
            )
        }
        val coord = registry.get(target)
        if (coord.state != RestartState.IDLE) {
            return SchedCommandResult.Rejected(
                "restart already armed for '${target.value}' (state=${coord.state})",
            )
        }
        coord.arm(cohortFor(target), durationSeconds = durationSeconds)
        return SchedCommandResult.Armed(target, coord.durationSeconds)
    }

    fun cancel(target: ServerId): SchedCommandResult {
        if (target == ProxyRestartService.TARGET) {
            val controller = proxyRestart
                ?: return SchedCommandResult.Rejected("proxy restart support is unavailable")
            return if (controller.cancel()) {
                SchedCommandResult.Cancelled(target)
            } else {
                SchedCommandResult.Rejected(
                    "cannot cancel '${target.value}': not in cancellable state",
                )
            }
        }

        val coord = registry.get(target)
        return try {
            coord.cancel()
            SchedCommandResult.Cancelled(target)
        } catch (e: IllegalStateException) {
            SchedCommandResult.Rejected(
                "cannot cancel '${target.value}': ${e.message ?: "not in cancellable state"}",
            )
        }
    }

    fun status(): SchedCommandResult {
        val states = registry.all().mapValuesTo(linkedMapOf()) { it.value.state }
        proxyRestart?.let { controller ->
            if (controller.state != RestartState.IDLE) {
                states[ProxyRestartService.TARGET] = controller.state
            }
        }
        return SchedCommandResult.Status(states)
    }
}
