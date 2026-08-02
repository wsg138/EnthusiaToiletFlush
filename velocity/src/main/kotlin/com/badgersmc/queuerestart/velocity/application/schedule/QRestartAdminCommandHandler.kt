package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.application.ports.ConfigPort

sealed interface AdminCommandResult {
    data object Reloaded : AdminCommandResult
    data class Triggered(val schedule: String) : AdminCommandResult
    data class Rejected(val reason: String) : AdminCommandResult
}

/**
 * REQ-050, REQ-051.
 *
 * `/qrestart reload` reparses config and invokes [onReload] so proxy-owned
 * schedule registrations and permission probes can be rebuilt. It does NOT
 * touch in-flight restart state, so an armed countdown survives a reload.
 *
 * `/qrestart trigger <name>` looks up the schedule by name and arms its
 * restart immediately by delegating to [SchedRestartCommandHandler.arm]
 * — same path as if the cron had fired.
 */
class QRestartAdminCommandHandler(
    private val config: ConfigPort,
    private val scheduleService: ScheduleService,
    private val schedRestartHandler: SchedRestartCommandHandler,
    /**
     * SECURITY (REQ-090): callback fired after [ConfigPort.reload] so the
     * infrastructure layer can re-derive any state that depends on the
     * snapshot — notably the `VelocityProxyServerBackend.withRankLadder`
     * probe set and configured proxy restart schedules.
     */
    private val onReload: () -> Unit = {},
) {

    fun reload(): AdminCommandResult {
        config.reload()
        onReload()
        // Backend schedules remain sourced from companions via SLP. The
        // callback refreshes proxy-owned schedules from the new config while
        // preserving those cached backend definitions.
        return AdminCommandResult.Reloaded
    }

    fun trigger(name: String): AdminCommandResult {
        val def = scheduleService.findByName(name)
            ?: return AdminCommandResult.Rejected("unknown schedule '$name'")
        val armed = schedRestartHandler.arm(def.target, durationMinutes = def.warnMinutes)
        return when (armed) {
            is SchedCommandResult.Armed -> AdminCommandResult.Triggered(name)
            is SchedCommandResult.Rejected -> AdminCommandResult.Rejected(armed.reason)
            else -> AdminCommandResult.Rejected("unexpected arm outcome: $armed")
        }
    }
}
