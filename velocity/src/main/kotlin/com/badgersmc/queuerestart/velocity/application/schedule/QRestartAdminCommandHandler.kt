package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.application.ports.ConfigPort

sealed interface AdminCommandResult {
    data object Reloaded : AdminCommandResult
    data class Triggered(val schedule: String) : AdminCommandResult
    data class Resolved(val plan: String) : AdminCommandResult
    data class Rejected(val reason: String) : AdminCommandResult
}

/** Administrative configuration and proxy-owned schedule controls. */
class QRestartAdminCommandHandler(
    private val config: ConfigPort,
    private val triggerSchedule: (String) -> Boolean,
    private val resolveReview: (String) -> Boolean = { false },
    private val resolveServer: (String) -> Boolean = { false },
    private val onReload: () -> Unit = {},
) {
    fun reload(): AdminCommandResult {
        config.reload()
        onReload()
        return AdminCommandResult.Reloaded
    }

    fun trigger(name: String): AdminCommandResult =
        if (triggerSchedule(name)) AdminCommandResult.Triggered(name)
        else AdminCommandResult.Rejected("unknown, disabled, or conflicting schedule '$name'")

    fun resolve(planOrServer: String): AdminCommandResult = when {
        resolveReview(planOrServer) -> AdminCommandResult.Resolved(planOrServer)
        resolveServer(planOrServer) -> AdminCommandResult.Resolved(planOrServer)
        else -> AdminCommandResult.Rejected("no NEEDS_REVIEW plan or uncertain backend matches '$planOrServer'")
    }
}
