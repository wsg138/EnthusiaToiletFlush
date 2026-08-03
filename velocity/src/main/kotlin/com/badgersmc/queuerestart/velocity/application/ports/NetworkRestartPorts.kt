package com.badgersmc.queuerestart.velocity.application.ports

import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.plan.RestartPlan
import java.time.Duration
import java.util.concurrent.CompletionStage

data class PowerActionResult(val accepted: Boolean, val detail: String)

interface ExternalRestartExecutor {
    val name: String

    /** False for validation-only executors that must not move or disconnect players. */
    val performsPowerActions: Boolean get() = true

    /** Stable executor instance for one destructive plan execution. */
    fun snapshot(): ExternalRestartExecutor = this

    fun preflight(panelServerId: String): CompletionStage<PowerActionResult>
    fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult>
}

interface NetworkControlPort {
    fun broadcast(notice: RestartNotice)
    /** Play a countdown cue to every currently connected proxy player. */
    fun playSound(cue: SoundCue) = Unit
    fun disconnectAll(notice: RestartNotice)
    fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary>
    fun setMaintenance(enabled: Boolean, duration: Duration)
    fun maintenanceActive(): Boolean
}

data class RestartNotice(
    val type: String,
    val heading: String,
    val detail: String,
    val warning: String,
    val reason: String = "",
    val urgent: Boolean = false,
)

data class TransferSummary(val transferred: Int, val disconnected: Int, val failed: Int)

interface RestartPlanStore {
    fun load(): List<RestartPlan>
    fun save(plans: Collection<RestartPlan>)
}
