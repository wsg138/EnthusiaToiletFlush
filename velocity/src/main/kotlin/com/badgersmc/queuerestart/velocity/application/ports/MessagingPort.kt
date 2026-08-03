package com.badgersmc.queuerestart.velocity.application.ports

import com.badgersmc.queuerestart.common.protocol.CheckOutcome
import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.util.UUID

/**
 * Outbound port — sends plugin messages on channel `qrestart:v1` and
 * exposes inbound subscriptions. Implemented by
 * `infrastructure/messaging/PluginMessageAdapter`.
 */
interface MessagingPort {
    /** Send `DrainRequest` (0x01) to the named backend. */
    fun sendDrainRequest(target: ServerId)

    /**
     * Send `RestartNow` (0x10) to the named backend. For managed backend
     * restarts this is sent with a zero delay only after the T-0 drain has
     * completed. The independent SLP poll-back channel supplies the same
     * immediate arm when no player remains to carry a plugin message.
     */
    fun sendRestartNow(target: ServerId, deliveryId: UUID, mode: RestartMode, argument: String, delaySeconds: Int)

    /**
     * Abort a previously sent `RestartNow` that the companion already
     * scheduled. Used by `/schedrestart cancel` so a cancelled countdown
     * doesn't still result in `Bukkit.shutdown()` after the delay
     * elapses. Idempotent.
     */
    fun sendRestartCancel(target: ServerId, deliveryId: UUID)

    /** Register a handler for `DrainAck` (0x02) from any backend. */
    fun onDrainAck(handler: (ServerId, Int) -> Unit)

    /**
     * Register a handler for `CheckHacksResult` (0x20). Handler receives
     * the SOURCE backend that delivered the verdict — REQ-090 (finding B)
     * — so the application layer can reject cross-backend spoofs from a
     * compromised minigame backend forging CLEAN for a player it doesn't
     * actually host.
     */
    fun onCheckHacksResult(handler: (ServerId, PlayerId, CheckOutcome) -> Unit)
}
