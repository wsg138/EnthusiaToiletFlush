package com.badgersmc.queuerestart.common.protocol

import java.util.UUID

/**
 * Wire messages for channel `qrestart:v1`.
 * Frame: `[u8 type][payload]`. See implementation.md §6.
 */
sealed interface Message

object DrainRequestMessage : Message

data class DrainAckMessage(val remainingPlayers: Int) : Message

enum class RestartMode(val code: Byte) {
    SHUTDOWN(0x01),
    COMMAND(0x02),
    EXIT_CODE(0x03);

    companion object {
        fun fromCode(code: Byte): RestartMode =
            values().firstOrNull { it.code == code }
                ?: throw IllegalArgumentException("Unknown RestartMode code: 0x${"%02X".format(code)}")
    }
}

/**
 * `delaySeconds` lets the proxy schedule a restart while at least one player
 * is still on the target — Velocity's `RegisteredServer.sendPluginMessage`
 * silently drops if no player is connected. The companion receives this at
 * countdown start and defers the actual shutdown locally for [delaySeconds],
 * which keeps the restart on schedule even if the channel goes quiet later.
 */
data class RestartNowMessage(
    val deliveryId: UUID,
    val mode: RestartMode,
    val argument: String,
    val delaySeconds: Int = 0,
) : Message

/**
 * Aborts a `RestartNowMessage` that the companion already armed but
 * hasn't yet fired. Used by `/schedrestart cancel` on the proxy so a
 * cancelled countdown doesn't still result in an actual
 * `Bukkit.shutdown()` after `delaySeconds` elapses. Idempotent — if the
 * companion has no pending shutdown the message is a no-op.
 */
data class RestartCancelMessage(val deliveryId: UUID) : Message

enum class CheckOutcome(val code: Byte) {
    CLEAN(0x01),
    DETECTED(0x02),
    PROTECTED(0x03),
    TIMEOUT(0x04);

    companion object {
        fun fromCode(code: Byte): CheckOutcome =
            values().firstOrNull { it.code == code }
                ?: throw IllegalArgumentException("Unknown CheckOutcome code: 0x${"%02X".format(code)}")
    }
}

data class CheckHacksResultMessage(val playerId: UUID, val outcome: CheckOutcome) : Message
