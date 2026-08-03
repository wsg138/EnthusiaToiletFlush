package com.badgersmc.queuerestart.common.schedule

import com.badgersmc.queuerestart.common.protocol.RestartMode
import java.util.UUID

/**
 * A pending restart arm published by the proxy via SLP poll-back so a
 * companion with no online players can still discover and execute it.
 * Mirrors what `RestartNowMessage` carries on the plugin-message path.
 */
data class PendingArm(
    val deliveryId: UUID,
    val delaySeconds: Int,
    val mode: RestartMode,
    val argument: String,
)

/**
 * Wire format for [PendingArm] inside an SLP `samplePlayer.name`:
 * `QR_ARM:<delaySeconds>:<mode>:<argument>`. The argument tail may
 * contain `:` (preserved verbatim).
 *
 * The marker UUID [MARKER_UUID] disambiguates the entry from real player
 * samples and from the schedule-discovery marker. The proxy strips
 * QR_POLL pings before any real client sees them, but defence in depth
 * — using a marker UUID means even a leaked sample is still identifiable
 * as out-of-band metadata rather than a real player profile.
 */
object ArmEncoding {
    const val PREFIX: String = "QR_ARM:"
    val MARKER_UUID: UUID = UUID.fromString("00000000-0000-0000-0000-0000005152a0")

    fun encode(arm: PendingArm): String {
        require(arm.delaySeconds >= 0) { "delaySeconds must be ≥ 0" }
        require(arm.mode == RestartMode.SHUTDOWN) { "only SHUTDOWN arms are supported" }
        require(arm.argument.isEmpty()) { "SHUTDOWN arms must not contain an argument" }
        return "$PREFIX${arm.deliveryId}:${arm.delaySeconds}:${arm.mode.name}:${arm.argument}"
    }

    fun decode(name: String): PendingArm? {
        if (!name.startsWith(PREFIX)) return null
        val body = name.substring(PREFIX.length)
        val parts = body.split(':', limit = 4)
        if (parts.size != 4) return null
        val deliveryId = runCatching { UUID.fromString(parts[0]) }.getOrNull() ?: return null
        val delaySeconds = parts[1].toIntOrNull() ?: return null
        if (delaySeconds < 0) return null
        val mode = runCatching { RestartMode.valueOf(parts[2]) }.getOrNull() ?: return null
        if (mode != RestartMode.SHUTDOWN || parts[3].isNotEmpty()) return null
        return PendingArm(deliveryId, delaySeconds, mode, parts[3])
    }
}

/** Player-independent cancellation signal returned through the same SLP poll-back path as an arm. */
object CancelEncoding {
    const val VALUE: String = "QR_CANCEL"

    fun isCancel(name: String): Boolean = name == VALUE
}
