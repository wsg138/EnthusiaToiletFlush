package com.badgersmc.queuerestart.paper

import com.badgersmc.queuerestart.common.protocol.CheckHacksResultMessage
import com.badgersmc.queuerestart.common.protocol.DrainAckMessage
import com.badgersmc.queuerestart.common.protocol.RestartCancelMessage
import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.common.protocol.RestartNowMessage
import com.badgersmc.queuerestart.common.security.AuthenticatedMessageCodec
import com.badgersmc.queuerestart.common.security.ControlDirection
import org.bukkit.entity.Player
import org.bukkit.plugin.Plugin
import org.bukkit.plugin.messaging.PluginMessageListener
import java.util.concurrent.atomic.AtomicLong
import java.util.logging.Level

/** Authenticated proxy-to-backend control-channel endpoint. */
class ProxyMessageListener(
    private val plugin: Plugin,
    private val serverId: String,
    private val executor: RestartExecutor,
    private val codec: AuthenticatedMessageCodec,
) : PluginMessageListener {
    private val nextInvalidWarningAtMillis = AtomicLong(0)

    override fun onPluginMessageReceived(channel: String, player: Player, message: ByteArray) {
        if (channel != CHANNEL) return
        val frame = try {
            codec.decode(message, ControlDirection.PROXY_TO_BACKEND, serverId)
        } catch (error: IllegalArgumentException) {
            warnInvalidFrame(error.message ?: "invalid authenticated frame")
            return
        } catch (error: Throwable) {
            plugin.logger.log(Level.WARNING, "queue-restart: control frame decode failed", error)
            return
        }

        when (frame) {
            is RestartNowMessage -> {
                if (frame.mode != RestartMode.SHUTDOWN) {
                    plugin.logger.warning(
                        "queue-restart: rejected authenticated non-SHUTDOWN restart mode ${frame.mode}",
                    )
                    return
                }
                try {
                    val accepted = executor.execute(
                        frame.deliveryId,
                        frame.mode,
                        frame.argument,
                        frame.delaySeconds,
                    )
                    plugin.logger.info(
                        if (accepted) {
                            "queue-restart: accepted RestartNow ${frame.deliveryId} (delaySeconds=${frame.delaySeconds})"
                        } else {
                            "queue-restart: ignored duplicate RestartNow ${frame.deliveryId}"
                        },
                    )
                } catch (error: Throwable) {
                    plugin.logger.log(Level.SEVERE, "queue-restart: RestartNow execution failed", error)
                }
            }
            is RestartCancelMessage -> {
                val cancelled = executor.abort(frame.deliveryId)
                plugin.logger.info(
                    if (cancelled) {
                        "queue-restart: accepted RestartCancel ${frame.deliveryId}; pending shutdown aborted"
                    } else {
                        "queue-restart: accepted RestartCancel ${frame.deliveryId}; no pending shutdown existed"
                    },
                )
            }
            else -> Unit
        }
    }

    fun sendDrainAck(remainingPlayers: Int) {
        send(codec.encode(DrainAckMessage(remainingPlayers), ControlDirection.BACKEND_TO_PROXY, serverId))
    }

    fun sendCheckHacksResult(message: CheckHacksResultMessage) {
        send(codec.encode(message, ControlDirection.BACKEND_TO_PROXY, serverId))
    }

    private fun send(payload: ByteArray) {
        val carrier = plugin.server.onlinePlayers.firstOrNull()
        if (carrier == null) {
            plugin.logger.warning("queue-restart: cannot send plugin message; no online player is available")
            return
        }
        carrier.sendPluginMessage(plugin, CHANNEL, payload)
    }

    private fun warnInvalidFrame(reason: String) {
        val now = System.currentTimeMillis()
        val allowedAt = nextInvalidWarningAtMillis.get()
        if (now < allowedAt || !nextInvalidWarningAtMillis.compareAndSet(allowedAt, now + INVALID_WARNING_INTERVAL_MILLIS)) {
            return
        }
        plugin.logger.warning("queue-restart: rejected invalid control frame: $reason")
    }

    companion object {
        const val CHANNEL = "qrestart:v1"
        private const val INVALID_WARNING_INTERVAL_MILLIS = 30_000L
    }
}
