package com.badgersmc.queuerestart.velocity.infrastructure.messaging

import com.badgersmc.queuerestart.common.protocol.CheckHacksResultMessage
import com.badgersmc.queuerestart.common.protocol.CheckOutcome
import com.badgersmc.queuerestart.common.security.AuthenticatedMessageCodec
import com.badgersmc.queuerestart.common.security.ControlDirection
import com.badgersmc.queuerestart.common.protocol.DrainAckMessage
import com.badgersmc.queuerestart.common.protocol.DrainRequestMessage
import com.badgersmc.queuerestart.common.protocol.RestartCancelMessage
import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.common.protocol.RestartNowMessage
import com.badgersmc.queuerestart.velocity.application.ports.MessagingPort
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Pure send/receive abstraction. The Velocity-bound implementation
 * (`VelocityChannelTransport`, kept thin so it stays untested here) calls
 * `RegisteredServer.sendPluginMessage(channelIdentifier, payload)`.
 */
fun interface PluginMessageTransport {
    fun send(target: ServerId, payload: ByteArray)
}

/**
 * Adapter for [MessagingPort]. Encodes every message in an authenticated envelope and
 * dispatches inbound bytes to registered handlers. Velocity binding lives
 * in [VelocityChannelTransport] (separate file) — this class is fully
 * testable without the Velocity API on the classpath.
 *
 * implementation.md §6.
 */
class PluginMessageAdapter(
    private val transport: PluginMessageTransport,
    private val codec: AuthenticatedMessageCodec,
) : MessagingPort {

    // Handlers register at startup, dispatch on the Velocity event thread.
    private val drainAckHandlers = CopyOnWriteArrayList<(ServerId, Int) -> Unit>()
    private val checkResultHandlers = CopyOnWriteArrayList<(ServerId, PlayerId, CheckOutcome) -> Unit>()

    override fun sendDrainRequest(target: ServerId) {
        transport.send(target, codec.encode(DrainRequestMessage, ControlDirection.PROXY_TO_BACKEND, target.value))
    }

    override fun sendRestartNow(target: ServerId, deliveryId: UUID, mode: RestartMode, argument: String, delaySeconds: Int) {
        transport.send(target, codec.encode(RestartNowMessage(deliveryId, mode, argument, delaySeconds), ControlDirection.PROXY_TO_BACKEND, target.value))
    }

    override fun sendRestartCancel(target: ServerId, deliveryId: UUID) {
        transport.send(target, codec.encode(RestartCancelMessage(deliveryId), ControlDirection.PROXY_TO_BACKEND, target.value))
    }

    override fun onDrainAck(handler: (ServerId, Int) -> Unit) {
        drainAckHandlers += handler
    }

    override fun onCheckHacksResult(handler: (ServerId, PlayerId, CheckOutcome) -> Unit) {
        checkResultHandlers += handler
    }

    /**
     * Entry point for inbound bytes from any backend. Malformed frames are
     * swallowed — the Velocity-bound transport is responsible for logging.
     */
    fun handleInbound(source: ServerId, payload: ByteArray) {
        val message = try {
            codec.decode(payload, ControlDirection.BACKEND_TO_PROXY, source.value)
        } catch (_: IllegalArgumentException) {
            return
        }
        when (message) {
            is DrainAckMessage -> drainAckHandlers.forEach { it(source, message.remainingPlayers) }
            is CheckHacksResultMessage -> {
                // SECURITY (REQ-090, finding B): forward the source server
                // alongside the verdict so the rejoin service can verify
                // the player is actually on the source backend before
                // accepting the verdict. Without this, any backend can
                // forge a CLEAN verdict for any player on any other
                // backend → anti-cheat bypass.
                checkResultHandlers.forEach {
                    it(source, PlayerId(message.playerId), message.outcome)
                }
            }
            else -> { /* proxy→backend frames travelling the wrong way — ignore */ }
        }
    }
}
