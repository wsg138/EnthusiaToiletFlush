package com.badgersmc.queuerestart.velocity.infrastructure.messaging

import com.badgersmc.queuerestart.common.protocol.CheckHacksResultMessage
import com.badgersmc.queuerestart.common.protocol.CheckOutcome
import com.badgersmc.queuerestart.common.protocol.DrainAckMessage
import com.badgersmc.queuerestart.common.protocol.DrainRequestMessage
import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.common.protocol.RestartNowMessage
import com.badgersmc.queuerestart.common.security.AuthenticatedMessageCodec
import com.badgersmc.queuerestart.common.security.ControlDirection
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

class PluginMessageAdapterTest {
    private val secret = "0123456789abcdef0123456789abcdef"
    private fun secureCodec() = AuthenticatedMessageCodec(secret, nowSeconds = { 1_000L })

    private class FakeTransport : PluginMessageTransport {
        data class SentFrame(val target: ServerId, val payload: ByteArray)
        val sent = mutableListOf<SentFrame>()
        override fun send(target: ServerId, payload: ByteArray) { sent += SentFrame(target, payload) }
    }

    private val target = ServerId("survival")

    @Test
    fun `sendDrainRequest emits authenticated proxy frame`() {
        val transport = FakeTransport()
        val decoder = secureCodec()
        val adapter = PluginMessageAdapter(transport, secureCodec())
        adapter.sendDrainRequest(target)
        assertThat(decoder.decode(transport.sent.single().payload, ControlDirection.PROXY_TO_BACKEND, target.value))
            .isEqualTo(DrainRequestMessage)
    }

    @Test
    fun `sendRestartNow carries stable delivery id`() {
        val transport = FakeTransport()
        val decoder = secureCodec()
        val adapter = PluginMessageAdapter(transport, secureCodec())
        val id = UUID.randomUUID()
        adapter.sendRestartNow(target, id, RestartMode.SHUTDOWN, "stop", 30)
        assertThat(decoder.decode(transport.sent.single().payload, ControlDirection.PROXY_TO_BACKEND, target.value))
            .isEqualTo(RestartNowMessage(id, RestartMode.SHUTDOWN, "stop", 30))
    }

    @Test
    fun `authenticated inbound DrainAck dispatches`() {
        val adapter = PluginMessageAdapter(FakeTransport(), secureCodec())
        val received = mutableListOf<Pair<ServerId, Int>>()
        adapter.onDrainAck { server, count -> received += server to count }
        val payload = secureCodec().encode(DrainAckMessage(7), ControlDirection.BACKEND_TO_PROXY, target.value)
        adapter.handleInbound(target, payload)
        assertThat(received).containsExactly(target to 7)
    }

    @Test
    fun `authenticated CheckHacks result retains source server`() {
        val adapter = PluginMessageAdapter(FakeTransport(), secureCodec())
        val received = mutableListOf<Triple<ServerId, PlayerId, CheckOutcome>>()
        adapter.onCheckHacksResult { source, player, outcome -> received += Triple(source, player, outcome) }
        val pid = UUID.randomUUID()
        adapter.handleInbound(
            target,
            secureCodec().encode(CheckHacksResultMessage(pid, CheckOutcome.DETECTED), ControlDirection.BACKEND_TO_PROXY, target.value),
        )
        assertThat(received).containsExactly(Triple(target, PlayerId(pid), CheckOutcome.DETECTED))
    }

    @Test
    fun `wrong direction and unsigned payloads are ignored`() {
        val adapter = PluginMessageAdapter(FakeTransport(), secureCodec())
        val received = mutableListOf<Int>()
        adapter.onDrainAck { _, count -> received += count }
        adapter.handleInbound(target, secureCodec().encode(DrainAckMessage(3), ControlDirection.PROXY_TO_BACKEND, target.value))
        adapter.handleInbound(target, byteArrayOf(0x02, 0, 0, 0, 3))
        assertThat(received).isEmpty()
    }
}
