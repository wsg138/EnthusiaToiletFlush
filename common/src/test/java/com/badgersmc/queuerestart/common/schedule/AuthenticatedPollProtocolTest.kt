package com.badgersmc.queuerestart.common.schedule

import com.badgersmc.queuerestart.common.security.ReplayWindow
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

class AuthenticatedPollProtocolTest {
    private val secret = "0123456789abcdef0123456789abcdef"

    @Test
    fun `signed request records boot identity capabilities and ack`() {
        val client = AuthenticatedPollProtocol(secret, nowSeconds = { 1_000 })
        val server = AuthenticatedPollProtocol(secret, nowSeconds = { 1_000 })
        val boot = UUID.randomUUID()
        val ack = UUID.randomUUID()
        val request = client.newRequest("SMP", boot, CompanionCapabilities.REQUIRED, ack)
        val decoded = server.decodeRequest(client.encodeRequest(request))
        assertThat(decoded).isNotNull
        assertThat(decoded!!.serverId).isEqualTo("SMP")
        assertThat(decoded.bootId).isEqualTo(boot)
        assertThat(decoded.acknowledgement).isEqualTo(ack)
    }

    @Test
    fun `forged stale and replayed requests are rejected`() {
        val client = AuthenticatedPollProtocol(secret, nowSeconds = { 1_000 })
        val server = AuthenticatedPollProtocol(secret, nowSeconds = { 1_000 })
        val encoded = client.encodeRequest(client.newRequest("SMP", UUID.randomUUID(), CompanionCapabilities.REQUIRED))
        assertThat(server.decodeRequest(encoded)).isNotNull
        assertThat(server.decodeRequest(encoded)).isNull()
        assertThat(AuthenticatedPollProtocol(secret, nowSeconds = { 2_000 }).decodeRequest(encoded)).isNull()
        assertThat(AuthenticatedPollProtocol("fedcba9876543210fedcba9876543210", nowSeconds = { 1_000 }).decodeRequest(encoded)).isNull()
    }

    @Test
    fun `poll replay window must cover the full accepted clock skew`() {
        assertThatThrownBy {
            AuthenticatedPollProtocol(
                secret,
                maxClockSkewSeconds = 45,
                replayWindow = ReplayWindow(ttlSeconds = 89),
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("twice the maximum clock skew")

        AuthenticatedPollProtocol(
            secret,
            maxClockSkewSeconds = 45,
            replayWindow = ReplayWindow(ttlSeconds = 90),
        )
    }

    @Test
    fun `signal is bound to server and request nonce`() {
        val protocol = AuthenticatedPollProtocol(secret, nowSeconds = { 1_000 })
        val request = protocol.newRequest("SMP", UUID.randomUUID(), CompanionCapabilities.REQUIRED)
        val signal = AuthenticatedPollSignal(UUID.randomUUID(), PollSignalKind.CANCEL, "")
        val encoded = protocol.encodeSignal("SMP", request.nonce, signal)
        assertThat(protocol.decodeSignal("SMP", request.nonce, encoded)).isEqualTo(signal)
        assertThat(protocol.decodeSignal("HUB", request.nonce, encoded)).isNull()
        assertThat(protocol.decodeSignal("SMP", "AAAAAAAAAAAAAAAAAAAAAA", encoded)).isNull()
    }
}
