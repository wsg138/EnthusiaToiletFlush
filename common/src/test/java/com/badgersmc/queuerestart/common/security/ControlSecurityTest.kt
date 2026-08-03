package com.badgersmc.queuerestart.common.security

import com.badgersmc.queuerestart.common.protocol.DrainRequestMessage
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class ControlSecurityTest {
    private val secret = "0123456789abcdef0123456789abcdef"

    @Test
    fun `authenticated frame round trips with direction binding`() {
        val encoder = AuthenticatedMessageCodec(secret, nowSeconds = { 1_000 })
        val decoder = AuthenticatedMessageCodec(secret, nowSeconds = { 1_000 })
        val frame = encoder.encode(DrainRequestMessage, ControlDirection.PROXY_TO_BACKEND, "SMP")
        assertThat(decoder.decode(frame, ControlDirection.PROXY_TO_BACKEND, "SMP")).isEqualTo(DrainRequestMessage)
    }

    @Test
    fun `tampering wrong direction stale and replay are rejected`() {
        val encoder = AuthenticatedMessageCodec(secret, nowSeconds = { 1_000 })
        val frame = encoder.encode(DrainRequestMessage, ControlDirection.PROXY_TO_BACKEND, "SMP")
        val tampered = frame.clone().also { it[10] = (it[10].toInt() xor 1).toByte() }
        assertThatThrownBy {
            AuthenticatedMessageCodec(secret, nowSeconds = { 1_000 }).decode(tampered, ControlDirection.PROXY_TO_BACKEND, "SMP")
        }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy {
            AuthenticatedMessageCodec(secret, nowSeconds = { 1_000 }).decode(frame, ControlDirection.BACKEND_TO_PROXY, "SMP")
        }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy {
            AuthenticatedMessageCodec(secret, nowSeconds = { 2_000 }).decode(frame, ControlDirection.PROXY_TO_BACKEND, "SMP")
        }.isInstanceOf(IllegalArgumentException::class.java)

        val decoder = AuthenticatedMessageCodec(secret, nowSeconds = { 1_000 })
        decoder.decode(frame, ControlDirection.PROXY_TO_BACKEND, "SMP")
        assertThatThrownBy { decoder.decode(frame, ControlDirection.PROXY_TO_BACKEND, "SMP") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `short and placeholder secrets fail closed`() {
        assertThatThrownBy { ControlAuthenticator.validateSecret("short") }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { ControlAuthenticator.validateSecret("CHANGE_ME_012345678901234567890123456789") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
