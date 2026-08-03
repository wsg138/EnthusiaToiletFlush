package com.badgersmc.queuerestart.common.schedule

import com.badgersmc.queuerestart.common.protocol.RestartMode
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * REQ-022. SLP poll-back arm encoding mirrors [ScheduleEncoding] but
 * carries (delaySeconds, mode, argument) instead of times+zone+warn.
 * Distinct prefix + marker UUID so a single SLP response can carry both
 * a schedule sample and an arm sample without ambiguity.
 */
class ArmEncodingTest {

    @Test
    fun `cancel marker is recognized exactly`() {
        assertThat(CancelEncoding.isCancel(CancelEncoding.VALUE)).isTrue()
        assertThat(CancelEncoding.isCancel("QR_CANCEL_extra")).isFalse()
    }

    @Test
    fun `round trips a SHUTDOWN arm with empty argument`() {
        val original = PendingArm(
            deliveryId = UUID.randomUUID(),
            delaySeconds = 60,
            mode = RestartMode.SHUTDOWN,
            argument = "",
        )
        val encoded = ArmEncoding.encode(original)
        assertThat(encoded).isEqualTo("QR_ARM:${original.deliveryId}:60:SHUTDOWN:")
        assertThat(ArmEncoding.decode(encoded)).isEqualTo(original)
    }

    @Test
    fun `encode rejects COMMAND arms`() {
        val original = PendingArm(
            deliveryId = UUID.randomUUID(),
            delaySeconds = 30,
            mode = RestartMode.COMMAND,
            argument = "stop",
        )
        assertThatThrownBy { ArmEncoding.encode(original) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `encode rejects EXIT_CODE arms`() {
        val original = PendingArm(
            deliveryId = UUID.randomUUID(),
            delaySeconds = 0,
            mode = RestartMode.EXIT_CODE,
            argument = "137",
        )
        assertThatThrownBy { ArmEncoding.encode(original) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `decode rejects missing prefix`() {
        assertThat(ArmEncoding.decode("QR_SCHEDULE:nope")).isNull()
        assertThat(ArmEncoding.decode("not_a_marker")).isNull()
    }

    @Test
    fun `decode rejects malformed payload`() {
        assertThat(ArmEncoding.decode("QR_ARM:${UUID.randomUUID()}:notanumber:SHUTDOWN:")).isNull()
        assertThat(ArmEncoding.decode("QR_ARM:${UUID.randomUUID()}:30:NOT_A_MODE:")).isNull()
        assertThat(ArmEncoding.decode("QR_ARM:30")).isNull()
    }

    @Test
    fun `decode rejects command or argument-bearing arms`() {
        val id = UUID.randomUUID()
        assertThat(ArmEncoding.decode("QR_ARM:$id:5:COMMAND:say hello:world")).isNull()
        assertThat(ArmEncoding.decode("QR_ARM:$id:5:SHUTDOWN:stop")).isNull()
    }

    @Test
    fun `encode rejects negative delaySeconds`() {
        assertThatThrownBy {
            ArmEncoding.encode(PendingArm(UUID.randomUUID(), -1, RestartMode.SHUTDOWN, ""))
        }.isInstanceOf(IllegalArgumentException::class.java)
    }
}
