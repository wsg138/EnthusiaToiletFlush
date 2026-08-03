package com.badgersmc.queuerestart.common.protocol

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * REQ-020, implementation.md §6.
 * Frame: [u8 type][payload].
 *  0x01 DrainRequest      — no body
 *  0x02 DrainAck          — i32 remainingPlayers
 *  0x10 RestartNow        — u8 mode, i32 delaySeconds, string arg
 *  0x20 CheckHacksResult  — uuid playerId, u8 outcome
 */
class CodecTest {

    private val codec = Codec()

    @Test
    fun `drain request round trips and is single byte frame`() {
        val original = DrainRequestMessage
        val encoded = codec.encode(original)

        assertThat(encoded).hasSize(1)
        assertThat(encoded[0]).isEqualTo(0x01.toByte())
        assertThat(codec.decode(encoded)).isEqualTo(original)
    }

    @Test
    fun `drain ack round trips`() {
        val original = DrainAckMessage(remainingPlayers = 12)
        val encoded = codec.encode(original)

        assertThat(encoded[0]).isEqualTo(0x02.toByte())
        assertThat(codec.decode(encoded)).isEqualTo(original)
    }

    @Test
    fun `drain ack round trips at edges`() {
        assertThat(codec.decode(codec.encode(DrainAckMessage(0))))
            .isEqualTo(DrainAckMessage(0))
        assertThat(codec.decode(codec.encode(DrainAckMessage(Int.MAX_VALUE))))
            .isEqualTo(DrainAckMessage(Int.MAX_VALUE))
    }

    @Test
    fun `restart now round trips for every mode`() {
        for (mode in RestartMode.values()) {
            val original = RestartNowMessage(deliveryId = UUID.randomUUID(), mode = mode, argument = "stop", delaySeconds = 30)
            val encoded = codec.encode(original)
            assertThat(encoded[0]).isEqualTo(0x10.toByte())
            assertThat(codec.decode(encoded)).isEqualTo(original)
        }
    }

    @Test
    fun `restart now round trips with empty and unicode argument`() {
        val empty = RestartNowMessage(UUID.randomUUID(), RestartMode.SHUTDOWN, "", delaySeconds = 0)
        assertThat(codec.decode(codec.encode(empty))).isEqualTo(empty)

        val unicode = RestartNowMessage(UUID.randomUUID(), RestartMode.COMMAND, "rëstart 世界", delaySeconds = 60)
        assertThat(codec.decode(codec.encode(unicode))).isEqualTo(unicode)
    }

    @Test
    fun `restart now delaySeconds round trips at edges`() {
        val zero = RestartNowMessage(UUID.randomUUID(), RestartMode.SHUTDOWN, "stop", delaySeconds = 0)
        assertThat(codec.decode(codec.encode(zero))).isEqualTo(zero)

        val max = RestartNowMessage(UUID.randomUUID(), RestartMode.SHUTDOWN, "stop", delaySeconds = Int.MAX_VALUE)
        assertThat(codec.decode(codec.encode(max))).isEqualTo(max)
    }

    @Test
    fun `check hacks result round trips for every outcome`() {
        val id = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")
        for (outcome in CheckOutcome.values()) {
            val original = CheckHacksResultMessage(playerId = id, outcome = outcome)
            val encoded = codec.encode(original)
            assertThat(encoded[0]).isEqualTo(0x20.toByte())
            assertThat(codec.decode(encoded)).isEqualTo(original)
        }
    }

    @Test
    fun `decode rejects empty frame`() {
        assertThatThrownBy { codec.decode(ByteArray(0)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `decode rejects unknown type byte`() {
        assertThatThrownBy { codec.decode(byteArrayOf(0x7F)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `decode rejects truncated drain ack`() {
        assertThatThrownBy { codec.decode(byteArrayOf(0x02, 0x00)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `decode rejects truncated check hacks result`() {
        assertThatThrownBy { codec.decode(byteArrayOf(0x20, 0x00, 0x00)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `decode rejects unknown outcome byte`() {
        val frame = codec.encode(CheckHacksResultMessage(UUID.randomUUID(), CheckOutcome.CLEAN))
        frame[frame.size - 1] = 0x7E.toByte()
        assertThatThrownBy { codec.decode(frame) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `decode rejects unknown restart mode byte`() {
        val frame = codec.encode(RestartNowMessage(UUID.randomUUID(), RestartMode.SHUTDOWN, "x", delaySeconds = 0))
        frame[17] = 0x7E.toByte()
        assertThatThrownBy { codec.decode(frame) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
