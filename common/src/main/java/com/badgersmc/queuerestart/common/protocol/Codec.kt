package com.badgersmc.queuerestart.common.protocol

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.util.UUID

/**
 * Symmetric encode/decode for [Message] frames.
 *
 * Frame: `[u8 type][payload]`. Single source of truth for both proxy and
 * companion modules. See implementation.md §6.
 */
class Codec {

    fun encode(message: Message): ByteArray {
        val baos = ByteArrayOutputStream()
        DataOutputStream(baos).use { out ->
            when (message) {
                is DrainRequestMessage -> out.writeByte(TYPE_DRAIN_REQUEST.toInt())
                is DrainAckMessage -> {
                    out.writeByte(TYPE_DRAIN_ACK.toInt())
                    out.writeInt(message.remainingPlayers)
                }
                is RestartNowMessage -> {
                    out.writeByte(TYPE_RESTART_NOW.toInt())
                    out.writeLong(message.deliveryId.mostSignificantBits)
                    out.writeLong(message.deliveryId.leastSignificantBits)
                    out.writeByte(message.mode.code.toInt())
                    out.writeInt(message.delaySeconds)
                    out.writeUTF(message.argument)
                }
                is RestartCancelMessage -> {
                    out.writeByte(TYPE_RESTART_CANCEL.toInt())
                    out.writeLong(message.deliveryId.mostSignificantBits)
                    out.writeLong(message.deliveryId.leastSignificantBits)
                }
                is CheckHacksResultMessage -> {
                    out.writeByte(TYPE_CHECK_HACKS_RESULT.toInt())
                    out.writeLong(message.playerId.mostSignificantBits)
                    out.writeLong(message.playerId.leastSignificantBits)
                    out.writeByte(message.outcome.code.toInt())
                }
            }
        }
        return baos.toByteArray()
    }

    fun decode(frame: ByteArray): Message {
        require(frame.isNotEmpty()) { "Empty frame" }
        val input = DataInputStream(ByteArrayInputStream(frame))
        val type = input.readByte()
        return try {
            when (type) {
                TYPE_DRAIN_REQUEST -> DrainRequestMessage.also { requireFullyConsumed(input) }
                TYPE_DRAIN_ACK -> DrainAckMessage(input.readInt()).also { requireFullyConsumed(input) }
                TYPE_RESTART_NOW -> {
                    val deliveryId = UUID(input.readLong(), input.readLong())
                    val mode = RestartMode.fromCode(input.readByte())
                    val delaySeconds = input.readInt()
                    val arg = input.readUTF()
                    requireFullyConsumed(input)
                    RestartNowMessage(deliveryId, mode, arg, delaySeconds)
                }
                TYPE_RESTART_CANCEL -> RestartCancelMessage(UUID(input.readLong(), input.readLong())).also { requireFullyConsumed(input) }
                TYPE_CHECK_HACKS_RESULT -> {
                    val msb = input.readLong()
                    val lsb = input.readLong()
                    val outcome = CheckOutcome.fromCode(input.readByte())
                    requireFullyConsumed(input)
                    CheckHacksResultMessage(UUID(msb, lsb), outcome)
                }
                else -> throw IllegalArgumentException(
                    "Unknown message type: 0x${"%02X".format(type)}"
                )
            }
        } catch (e: EOFException) {
            throw IllegalArgumentException("Truncated frame for type 0x${"%02X".format(type)}", e)
        } catch (e: IOException) {
            throw IllegalArgumentException("Malformed frame for type 0x${"%02X".format(type)}", e)
        }
    }

    private fun requireFullyConsumed(input: DataInputStream) {
        if (input.available() > 0) {
            throw IllegalArgumentException("Trailing bytes in frame")
        }
    }

    private companion object {
        const val TYPE_DRAIN_REQUEST: Byte = 0x01
        const val TYPE_DRAIN_ACK: Byte = 0x02
        const val TYPE_RESTART_NOW: Byte = 0x10
        const val TYPE_RESTART_CANCEL: Byte = 0x11
        const val TYPE_CHECK_HACKS_RESULT: Byte = 0x20
    }
}
