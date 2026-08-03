package com.badgersmc.queuerestart.paper

import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.common.schedule.ArmEncoding
import com.badgersmc.queuerestart.common.schedule.AuthenticatedPollProtocol
import com.badgersmc.queuerestart.common.schedule.CompanionCapabilities
import com.badgersmc.queuerestart.common.schedule.PollSignalKind
import org.bukkit.plugin.Plugin
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.logging.Level

/**
 * Authenticated, acknowledged, player-independent restart delivery and heartbeat.
 * Every poll is signed and carries a stable JVM boot id. Signals are signed back
 * to the exact request nonce, then acknowledged only after the companion has
 * durably accepted the delivery id through [RestartExecutor].
 */
class ProxyArmPoller(
    private val plugin: Plugin,
    private val proxyHost: String,
    private val proxyPort: Int,
    private val serverId: String,
    private val bootId: UUID,
    private val executor: RestartExecutor,
    private val protocol: AuthenticatedPollProtocol,
    private val pollIntervalSeconds: Int = 5,
    private val socketTimeoutMillis: Int = 3_000,
) {
    @Volatile private var taskId: Int = -1
    @Volatile private var inFlight: Boolean = false
    @Volatile private var consecutiveFailures: Int = 0

    fun start() {
        if (taskId != -1) return
        val periodTicks = pollIntervalSeconds.toLong() * 20L
        taskId = plugin.server.scheduler.runTaskTimerAsynchronously(plugin, Runnable(::pollOnce), 1L, periodTicks).taskId
        plugin.logger.info(
            "queue-restart: authenticated arm/heartbeat poller started " +
                "(proxy=$proxyHost:$proxyPort, server-id=$serverId, boot-id=$bootId, every ${pollIntervalSeconds}s)",
        )
    }

    fun stop() {
        if (taskId == -1) return
        plugin.server.scheduler.cancelTask(taskId)
        taskId = -1
    }

    private fun pollOnce() {
        if (inFlight) return
        inFlight = true
        try {
            val request = protocol.newRequest(serverId, bootId, CompanionCapabilities.REQUIRED)
            val response = fetchStatusJson(protocol.encodeRequest(request))
            val encoded = SIGNAL_REGEX.find(response)?.value ?: run {
                consecutiveFailures = 0
                return
            }
            val signal = protocol.decodeSignal(serverId, request.nonce, encoded) ?: run {
                plugin.logger.warning("queue-restart: rejected invalid authenticated poll signal")
                return
            }

            val accepted = when (signal.kind) {
                PollSignalKind.CANCEL -> runOnMainThread {
                    executor.abort(signal.deliveryId)
                    true
                }
                PollSignalKind.ARM -> {
                    val arm = ArmEncoding.decode(signal.payload) ?: run {
                        plugin.logger.warning("queue-restart: rejected malformed authenticated arm")
                        return
                    }
                    if (arm.deliveryId != signal.deliveryId || arm.mode != RestartMode.SHUTDOWN) {
                        plugin.logger.warning("queue-restart: rejected mismatched or non-SHUTDOWN poll arm")
                        return
                    }
                    runOnMainThread {
                        executor.execute(arm.deliveryId, arm.mode, arm.argument, arm.delaySeconds.coerceAtLeast(1))
                        true
                    }
                }
            }
            if (accepted) acknowledge(signal.deliveryId)
            consecutiveFailures = 0
        } catch (t: Throwable) {
            consecutiveFailures++
            if (consecutiveFailures == 1 || consecutiveFailures % 12 == 0) {
                plugin.logger.log(
                    Level.WARNING,
                    "queue-restart: authenticated control poll failed (#$consecutiveFailures): ${t.javaClass.simpleName}: ${t.message}",
                )
            }
        } finally {
            inFlight = false
        }
    }

    private fun acknowledge(deliveryId: UUID) {
        val request = protocol.newRequest(
            serverId = serverId,
            bootId = bootId,
            capabilities = CompanionCapabilities.REQUIRED,
            acknowledgement = deliveryId,
        )
        // The response is irrelevant; a successfully written/read status exchange
        // proves Velocity processed the signed ACK. Repeated ACKs are idempotent.
        fetchStatusJson(protocol.encodeRequest(request))
    }

    private fun runOnMainThread(action: () -> Boolean): Boolean {
        val result = CompletableFuture<Boolean>()
        plugin.server.scheduler.runTask(plugin, Runnable {
            runCatching(action).fold(result::complete) { result.completeExceptionally(it) }
        })
        return result.get(5, TimeUnit.SECONDS)
    }

    private fun fetchStatusJson(hostname: String): String {
        Socket().use { socket ->
            socket.soTimeout = socketTimeoutMillis
            socket.connect(InetSocketAddress(proxyHost, proxyPort), socketTimeoutMillis)
            val out = DataOutputStream(socket.getOutputStream())
            val input = DataInputStream(socket.getInputStream())

            val handshake = ByteArrayOutputStream()
            val hsOut = DataOutputStream(handshake)
            writeVarInt(hsOut, 0x00)
            writeVarInt(hsOut, -1)
            writeString(hsOut, hostname)
            hsOut.writeShort(proxyPort)
            writeVarInt(hsOut, 1)
            writePacket(out, handshake.toByteArray())

            val request = ByteArrayOutputStream()
            writeVarInt(DataOutputStream(request), 0x00)
            writePacket(out, request.toByteArray())
            out.flush()

            val packetLength = readVarInt(input)
            require(packetLength in 1..MAX_STATUS_PACKET_BYTES) { "invalid status packet length $packetLength" }
            val packet = ByteArray(packetLength)
            input.readFully(packet)
            val packetInput = DataInputStream(packet.inputStream())
            val packetId = readVarInt(packetInput)
            require(packetId == 0x00) { "unexpected status packet id $packetId" }
            val json = readString(packetInput, MAX_STATUS_JSON_BYTES)
            require(packetInput.available() == 0) { "trailing bytes in status packet" }
            return json
        }
    }

    companion object {
        private const val MAX_STATUS_PACKET_BYTES = 64 * 1024
        private const val MAX_STATUS_JSON_BYTES = 60 * 1024
        private val SIGNAL_REGEX = Regex("QR2S:[A-Za-z0-9_:\\-]*")

        private fun writeVarInt(out: DataOutputStream, valueIn: Int) {
            var value = valueIn
            while (true) {
                if ((value and 0x7F.inv()) == 0) { out.writeByte(value); return }
                out.writeByte((value and 0x7F) or 0x80)
                value = value ushr 7
            }
        }

        private fun readVarInt(input: DataInputStream): Int {
            var result = 0
            var shift = 0
            while (true) {
                val b = input.readUnsignedByte()
                result = result or ((b and 0x7F) shl shift)
                if ((b and 0x80) == 0) return result
                shift += 7
                require(shift < 35) { "VarInt too big" }
            }
        }

        private fun writeString(out: DataOutputStream, value: String) {
            val bytes = value.toByteArray(StandardCharsets.UTF_8)
            require(bytes.size <= 255) { "handshake hostname exceeds 255 bytes" }
            writeVarInt(out, bytes.size)
            out.write(bytes)
        }

        private fun readString(input: DataInputStream, maximumBytes: Int): String {
            val length = readVarInt(input)
            require(length in 0..maximumBytes) { "invalid status string length $length" }
            require(length <= input.available()) { "truncated status string" }
            val bytes = ByteArray(length)
            input.readFully(bytes)
            return String(bytes, StandardCharsets.UTF_8)
        }

        private fun writePacket(out: DataOutputStream, payload: ByteArray) {
            writeVarInt(out, payload.size)
            out.write(payload)
        }
    }
}
