package com.badgersmc.queuerestart.common.security

import com.badgersmc.queuerestart.common.protocol.Codec
import com.badgersmc.queuerestart.common.protocol.Message
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.SecureRandom
import java.time.Instant
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

enum class ControlDirection(val code: Byte) {
    PROXY_TO_BACKEND(1),
    BACKEND_TO_PROXY(2);

    companion object {
        fun fromCode(code: Byte): ControlDirection = entries.firstOrNull { it.code == code }
            ?: throw IllegalArgumentException("unknown control direction: $code")
    }
}

/** Bounded replay window shared by the authenticated channel and SLP protocol. */
class ReplayWindow(
    val ttlSeconds: Long = 120,
    private val maxEntries: Int = 20_000,
) {
    private val seen = ConcurrentHashMap<String, Long>()

    init {
        require(ttlSeconds > 0) { "replay TTL must be positive" }
        require(maxEntries > 0) { "replay window capacity must be positive" }
    }

    fun accept(token: String, nowSeconds: Long): Boolean {
        cleanup(nowSeconds)
        if (seen.size >= maxEntries) return false
        return seen.putIfAbsent(token, nowSeconds + ttlSeconds) == null
    }

    fun cleanup(nowSeconds: Long) {
        seen.entries.removeIf { it.value <= nowSeconds }
    }
}

/** Length-prefixed HMAC helper to avoid delimiter ambiguity in textual protocols. */
class ControlAuthenticator(secret: String) {
    private val key = validateSecret(secret)

    fun sign(vararg fields: String): String = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(mac(canonical(fields.asList())))

    fun verify(signature: String, vararg fields: String): Boolean {
        val actual = runCatching { Base64.getUrlDecoder().decode(signature) }.getOrNull() ?: return false
        return MessageDigest.isEqual(mac(canonical(fields.asList())), actual)
    }

    internal fun mac(bytes: ByteArray): ByteArray {
        val mac = Mac.getInstance(HMAC_ALGORITHM)
        mac.init(SecretKeySpec(key, HMAC_ALGORITHM))
        return mac.doFinal(bytes)
    }

    private fun canonical(fields: List<String>): ByteArray {
        val out = ByteArrayOutputStream()
        DataOutputStream(out).use { data ->
            fields.forEach { field ->
                val bytes = field.toByteArray(StandardCharsets.UTF_8)
                data.writeInt(bytes.size)
                data.write(bytes)
            }
        }
        return out.toByteArray()
    }

    companion object {
        const val MIN_SECRET_BYTES = 32
        private const val MAX_SECRET_BYTES = 256
        private const val HMAC_ALGORITHM = "HmacSHA256"
        private const val CONFIGURATION_HINT =
            "set the same 32-256 UTF-8 byte random value in Velocity plugins/queue-restart/config.yml " +
                "(control-security.secret) and every Paper plugins/EnthusiaToiletFlush/config.yml " +
                "(control-secret), or provide QUEUE_RESTART_CONTROL_SECRET to every process"

        fun validateSecret(secret: String): ByteArray {
            val bytes = secret.toByteArray(StandardCharsets.UTF_8)
            require(bytes.isNotEmpty()) {
                "control secret is not configured; $CONFIGURATION_HINT"
            }
            require(bytes.size >= MIN_SECRET_BYTES) {
                "control secret is too short (${bytes.size} UTF-8 bytes); $CONFIGURATION_HINT"
            }
            require(bytes.size <= MAX_SECRET_BYTES) {
                "control secret is too long (${bytes.size} UTF-8 bytes); maximum is $MAX_SECRET_BYTES"
            }
            require(secret == secret.trim()) { "control secret must not start or end with whitespace" }
            require(!secret.contains("CHANGE_ME", ignoreCase = true)) { "control secret is still a placeholder" }
            return bytes
        }
    }
}

/**
 * Versioned, timestamped, replay-resistant envelope for qrestart plugin messages.
 * Direction and the logical backend peer are authenticated, preventing reflection
 * and cross-routing by network observers or packet tampering. Deployments that
 * share one control secret across all backends do not isolate a compromised
 * backend: a peer holding that secret can authenticate another peer id.
 */
class AuthenticatedMessageCodec(
    secret: String,
    private val nowSeconds: () -> Long = { Instant.now().epochSecond },
    private val maxClockSkewSeconds: Long = 60,
    private val replayWindow: ReplayWindow = ReplayWindow(),
    private val random: SecureRandom = SecureRandom(),
    private val payloadCodec: Codec = Codec(),
) {
    private val authenticator = ControlAuthenticator(secret)

    init {
        require(maxClockSkewSeconds in 1..300) { "maximum clock skew must be 1..300 seconds" }
        require(replayWindow.ttlSeconds >= 2 * maxClockSkewSeconds) {
            "replay window ttl must be at least twice the maximum clock skew"
        }
    }

    fun encode(message: Message, direction: ControlDirection, peerId: String): ByteArray {
        val peer = validatePeerId(peerId).toByteArray(StandardCharsets.US_ASCII)
        val payload = payloadCodec.encode(message)
        require(payload.size in 1..MAX_PAYLOAD_BYTES) { "control payload is too large" }
        val nonce = ByteArray(NONCE_BYTES).also(random::nextBytes)
        val unsigned = ByteArrayOutputStream().also { buffer ->
            DataOutputStream(buffer).use { out ->
                out.write(MAGIC)
                out.writeByte(VERSION)
                out.writeByte(direction.code.toInt())
                out.writeLong(nowSeconds())
                out.write(nonce)
                out.writeByte(peer.size)
                out.write(peer)
                out.writeInt(payload.size)
                out.write(payload)
            }
        }.toByteArray()
        return unsigned + authenticator.mac(unsigned)
    }

    fun decode(frame: ByteArray, expectedDirection: ControlDirection, expectedPeerId: String): Message {
        require(frame.size in MIN_FRAME_BYTES..MAX_FRAME_BYTES) { "invalid authenticated frame length" }
        val unsignedLength = frame.size - MAC_BYTES
        val unsigned = frame.copyOfRange(0, unsignedLength)
        val suppliedMac = frame.copyOfRange(unsignedLength, frame.size)
        require(MessageDigest.isEqual(authenticator.mac(unsigned), suppliedMac)) { "invalid control frame signature" }

        val input = DataInputStream(ByteArrayInputStream(unsigned))
        val magic = ByteArray(MAGIC.size).also(input::readFully)
        require(magic.contentEquals(MAGIC)) { "invalid control frame magic" }
        val version = input.readUnsignedByte()
        require(version == VERSION) { "unsupported control frame version $version" }
        val direction = ControlDirection.fromCode(input.readByte())
        require(direction == expectedDirection) { "unexpected control frame direction $direction" }
        val timestamp = input.readLong()
        val now = nowSeconds()
        require(timestamp >= now - maxClockSkewSeconds && timestamp <= now + maxClockSkewSeconds) {
            "stale control frame"
        }
        val nonce = ByteArray(NONCE_BYTES).also(input::readFully)
        val peerLength = input.readUnsignedByte()
        require(peerLength in 1..MAX_PEER_ID_BYTES) { "invalid control peer id length" }
        val peer = String(ByteArray(peerLength).also(input::readFully), StandardCharsets.US_ASCII)
        val expectedPeer = validatePeerId(expectedPeerId)
        require(peer == expectedPeer) { "control frame was addressed to a different backend" }
        val payloadLength = input.readInt()
        require(payloadLength in 1..MAX_PAYLOAD_BYTES) { "invalid control payload length" }
        require(input.available() == payloadLength) { "control frame length mismatch" }
        val payload = ByteArray(payloadLength).also(input::readFully)
        val replayKey = buildString {
            append(direction.code)
            append(':')
            append(peer)
            append(':')
            append(Base64.getUrlEncoder().withoutPadding().encodeToString(nonce))
        }
        require(replayWindow.accept(replayKey, now)) { "replayed control frame" }
        return payloadCodec.decode(payload)
    }

    private fun validatePeerId(peerId: String): String {
        require(PEER_ID_REGEX.matches(peerId)) { "invalid control peer id" }
        return peerId
    }

    companion object {
        private val MAGIC = byteArrayOf('Q'.code.toByte(), 'R'.code.toByte(), 'M'.code.toByte(), '2'.code.toByte())
        private const val VERSION = 1
        private const val NONCE_BYTES = 16
        private const val MAC_BYTES = 32
        private const val MAX_PEER_ID_BYTES = 64
        private const val MAX_PAYLOAD_BYTES = 24_000
        private const val MAX_FRAME_BYTES = 25_000
        private const val MIN_FRAME_BYTES = 4 + 1 + 1 + 8 + NONCE_BYTES + 1 + 1 + 4 + 1 + MAC_BYTES
        private val PEER_ID_REGEX = Regex("[A-Za-z0-9_.-]{1,64}")
    }
}
