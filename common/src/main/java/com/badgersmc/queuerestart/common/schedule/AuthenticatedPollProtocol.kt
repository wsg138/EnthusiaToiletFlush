package com.badgersmc.queuerestart.common.schedule

import com.badgersmc.queuerestart.common.security.ControlAuthenticator
import com.badgersmc.queuerestart.common.security.ReplayWindow
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.time.Instant
import java.util.Base64
import java.util.UUID

object CompanionCapabilities {
    const val RESTART_SHUTDOWN: Int = 1
    const val ACKNOWLEDGED_DELIVERY: Int = 1 shl 1
    const val BOOT_IDENTITY: Int = 1 shl 2
    const val REQUIRED: Int = RESTART_SHUTDOWN or ACKNOWLEDGED_DELIVERY or BOOT_IDENTITY
}

data class AuthenticatedPollRequest(
    val serverId: String,
    val timestampSeconds: Long,
    val nonce: String,
    val bootId: UUID,
    val capabilities: Int,
    val acknowledgement: UUID?,
)

enum class PollSignalKind(val code: String) { ARM("A"), CANCEL("C") }

data class AuthenticatedPollSignal(
    val deliveryId: UUID,
    val kind: PollSignalKind,
    val payload: String,
)

/** Authenticated request/response encoding carried over Minecraft status ping metadata. */
class AuthenticatedPollProtocol(
    secret: String,
    private val nowSeconds: () -> Long = { Instant.now().epochSecond },
    private val maxClockSkewSeconds: Long = 45,
    private val replayWindow: ReplayWindow = ReplayWindow(),
    private val random: SecureRandom = SecureRandom(),
) {
    private val auth = ControlAuthenticator(secret)

    init {
        require(maxClockSkewSeconds in 1..300) { "maximum clock skew must be 1..300 seconds" }
    }

    fun newRequest(
        serverId: String,
        bootId: UUID,
        capabilities: Int,
        acknowledgement: UUID? = null,
    ): AuthenticatedPollRequest {
        require(capabilities >= 0) { "capabilities must not be negative" }
        return AuthenticatedPollRequest(
            serverId = validateServerId(serverId),
            timestampSeconds = nowSeconds(),
            nonce = randomNonce(),
            bootId = bootId,
            capabilities = capabilities,
            acknowledgement = acknowledgement,
        )
    }

    fun encodeRequest(request: AuthenticatedPollRequest): String {
        val sid = validateServerId(request.serverId)
        val timestamp = request.timestampSeconds.toString()
        val boot = compact(request.bootId)
        val caps = request.capabilities.toString(16)
        val ack = request.acknowledgement?.let(::compact) ?: "-"
        val signature = auth.sign(REQUEST_DOMAIN, sid, timestamp, request.nonce, boot, caps, ack)
        val encoded = listOf(REQUEST_PREFIX, sid, timestamp, request.nonce, boot, caps, ack, signature).joinToString(":")
        require(encoded.toByteArray(StandardCharsets.UTF_8).size <= MAX_HOST_LENGTH) {
            "authenticated poll hostname exceeds protocol limit"
        }
        return encoded
    }

    fun decodeRequest(hostname: String): AuthenticatedPollRequest? {
        val clean = hostname.substringBefore('\u0000').substringBefore(' ').substringBefore('\t')
        val parts = clean.split(':')
        if (parts.size != 8 || parts[0] != REQUEST_PREFIX) return null
        val sid = runCatching { validateServerId(parts[1]) }.getOrNull() ?: return null
        val timestamp = parts[2].toLongOrNull() ?: return null
        val nonce = parts[3]
        if (!NONCE_REGEX.matches(nonce)) return null
        val boot = parseCompactUuid(parts[4]) ?: return null
        val capabilities = parts[5].toIntOrNull(16)?.takeIf { it >= 0 } ?: return null
        val acknowledgement = if (parts[6] == "-") null else parseCompactUuid(parts[6]) ?: return null
        if (!auth.verify(parts[7], REQUEST_DOMAIN, sid, parts[2], nonce, parts[4], parts[5], parts[6])) return null
        val now = nowSeconds()
        if (timestamp < now - maxClockSkewSeconds || timestamp > now + maxClockSkewSeconds) return null
        if (!replayWindow.accept("$sid:$nonce", now)) return null
        return AuthenticatedPollRequest(sid, timestamp, nonce, boot, capabilities, acknowledgement)
    }

    fun encodeSignal(serverId: String, requestNonce: String, signal: AuthenticatedPollSignal): String {
        val sid = validateServerId(serverId)
        require(NONCE_REGEX.matches(requestNonce)) { "invalid request nonce" }
        require(signal.payload.toByteArray(StandardCharsets.UTF_8).size <= MAX_SIGNAL_PAYLOAD_BYTES) {
            "poll signal payload is too large"
        }
        val id = compact(signal.deliveryId)
        val payload = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(signal.payload.toByteArray(StandardCharsets.UTF_8))
        val signature = auth.sign(SIGNAL_DOMAIN, sid, requestNonce, id, signal.kind.code, payload)
        return listOf(SIGNAL_PREFIX, id, signal.kind.code, payload, requestNonce, signature).joinToString(":")
    }

    fun decodeSignal(serverId: String, expectedRequestNonce: String, encoded: String): AuthenticatedPollSignal? {
        val parts = encoded.split(':')
        if (parts.size != 6 || parts[0] != SIGNAL_PREFIX) return null
        if (parts[4] != expectedRequestNonce) return null
        val sid = runCatching { validateServerId(serverId) }.getOrNull() ?: return null
        val id = parseCompactUuid(parts[1]) ?: return null
        val kind = PollSignalKind.entries.firstOrNull { it.code == parts[2] } ?: return null
        if (!auth.verify(parts[5], SIGNAL_DOMAIN, sid, expectedRequestNonce, parts[1], parts[2], parts[3])) return null
        val bytes = runCatching { Base64.getUrlDecoder().decode(parts[3]) }.getOrNull() ?: return null
        if (bytes.size > MAX_SIGNAL_PAYLOAD_BYTES) return null
        return AuthenticatedPollSignal(id, kind, String(bytes, StandardCharsets.UTF_8))
    }

    private fun randomNonce(): String = ByteArray(16).also(random::nextBytes).let {
        Base64.getUrlEncoder().withoutPadding().encodeToString(it)
    }

    private fun validateServerId(serverId: String): String {
        require(serverId.matches(SERVER_ID_REGEX)) { "invalid server id" }
        return serverId
    }

    private fun compact(uuid: UUID): String = uuid.toString().replace("-", "")
    private fun parseCompactUuid(value: String): UUID? {
        if (!UUID_REGEX.matches(value)) return null
        val expanded = "${value.substring(0, 8)}-${value.substring(8, 12)}-${value.substring(12, 16)}-${value.substring(16, 20)}-${value.substring(20)}"
        return runCatching { UUID.fromString(expanded) }.getOrNull()
    }

    companion object {
        const val REQUEST_PREFIX = "QR2"
        const val SIGNAL_PREFIX = "QR2S"
        private const val REQUEST_DOMAIN = "queue-restart-poll-v2"
        private const val SIGNAL_DOMAIN = "queue-restart-signal-v2"
        private const val MAX_HOST_LENGTH = 255
        private const val MAX_SIGNAL_PAYLOAD_BYTES = 1024
        private val SERVER_ID_REGEX = Regex("[A-Za-z0-9_.-]{1,64}")
        private val NONCE_REGEX = Regex("[A-Za-z0-9_-]{22}")
        private val UUID_REGEX = Regex("[a-fA-F0-9]{32}")

        fun looksLikeRequest(hostname: String): Boolean = hostname.startsWith("$REQUEST_PREFIX:")
        fun looksLikeSignal(value: String): Boolean = value.startsWith("$SIGNAL_PREFIX:")
    }
}
