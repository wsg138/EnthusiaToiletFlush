package com.badgersmc.queuerestart.velocity.application.arm

import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.common.schedule.PendingArm
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Persistent, acknowledged delivery store for the player-independent SLP path.
 * Polling only peeks. A delivery is removed after an authenticated ACK carrying
 * the exact delivery id and expected backend boot id, so connection loss or a
 * stale process cannot silently steal an arm.
 */
class PendingArmStore(
    private val ttl: Duration = Duration.ofMinutes(10),
    private val cancelTtl: Duration = Duration.ofDays(7),
    private val persistencePath: Path? = null,
) {
    sealed interface Delivery {
        data class Arm(val value: PendingArm) : Delivery
        data object Cancel : Delivery
    }

    data class PendingDelivery(
        val id: UUID,
        val delivery: Delivery,
        val expectedBootId: UUID,
        val expiresAt: Instant,
    )

    private val slots = ConcurrentHashMap<ServerId, PendingDelivery>()

    init {
        require(!ttl.isNegative && !ttl.isZero) { "delivery TTL must be positive" }
        require(!cancelTtl.isNegative && !cancelTtl.isZero) { "cancel TTL must be positive" }
        load()
    }

    @Synchronized
    fun put(
        serverId: ServerId,
        mode: RestartMode,
        argument: String,
        delaySeconds: Int,
        expectedBootId: UUID,
        now: Instant,
    ): UUID {
        require(mode == RestartMode.SHUTDOWN) { "only SHUTDOWN delivery is supported" }
        require(delaySeconds >= 0) { "delaySeconds must not be negative" }
        val id = UUID.randomUUID()
        val arm = PendingArm(id, delaySeconds, mode, argument)
        slots[serverId] = PendingDelivery(id, Delivery.Arm(arm), expectedBootId, now.plus(ttl))
        save()
        return id
    }

    @Synchronized
    fun put(serverId: ServerId, arm: PendingArm, expectedBootId: UUID, now: Instant): UUID {
        require(arm.mode == RestartMode.SHUTDOWN) { "only SHUTDOWN delivery is supported" }
        require(arm.delaySeconds >= 0) { "delaySeconds must not be negative" }
        slots[serverId] = PendingDelivery(arm.deliveryId, Delivery.Arm(arm), expectedBootId, now.plus(ttl))
        save()
        return arm.deliveryId
    }

    /** Replaces any undelivered arm with an acknowledged cancellation tombstone. */
    @Synchronized
    fun cancel(serverId: ServerId, expectedBootId: UUID, now: Instant): UUID {
        val id = UUID.randomUUID()
        slots[serverId] = PendingDelivery(id, Delivery.Cancel, expectedBootId, now.plus(cancelTtl))
        save()
        return id
    }

    @Synchronized
    fun peekDelivery(serverId: ServerId, now: Instant): PendingDelivery? {
        val entry = slots[serverId] ?: return null
        if (!now.isBefore(entry.expiresAt)) {
            slots.remove(serverId, entry)
            save()
            return null
        }
        return entry
    }

    /**
     * Returns a delivery only to the authenticated JVM identity it was
     * prepared for. A replacement backend process must never execute an arm
     * left unacknowledged by the process that was actually targeted.
     */
    @Synchronized
    fun peekDeliveryForBoot(serverId: ServerId, bootId: UUID, now: Instant): PendingDelivery? {
        val entry = peekDelivery(serverId, now) ?: return null
        if (entry.expectedBootId == bootId) return entry
        slots.remove(serverId, entry)
        save()
        return null
    }

    @Synchronized
    fun acknowledge(serverId: ServerId, deliveryId: UUID, bootId: UUID, now: Instant): Boolean {
        val entry = slots[serverId] ?: return false
        if (!now.isBefore(entry.expiresAt)) {
            slots.remove(serverId, entry)
            save()
            return false
        }
        if (entry.id != deliveryId || entry.expectedBootId != bootId) return false
        val removed = slots.remove(serverId, entry)
        if (removed) save()
        return removed
    }

    fun peek(serverId: ServerId, now: Instant): PendingArm? =
        (peekDelivery(serverId, now)?.delivery as? Delivery.Arm)?.value

    @Synchronized
    fun clear(serverId: ServerId) {
        if (slots.remove(serverId) != null) save()
    }

    fun pendingCount(): Int = slots.size

    private fun load() {
        val path = persistencePath ?: return
        if (Files.notExists(path)) return
        try {
            Files.readAllLines(path, StandardCharsets.UTF_8)
                .filter(String::isNotBlank)
                .forEach { line ->
                    val parts = line.split('|')
                    require(parts.size == FIELD_COUNT && parts[0] == FORMAT_VERSION) {
                        "invalid pending delivery record"
                    }
                    val server = ServerId(text(parts[1]))
                    val id = UUID.fromString(parts[2])
                    val expectedBootId = UUID.fromString(parts[3])
                    val expires = Instant.parse(parts[4])
                    val delivery = when (parts[5]) {
                        "C" -> Delivery.Cancel
                        "A" -> Delivery.Arm(
                            PendingArm(
                                deliveryId = id,
                                delaySeconds = parts[6].toInt().also { require(it >= 0) },
                                mode = RestartMode.valueOf(parts[7]).also {
                                    require(it == RestartMode.SHUTDOWN) { "unsupported persisted restart mode" }
                                },
                                argument = text(parts[8]),
                            ),
                        )
                        else -> error("unknown pending delivery type")
                    }
                    require(slots.putIfAbsent(server, PendingDelivery(id, delivery, expectedBootId, expires)) == null) {
                        "duplicate pending delivery for ${server.value}"
                    }
                }
        } catch (error: Exception) {
            slots.clear()
            throw IllegalStateException(
                "pending control delivery state is corrupt; refusing to start to prevent lost or replayed restarts: $path",
                error,
            )
        }
    }

    @Synchronized
    private fun save() {
        val path = persistencePath ?: return
        val parent = path.parent
        parent?.let(Files::createDirectories)
        val tmp = path.resolveSibling("${path.fileName}.tmp")
        val content = slots.entries
            .sortedBy { it.key.value }
            .joinToString("\n", postfix = if (slots.isEmpty()) "" else "\n") { (server, entry) ->
                when (val delivery = entry.delivery) {
                    Delivery.Cancel -> listOf(
                        FORMAT_VERSION, b64(server.value), entry.id, entry.expectedBootId, entry.expiresAt,
                        "C", 0, RestartMode.SHUTDOWN, b64(""),
                    )
                    is Delivery.Arm -> listOf(
                        FORMAT_VERSION,
                        b64(server.value),
                        entry.id,
                        entry.expectedBootId,
                        entry.expiresAt,
                        "A",
                        delivery.value.delaySeconds,
                        delivery.value.mode,
                        b64(delivery.value.argument),
                    )
                }.joinToString("|")
            }
        FileChannel.open(
            tmp,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE,
        ).use { channel ->
            val bytes = ByteBuffer.wrap(content.toByteArray(StandardCharsets.UTF_8))
            while (bytes.hasRemaining()) channel.write(bytes)
            channel.force(true)
        }
        try {
            Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING)
        }
        if (parent != null) {
            FileChannel.open(parent, StandardOpenOption.READ).use { channel ->
                channel.force(true)
            }
        }
    }

    private fun b64(value: String): String = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(value.toByteArray(StandardCharsets.UTF_8))

    private fun text(value: String): String = String(
        Base64.getUrlDecoder().decode(value),
        StandardCharsets.UTF_8,
    )

    companion object {
        private const val FORMAT_VERSION = "3"
        private const val FIELD_COUNT = 9
    }
}
