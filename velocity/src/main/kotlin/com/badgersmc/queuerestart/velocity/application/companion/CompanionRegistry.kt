package com.badgersmc.queuerestart.velocity.application.companion

import com.badgersmc.queuerestart.common.schedule.CompanionCapabilities
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/** Signed heartbeat/capability registry populated only by authenticated polls. */
class CompanionRegistry {
    data class Heartbeat(
        val bootId: UUID,
        val capabilities: Int,
        val lastSeen: Instant,
    )

    private val heartbeats = ConcurrentHashMap<ServerId, Heartbeat>()

    fun record(serverId: ServerId, bootId: UUID, capabilities: Int, now: Instant) {
        heartbeats[serverId] = Heartbeat(bootId, capabilities, now)
    }

    fun heartbeat(serverId: ServerId): Heartbeat? = heartbeats[serverId]

    fun compatibleHeartbeat(serverId: ServerId, now: Instant, timeout: Duration): Heartbeat? {
        val heartbeat = heartbeats[serverId] ?: return null
        if (Duration.between(heartbeat.lastSeen, now) > timeout) return null
        if (heartbeat.capabilities and CompanionCapabilities.REQUIRED != CompanionCapabilities.REQUIRED) return null
        return heartbeat
    }

    fun isCompatible(serverId: ServerId, now: Instant, timeout: Duration): Boolean =
        compatibleHeartbeat(serverId, now, timeout) != null
}
