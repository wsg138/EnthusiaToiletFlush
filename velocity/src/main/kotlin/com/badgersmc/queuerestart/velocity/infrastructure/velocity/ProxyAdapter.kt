package com.badgersmc.queuerestart.velocity.infrastructure.velocity

import com.badgersmc.queuerestart.common.schedule.BackendSchedule
import com.badgersmc.queuerestart.velocity.application.ports.ProxyPort
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Thin abstraction over Velocity's `ProxyServer` / `Player` /
 * `RegisteredServer` API. The Velocity-bound impl
 * (`VelocityProxyServerBackend`, separate file) is registered at plugin
 * enable — kept untested here so the unit suite stays Velocity-free.
 */
interface VelocityProxyBackend {
    fun isOnline(playerId: PlayerId): Boolean
    fun permissionsOf(playerId: PlayerId): Set<String>
    fun isReachable(serverId: ServerId): Boolean
    fun playersOn(serverId: ServerId): Set<PlayerId>
    fun transferPlayer(playerId: PlayerId, target: ServerId)
    fun transferPlayerAwaitable(playerId: PlayerId, target: ServerId): CompletionStage<Boolean> {
        transferPlayer(playerId, target)
        return CompletableFuture.completedFuture(true)
    }
    fun registeredServerIds(): Set<ServerId>
    fun pingForSchedule(serverId: ServerId): BackendSchedule?
}

/**
 * REQ-014, REQ-031, REQ-033, REQ-034, REQ-043 adapter. Forwards every
 * proxy-state read and transfer command verbatim to the
 * [VelocityProxyBackend]. Carries no Velocity API types — the binding to
 * `ProxyServer` lives in the backend impl.
 */
class ProxyAdapter(private val backend: VelocityProxyBackend) : ProxyPort {

    override fun isOnline(playerId: PlayerId): Boolean =
        backend.isOnline(playerId)

    override fun permissionsOf(playerId: PlayerId): Set<String> =
        backend.permissionsOf(playerId)

    override fun isReachable(serverId: ServerId): Boolean =
        backend.isReachable(serverId)

    override fun playersOn(serverId: ServerId): Set<PlayerId> =
        backend.playersOn(serverId)

    override fun transferPlayer(playerId: PlayerId, target: ServerId) {
        backend.transferPlayer(playerId, target)
    }

    override fun transferPlayerAwaitable(playerId: PlayerId, target: ServerId): CompletionStage<Boolean> =
        backend.transferPlayerAwaitable(playerId, target)

    override fun registeredServerIds(): Set<ServerId> =
        backend.registeredServerIds()

    override fun pingForSchedule(serverId: ServerId): BackendSchedule? =
        backend.pingForSchedule(serverId)
}
