package com.badgersmc.queuerestart.velocity.application.ports

import com.badgersmc.queuerestart.common.schedule.BackendSchedule
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Outbound port — read-only proxy state. Implemented by an adapter against
 * the Velocity API in `infrastructure/velocity/`.
 */
interface ProxyPort {
    /** True iff the player is currently connected to any backend. */
    fun isOnline(playerId: PlayerId): Boolean

    /** Permission nodes the player effectively holds (LuckPerms-resolved). */
    fun permissionsOf(playerId: PlayerId): Set<String>

    /** True iff the named backend is registered and reachable (ping). */
    fun isReachable(serverId: ServerId): Boolean

    /** Players currently connected to the named backend. */
    fun playersOn(serverId: ServerId): Set<PlayerId>

    /** Issue a transfer request to send [playerId] to [target]. */
    fun transferPlayer(playerId: PlayerId, target: ServerId)

    /**
     * Issue a transfer request and complete only when Velocity has resolved it.
     *
     * Existing test and non-Velocity adapters retain the legacy synchronous
     * contract through the default implementation. The production Velocity
     * adapter overrides this so restart draining can wait for a real connection
     * result instead of guessing from a fixed settle delay.
     */
    fun transferPlayerAwaitable(playerId: PlayerId, target: ServerId): CompletionStage<Boolean> {
        transferPlayer(playerId, target)
        return CompletableFuture.completedFuture(true)
    }

    /** Names of every backend registered with the proxy. */
    fun registeredServerIds(): Set<ServerId>

    /**
     * Open a Server-List-Ping to [serverId] and return the [BackendSchedule]
     * encoded by the companion in a sample-player entry, if any. Returns
     * `null` if the ping fails or the backend has no schedule sample. Used
     * by `ScheduleDiscoveryPoller` to learn each backend's restart cadence
     * without a player connection.
     */
    fun pingForSchedule(serverId: ServerId): BackendSchedule?
}
