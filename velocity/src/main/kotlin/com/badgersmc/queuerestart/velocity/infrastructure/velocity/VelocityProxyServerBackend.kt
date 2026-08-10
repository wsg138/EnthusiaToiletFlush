package com.badgersmc.queuerestart.velocity.infrastructure.velocity

import com.badgersmc.queuerestart.common.schedule.BackendSchedule
import com.badgersmc.queuerestart.common.schedule.ScheduleEncoding
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.velocitypowered.api.proxy.ProxyServer
import org.slf4j.Logger
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit

/**
 * REQ-014, REQ-031, REQ-033, REQ-034, REQ-043 binding. Implements
 * [VelocityProxyBackend] over Velocity's `ProxyServer` API.
 *
 * Untested in the unit suite (the [ProxyAdapter] tests cover the contract
 * the application layer depends on); validated by the e2e runbook.
 *
 * Reachability: `RegisteredServer.ping()` returns a `CompletableFuture` —
 * blocked on with [REACHABILITY_TIMEOUT] so a slow/dead backend doesn't
 * stall the proxy tick task. Failures (timeout / exception) are treated
 * as "unreachable" without surfacing a stack trace at INFO.
 */
class VelocityProxyServerBackend(
    private val proxy: ProxyServer,
    private val logger: Logger,
) : VelocityProxyBackend {

    override fun isOnline(playerId: PlayerId): Boolean =
        proxy.getPlayer(playerId.uuid).isPresent

    override fun permissionsOf(playerId: PlayerId): Set<String> {
        // Velocity's permission API is callback-style (`hasPermission(node)`)
        // — there's no enumerate-effective-permissions. The application
        // layer relies on this set to test for specific bypass nodes and
        // rank-ladder entries; we materialise it lazily via a
        // PermissionTrie or — simpler — by probing the well-known nodes
        // we care about. The set contains every node the player effectively
        // holds **out of those probed**.
        val player = proxy.getPlayer(playerId.uuid).orElse(null) ?: return emptySet()
        val held = mutableSetOf<String>()
        for (node in PROBED_PERMISSIONS) {
            if (player.hasPermission(node)) held += node
        }
        return held
    }

    override fun isReachable(serverId: ServerId): Boolean {
        val server = proxy.getServer(serverId.value).orElse(null) ?: return false
        return try {
            server.ping().get(REACHABILITY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
            true
        } catch (t: Throwable) {
            logger.debug("ping {} failed: {}", serverId.value, t.javaClass.simpleName)
            false
        }
    }

    override fun playersOn(serverId: ServerId): Set<PlayerId> {
        val server = proxy.getServer(serverId.value).orElse(null) ?: return emptySet()
        return server.playersConnected.asSequence()
            .map { PlayerId(it.uniqueId) }
            .toSet()
    }

    override fun registeredServerIds(): Set<ServerId> =
        proxy.allServers.asSequence().map { ServerId(it.serverInfo.name) }.toSet()

    override fun pingForSchedule(serverId: ServerId): BackendSchedule? {
        val server = proxy.getServer(serverId.value).orElse(null) ?: return null
        val ping = try {
            server.ping().get(REACHABILITY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
        } catch (t: Throwable) {
            logger.debug("schedule-ping {} failed: {}", serverId.value, t.javaClass.simpleName)
            return null
        }
        val samples = ping.players.orElse(null)?.sample ?: return null
        for (sp in samples) {
            if (sp.id != ScheduleEncoding.MARKER_UUID) continue
            val decoded = ScheduleEncoding.decode(sp.name) ?: continue
            return decoded
        }
        return null
    }

    override fun transferPlayer(playerId: PlayerId, target: ServerId) {
        transferPlayerAwaitable(playerId, target)
    }

    override fun transferPlayerAwaitable(playerId: PlayerId, target: ServerId): CompletionStage<Boolean> {
        val player = proxy.getPlayer(playerId.uuid).orElse(null)
        if (player == null) {
            logger.debug("transferPlayer: player {} offline; treating as drained", playerId.uuid)
            return CompletableFuture.completedFuture(true)
        }
        val server = proxy.getServer(target.value).orElse(null)
        if (server == null) {
            logger.warn("transferPlayer: server '{}' not registered; cannot move {}", target.value, player.username)
            return CompletableFuture.completedFuture(false)
        }
        return player.createConnectionRequest(server).connect().toCompletableFuture().handle { result, error ->
            val successful = error == null && result != null && result.isSuccessful
            if (!successful) {
                logger.warn(
                    "transferPlayer: failed to move {} to '{}': {}",
                    player.username,
                    target.value,
                    error?.javaClass?.simpleName ?: "unsuccessful connection result",
                )
            }
            successful
        }
    }

    /**
     * Permission nodes the queue-restart plugin needs to probe. Update if
     * a future REQ depends on a new node — the application layer is
     * blind to nodes outside this set.
     */
    fun probedPermissions(): Set<String> = PROBED_PERMISSIONS

    companion object {
        /** Hard-coded for now; rank-ladder entries can be appended via
         *  [withRankLadder]. */
        private val BASE_PROBES: Set<String> = setOf(
            "queuerestart.bypass.drain",       // REQ-014
            "queuerestart.bypass.checkhacks",  // REQ-043
            "queuerestart.bypass.maintenance",
            "queuerestart.command.schedrestart",
            "queuerestart.command.admin",
        )

        /**
         * Mutable to allow the entrypoint to register rank-ladder
         * permission nodes once the config has been parsed (REQ-033).
         */
        private var PROBED_PERMISSIONS: Set<String> = BASE_PROBES

        val REACHABILITY_TIMEOUT: Duration = Duration.ofSeconds(2)

        /**
         * Extend the permission probe set with the rank-ladder nodes
         * loaded from config so the application's [permissionsOf] can
         * resolve queue weights. Idempotent.
         */
        fun withRankLadder(nodes: Set<String>) {
            PROBED_PERMISSIONS = BASE_PROBES + nodes
        }
    }
}
