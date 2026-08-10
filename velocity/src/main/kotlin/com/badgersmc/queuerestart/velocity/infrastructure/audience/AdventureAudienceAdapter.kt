package com.badgersmc.queuerestart.velocity.infrastructure.audience

import com.badgersmc.queuerestart.velocity.application.ports.AudiencePort
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.velocitypowered.api.proxy.Player
import com.velocitypowered.api.proxy.ProxyServer
import net.kyori.adventure.key.Key
import net.kyori.adventure.sound.Sound
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit

/**
 * REQ-003, REQ-004.
 *
 * AudiencePort backed by Velocity's proxy + Adventure. Iterates players
 * whose current backend matches [ServerId] and dispatches MiniMessage
 * components + Adventure sounds to each.
 */
class AdventureAudienceAdapter(
    private val proxyServer: ProxyServer,
    private val renderer: MiniMessageRenderer = MiniMessageRenderer(),
    private val logger: Logger = LoggerFactory.getLogger("queue-restart"),
) : AudiencePort {

    override fun broadcast(target: ServerId, miniMessage: String, placeholders: Map<String, String>) {
        val component = renderer.render(miniMessage, placeholders)
        val players = playersOn(target).toList()
        players.forEach { it.sendMessage(component) }
        logger.debug("broadcast to {} on {}: {}", players.size, target.value, miniMessage)
    }

    override fun disconnect(playerId: PlayerId, miniMessage: String, placeholders: Map<String, String>) {
        val player = proxyServer.getPlayer(playerId.uuid).orElse(null) ?: return
        player.disconnect(renderer.render(miniMessage, placeholders))
        logger.debug("disconnected {} with queue-restart message", player.username)
    }

    override fun disconnectAndAwait(
        playerId: PlayerId,
        miniMessage: String,
        placeholders: Map<String, String>,
    ): CompletionStage<Boolean> {
        val player = proxyServer.getPlayer(playerId.uuid).orElse(null)
            ?: return CompletableFuture.completedFuture(true)
        player.disconnect(renderer.render(miniMessage, placeholders))
        logger.debug("disconnect requested for {}; awaiting proxy settlement", player.username)
        return awaitPlayerGone(playerId, 0)
    }

    override fun playSound(target: ServerId, cue: SoundCue) {
        val sound = Sound.sound(
            Key.key(cue.key),
            Sound.Source.MASTER,
            cue.volume,
            cue.pitch,
        )
        val players = playersOn(target).toList()
        players.forEach { it.playSound(sound) }
        logger.info(
            "queue-restart: dispatched sound key={} vol={} pitch={} to {} player(s) on {}",
            cue.key, cue.volume, cue.pitch, players.size, target.value,
        )
    }

    private fun awaitPlayerGone(playerId: PlayerId, attempt: Int): CompletionStage<Boolean> {
        if (proxyServer.getPlayer(playerId.uuid).isEmpty) {
            return CompletableFuture.completedFuture(true)
        }
        if (attempt >= DISCONNECT_SETTLE_ATTEMPTS) {
            logger.warn("queue-restart: player {} remained connected after disconnect settle window", playerId.uuid)
            return CompletableFuture.completedFuture(false)
        }
        return CompletableFuture.runAsync(
            {},
            CompletableFuture.delayedExecutor(DISCONNECT_SETTLE_POLL_MILLIS, TimeUnit.MILLISECONDS),
        ).thenCompose { awaitPlayerGone(playerId, attempt + 1) }
    }

    private fun playersOn(target: ServerId): Sequence<Player> =
        proxyServer.allPlayers.asSequence()
            .filter { p -> p.currentServer.map { it.serverInfo.name == target.value }.orElse(false) }

    companion object {
        private const val DISCONNECT_SETTLE_POLL_MILLIS = 50L
        private const val DISCONNECT_SETTLE_ATTEMPTS = 100 // five seconds maximum
    }
}
