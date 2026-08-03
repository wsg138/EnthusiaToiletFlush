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

    private fun playersOn(target: ServerId): Sequence<Player> =
        proxyServer.allPlayers.asSequence()
            .filter { p -> p.currentServer.map { it.serverInfo.name == target.value }.orElse(false) }
}
