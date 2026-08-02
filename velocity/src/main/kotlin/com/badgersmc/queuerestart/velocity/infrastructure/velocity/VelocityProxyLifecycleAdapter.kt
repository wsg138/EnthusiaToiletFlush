package com.badgersmc.queuerestart.velocity.infrastructure.velocity

import com.badgersmc.queuerestart.velocity.application.ports.ProxyLifecyclePort
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.infrastructure.audience.MiniMessageRenderer
import com.velocitypowered.api.proxy.ProxyServer
import net.kyori.adventure.key.Key
import net.kyori.adventure.sound.Sound
import net.kyori.adventure.text.Component
import org.slf4j.Logger

/** Velocity API binding for proxy-wide countdown output and clean shutdown. */
class VelocityProxyLifecycleAdapter(
    private val proxy: ProxyServer,
    private val logger: Logger,
    private val renderer: MiniMessageRenderer = MiniMessageRenderer(),
) : ProxyLifecyclePort {

    override fun broadcast(miniMessage: String, placeholders: Map<String, String>) {
        val component = renderer.render(miniMessage, placeholders)
        val players = proxy.allPlayers.toList()
        players.forEach { it.sendMessage(component) }
        logger.debug("proxy-wide broadcast to {} player(s): {}", players.size, miniMessage)
    }

    override fun playSound(cue: SoundCue) {
        val sound = Sound.sound(
            Key.key(cue.key),
            Sound.Source.MASTER,
            cue.volume,
            cue.pitch,
        )
        val players = proxy.allPlayers.toList()
        players.forEach { it.playSound(sound) }
        logger.info(
            "queue-restart: dispatched proxy-wide sound key={} vol={} pitch={} to {} player(s)",
            cue.key, cue.volume, cue.pitch, players.size,
        )
    }

    override fun shutdown(reason: String) {
        logger.info("queue-restart: cleanly shutting down Velocity for scheduled proxy restart")
        proxy.shutdown(Component.text(reason))
    }
}
