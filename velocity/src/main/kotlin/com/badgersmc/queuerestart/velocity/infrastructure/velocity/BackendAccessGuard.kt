package com.badgersmc.queuerestart.velocity.infrastructure.velocity

import com.badgersmc.queuerestart.velocity.application.drain.HubResolver
import com.badgersmc.queuerestart.velocity.application.ports.QueueRestartConfig
import com.badgersmc.queuerestart.velocity.application.schedule.CoordinatorRegistry
import com.badgersmc.queuerestart.velocity.domain.coordinator.RestartState
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.infrastructure.audience.MiniMessageRenderer
import com.velocitypowered.api.event.PostOrder
import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.player.KickedFromServerEvent
import com.velocitypowered.api.event.player.ServerPreConnectEvent
import com.velocitypowered.api.proxy.ProxyServer
import com.velocitypowered.api.proxy.server.RegisteredServer
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.TranslatableComponent
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer

/**
 * Gives players a controlled response when they use an NPC/GUI to connect to
 * a backend that is restarting or currently rejecting joins through Paper's
 * whitelist. ServerPreConnectEvent can cancel a connection but cannot carry a
 * reason, so the guard sends the configured message before denying it.
 */
class BackendAccessGuard(
    private val proxy: ProxyServer,
    private val registry: CoordinatorRegistry,
    private val config: () -> QueueRestartConfig,
    private val hubResolver: HubResolver,
    private val renderer: MiniMessageRenderer = MiniMessageRenderer(),
) {
    @Subscribe(order = PostOrder.LAST)
    fun onPreConnect(event: ServerPreConnectEvent) {
        if (!event.result.isAllowed) return
        if (event.player.hasPermission("queuerestart.bypass.maintenance")) return

        val target = event.result.server.orElse(event.originalServer)
        if (!restartBlocksConnections(ServerId(target.serverInfo.name))) return

        event.player.sendMessage(
            renderer.render(
                config().accessMessages.backendRestarting,
                mapOf("server" to target.serverInfo.name),
            ),
        )
        event.result = ServerPreConnectEvent.ServerResult.denied()
    }

    @Subscribe(order = PostOrder.LAST)
    fun onBackendKick(event: KickedFromServerEvent) {
        val targetName = event.server.serverInfo.name
        val restarting = restartBlocksConnections(ServerId(targetName))
        val whitelisted = BackendKickReasonClassifier.isWhitelist(event.serverKickReason.orElse(null))
        if (!restarting && !whitelisted) return

        val template = if (restarting) {
            config().accessMessages.backendRestarting
        } else {
            config().accessMessages.backendWhitelisted
        }
        val message = renderer.render(template, mapOf("server" to targetName))

        // An NPC/GUI switch originates from an already-connected hub player.
        // Notify keeps them on their current server instead of disconnecting
        // the entire proxy session. If the kick happened after they were fully
        // on the backend, redirect to a configured hub when one is available.
        if (event.kickedDuringServerConnect() && event.player.currentServer.isPresent) {
            event.result = KickedFromServerEvent.Notify.create(message)
            return
        }

        val hub = resolveHub(excluding = event.server)
        event.result = if (hub != null) {
            KickedFromServerEvent.RedirectPlayer.create(hub, message)
        } else {
            KickedFromServerEvent.DisconnectPlayer.create(message)
        }
    }

    private fun restartBlocksConnections(serverId: ServerId): Boolean =
        registry.all()[serverId]?.state in BLOCKED_STATES

    private fun resolveHub(excluding: RegisteredServer): RegisteredServer? {
        val cfg = config()
        val candidates = (listOf(cfg.hubServer) + cfg.fallbackHubs)
            .filterNot { it.value == excluding.serverInfo.name }
        val primary = candidates.firstOrNull() ?: return null
        val selected = hubResolver.resolve(primary, candidates.drop(1)) ?: return null
        return proxy.getServer(selected.value).orElse(null)
    }

    companion object {
        private val BLOCKED_STATES = setOf(
            RestartState.DRAINING,
            RestartState.RESTART_SENT,
            RestartState.SERVER_DOWN,
        )
    }
}

internal object BackendKickReasonClassifier {
    private val plain = PlainTextComponentSerializer.plainText()

    fun isWhitelist(component: Component?): Boolean {
        if (component == null) return false
        if (containsWhitelistTranslation(component)) return true
        val text = plain.serialize(component).lowercase()
        return "whitelist" in text || "white-listed" in text || "white listed" in text
    }

    private fun containsWhitelistTranslation(component: Component): Boolean {
        if (component is TranslatableComponent && "whitelist" in component.key().lowercase()) return true
        return component.children().any(::containsWhitelistTranslation)
    }
}
