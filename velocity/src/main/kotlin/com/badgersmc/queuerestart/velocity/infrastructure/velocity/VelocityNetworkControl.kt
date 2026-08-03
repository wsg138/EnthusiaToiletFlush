package com.badgersmc.queuerestart.velocity.infrastructure.velocity

import com.badgersmc.queuerestart.velocity.application.ports.AccessMessagesConfig
import com.badgersmc.queuerestart.velocity.application.ports.NetworkControlPort
import com.badgersmc.queuerestart.velocity.application.ports.RestartNotice
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.application.ports.TransferSummary
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.infrastructure.audience.MiniMessageRenderer
import com.velocitypowered.api.event.ResultedEvent
import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.connection.LoginEvent
import com.velocitypowered.api.proxy.Player
import com.velocitypowered.api.proxy.ProxyServer
import com.velocitypowered.api.proxy.server.RegisteredServer
import net.kyori.adventure.key.Key
import net.kyori.adventure.sound.Sound
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

class VelocityNetworkControl(
    private val proxy: ProxyServer,
    private val accessMessages: () -> AccessMessagesConfig = AccessMessagesConfig::defaults,
    private val renderer: MiniMessageRenderer = MiniMessageRenderer(),
    private val noticeRenderer: RestartNoticeRenderer = RestartNoticeRenderer(),
) : NetworkControlPort {
    @Volatile private var maintenanceUntil: Instant? = null

    override fun broadcast(notice: RestartNotice) {
        val component = noticeRenderer.notice(notice)
        proxy.allPlayers.forEach { it.sendMessage(component) }
    }

    override fun playSound(cue: SoundCue) {
        val sound = Sound.sound(Key.key(cue.key), Sound.Source.MASTER, cue.volume, cue.pitch)
        proxy.allPlayers.forEach { it.playSound(sound) }
    }

    override fun disconnectAll(notice: RestartNotice) {
        val component = noticeRenderer.disconnect(notice)
        proxy.allPlayers.forEach { it.disconnect(component) }
    }

    override fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary> {
        val players = proxy.getServer(from.value).orElse(null)?.playersConnected?.toList().orEmpty()
        val targets = destinations.mapNotNull { proxy.getServer(it.value).orElse(null) }
        val work = players.map { player -> move(player, targets, from) }
        return CompletableFuture.allOf(*work.toTypedArray()).thenApply {
            val results = work.map { it.getNow(TransferResult(false, true)) }
            TransferSummary(
                results.count { it.moved },
                results.count { it.disconnected },
                results.count { !it.moved && !it.disconnected },
            )
        }
    }

    override fun setMaintenance(enabled: Boolean, duration: Duration) {
        maintenanceUntil = if (enabled) Instant.now().plus(duration) else null
    }

    override fun maintenanceActive(): Boolean = maintenanceUntil?.isAfter(Instant.now()) == true

    @Subscribe
    fun onLogin(event: LoginEvent) {
        if (!maintenanceActive()) return
        if (event.player.hasPermission("queuerestart.bypass.maintenance")) return
        event.result = ResultedEvent.ComponentResult.denied(
            renderer.render(accessMessages().networkMaintenance, emptyMap()),
        )
    }

    private fun move(
        player: Player,
        targets: List<RegisteredServer>,
        source: ServerId,
    ): CompletableFuture<TransferResult> {
        fun next(index: Int): CompletableFuture<TransferResult> {
            if (index >= targets.size) {
                player.disconnect(
                    renderer.render(
                        accessMessages().drainDisconnect,
                        mapOf("server" to source.value),
                    ),
                )
                return CompletableFuture.completedFuture(TransferResult(false, true))
            }
            return player.createConnectionRequest(targets[index]).connect().toCompletableFuture().handle { result, error ->
                if (error == null && result.isSuccessful) {
                    CompletableFuture.completedFuture(TransferResult(true, false))
                } else {
                    next(index + 1)
                }
            }.thenCompose { it }
        }
        return next(0)
    }

    private data class TransferResult(val moved: Boolean, val disconnected: Boolean)
}
