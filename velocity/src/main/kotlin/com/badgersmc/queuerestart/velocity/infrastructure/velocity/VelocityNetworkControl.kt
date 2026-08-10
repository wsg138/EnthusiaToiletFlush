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
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

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
        proxy.allPlayers.toList().forEach { it.disconnect(component) }

        // Player.disconnect() only requests a disconnect. The old implementation
        // returned immediately, allowing the following Pterodactyl restart to
        // kill Velocity before the client-side disconnect completed. This method
        // runs in the asynchronous network-restart dispatch chain, so a short,
        // bounded settlement barrier is preferable to racing process teardown.
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DISCONNECT_SETTLE_SECONDS)
        while (proxy.allPlayers.isNotEmpty() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(DISCONNECT_POLL_MILLIS))
        }
    }

    override fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary> {
        val players = proxy.getServer(from.value).orElse(null)?.playersConnected?.toList().orEmpty()
        val targets = destinations.mapNotNull { proxy.getServer(it.value).orElse(null) }
        val work = players.map { player -> move(player, targets, from) }
        return CompletableFuture.allOf(*work.toTypedArray()).thenApply {
            val results = work.map { it.getNow(TransferResult(false, false)) }
            val summary = TransferSummary(
                results.count { it.moved },
                results.count { it.disconnected },
                results.count { !it.moved && !it.disconnected },
            )
            check(summary.failed == 0) {
                "failed to move or disconnect ${summary.failed} player(s) from ${from.value} before network restart"
            }
            summary
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
                val playerId = player.uniqueId
                player.disconnect(
                    renderer.render(
                        accessMessages().drainDisconnect,
                        mapOf("server" to source.value),
                    ),
                )
                return awaitPlayerGone(playerId, 0).thenApply { disconnected ->
                    TransferResult(false, disconnected)
                }
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

    private fun awaitPlayerGone(playerId: UUID, attempt: Int): CompletableFuture<Boolean> {
        if (proxy.getPlayer(playerId).isEmpty) {
            return CompletableFuture.completedFuture(true)
        }
        if (attempt >= DISCONNECT_SETTLE_ATTEMPTS) {
            return CompletableFuture.completedFuture(false)
        }
        return CompletableFuture.runAsync(
            {},
            CompletableFuture.delayedExecutor(DISCONNECT_POLL_MILLIS, TimeUnit.MILLISECONDS),
        ).thenCompose { awaitPlayerGone(playerId, attempt + 1) }
    }

    private data class TransferResult(val moved: Boolean, val disconnected: Boolean)

    companion object {
        private const val DISCONNECT_SETTLE_SECONDS = 5L
        private const val DISCONNECT_POLL_MILLIS = 50L
        private const val DISCONNECT_SETTLE_ATTEMPTS = 100
    }
}
