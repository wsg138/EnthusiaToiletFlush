package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.common.protocol.CheckOutcome
import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.common.schedule.BackendSchedule
import com.badgersmc.queuerestart.velocity.application.drain.DrainOrder
import com.badgersmc.queuerestart.velocity.application.drain.DrainPlanner
import com.badgersmc.queuerestart.velocity.application.drain.HubResolver
import com.badgersmc.queuerestart.velocity.application.drain.RejoinService
import com.badgersmc.queuerestart.velocity.application.gate.CheckGate
import com.badgersmc.queuerestart.velocity.application.ports.AudiencePort
import com.badgersmc.queuerestart.velocity.application.ports.CountdownConfig
import com.badgersmc.queuerestart.velocity.application.ports.DrainConfig
import com.badgersmc.queuerestart.velocity.application.ports.MessagingPort
import com.badgersmc.queuerestart.velocity.application.ports.ProxyPort
import com.badgersmc.queuerestart.velocity.application.ports.QueuePort
import com.badgersmc.queuerestart.velocity.application.ports.QueueRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.RejoinConfig
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.domain.cohort.Cohort
import com.badgersmc.queuerestart.velocity.domain.cohort.CohortMember
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.rank.RankLadder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

class RestartDrainSettlementTest {
    private val survival = ServerId("survival")
    private val hub = ServerId("lobby")
    private val player = PlayerId(UUID.nameUUIDFromBytes("player".toByteArray()))

    @Test
    fun `drain does not disconnect while Velocity transfer is still in flight`() {
        val proxy = SettlementProxy(player, hub)
        val transfer = CompletableFuture<Boolean>()
        proxy.transferResult = transfer
        val audience = SettlementAudience(proxy)
        val fixture = fixture(proxy, audience)
        val t0 = Instant.parse("2026-01-01T00:00:00Z")

        fixture.registry.get(survival).arm(cohort(), durationSeconds = 10)
        fixture.orchestrator.tick(t0)
        assertThat(fixture.orchestrator.prepareRestartHandoff(survival, fixture.baseline)).isTrue()
        fixture.orchestrator.tick(t0.plusSeconds(10))

        fixture.orchestrator.tick(t0.plusSeconds(12))
        assertThat(audience.disconnects).isEmpty()
        assertThat(fixture.messaging.restarts).isZero()

        transfer.complete(false)
        fixture.orchestrator.tick(t0.plusSeconds(13))

        assertThat(audience.disconnects).containsExactly(player)
        assertThat(fixture.messaging.restarts).isEqualTo(1)
    }

    @Test
    fun `empty source waits for in flight transfer result before restart`() {
        val proxy = SettlementProxy(player, hub)
        val transfer = CompletableFuture<Boolean>()
        proxy.transferResult = transfer
        val audience = SettlementAudience(proxy)
        val fixture = fixture(proxy, audience)
        val t0 = Instant.parse("2026-01-01T00:00:00Z")

        fixture.registry.get(survival).arm(cohort(), durationSeconds = 10)
        fixture.orchestrator.tick(t0)
        assertThat(fixture.orchestrator.prepareRestartHandoff(survival, fixture.baseline)).isTrue()
        fixture.orchestrator.tick(t0.plusSeconds(10))

        proxy.playersOnTarget.clear()
        fixture.orchestrator.tick(t0.plusSeconds(11))
        assertThat(fixture.messaging.restarts).isZero()

        transfer.complete(true)
        fixture.orchestrator.tick(t0.plusSeconds(12))
        assertThat(fixture.messaging.restarts).isEqualTo(1)
    }

    @Test
    fun `restart publication waits for fallback disconnect settlement`() {
        val proxy = SettlementProxy(player, hub)
        proxy.transferResult = CompletableFuture.completedFuture(false)
        val audience = SettlementAudience(proxy).apply { holdDisconnectOpen = true }
        val fixture = fixture(proxy, audience)
        val t0 = Instant.parse("2026-01-01T00:00:00Z")

        fixture.registry.get(survival).arm(cohort(), durationSeconds = 10)
        fixture.orchestrator.tick(t0)
        assertThat(fixture.orchestrator.prepareRestartHandoff(survival, fixture.baseline)).isTrue()
        fixture.orchestrator.tick(t0.plusSeconds(10))
        fixture.orchestrator.tick(t0.plusSeconds(12))

        assertThat(audience.disconnects).containsExactly(player)
        assertThat(fixture.messaging.restarts).isZero()

        proxy.playersOnTarget.remove(player)
        audience.disconnectResult.complete(true)
        fixture.orchestrator.tick(t0.plusSeconds(13))

        assertThat(fixture.messaging.restarts).isEqualTo(1)
    }

    @Test
    fun `force timeout publishes restart when disconnect settlement never completes`() {
        val proxy = SettlementProxy(player, hub)
        proxy.transferResult = CompletableFuture.completedFuture(false)
        val audience = SettlementAudience(proxy).apply { holdDisconnectOpen = true }
        val fixture = fixture(proxy, audience)
        val t0 = Instant.parse("2026-01-01T00:00:00Z")

        fixture.registry.get(survival).arm(cohort(), durationSeconds = 10)
        fixture.orchestrator.tick(t0)
        assertThat(fixture.orchestrator.prepareRestartHandoff(survival, fixture.baseline)).isTrue()
        fixture.orchestrator.tick(t0.plusSeconds(10))
        fixture.orchestrator.tick(t0.plusSeconds(12))

        assertThat(audience.disconnects).containsExactly(player)
        assertThat(audience.disconnectResult).isNotDone()
        assertThat(fixture.messaging.restarts).isZero()

        fixture.orchestrator.tick(t0.plusSeconds(130))

        assertThat(audience.disconnectResult).isNotDone()
        assertThat(fixture.messaging.restarts).isEqualTo(1)
    }

    private fun fixture(proxy: SettlementProxy, audience: SettlementAudience): Fixture {
        val registry = CoordinatorRegistry()
        val messaging = SettlementMessaging()
        val cfg = QueueRestartConfig(
            hubServer = hub,
            fallbackHubs = emptyList(),
            drain = DrainConfig(10, 40, 0, 120, DrainOrder.PRIORITY_ASC),
            rejoin = RejoinConfig(true, true, true, 60, true, 3),
            countdown = CountdownConfig(listOf(10, 5, 1), "warn", "now", "cancel"),
            sounds = emptyMap(),
            rankLadder = emptyMap(),
            rankDefault = 0,
        )
        val gate = CheckGate(timeoutSeconds = 60, releaseOnTimeout = true)
        val rejoin = RejoinService(proxy, SettlementQueue(), RankLadder(emptyMap(), 0), gate)
        val baseline = UUID.randomUUID()
        val orchestrator = RestartOrchestrator(
            registry = registry,
            proxy = proxy,
            messaging = messaging,
            audience = audience,
            broadcaster = CountdownBroadcaster(
                audience = audience,
                messageTemplate = cfg.countdown.message,
                t0Template = cfg.countdown.messageT0,
                soundResolver = { null },
            ),
            planner = DrainPlanner(),
            hubResolver = HubResolver(proxy),
            rejoin = rejoin,
            gate = gate,
            rankLadder = RankLadder(emptyMap(), 0),
            configSupplier = { cfg },
            companionIdentity = { baseline },
            onRestartPublished = { _, _ -> true },
        )
        orchestrator.start()
        return Fixture(orchestrator, registry, messaging, baseline)
    }

    private fun cohort() = Cohort(setOf(CohortMember(player)))

    private data class Fixture(
        val orchestrator: RestartOrchestrator,
        val registry: CoordinatorRegistry,
        val messaging: SettlementMessaging,
        val baseline: UUID,
    )

    private class SettlementProxy(player: PlayerId, private val hub: ServerId) : ProxyPort {
        val playersOnTarget = mutableSetOf(player)
        var transferResult: CompletionStage<Boolean> = CompletableFuture.completedFuture(true)

        override fun isOnline(playerId: PlayerId) = true
        override fun permissionsOf(playerId: PlayerId): Set<String> = emptySet()
        override fun isReachable(serverId: ServerId) = serverId == hub
        override fun playersOn(serverId: ServerId): Set<PlayerId> = playersOnTarget.toSet()
        override fun transferPlayer(playerId: PlayerId, target: ServerId) = Unit
        override fun transferPlayerAwaitable(playerId: PlayerId, target: ServerId): CompletionStage<Boolean> = transferResult
        override fun registeredServerIds(): Set<ServerId> = setOf(hub)
        override fun pingForSchedule(serverId: ServerId): BackendSchedule? = null
    }

    private class SettlementAudience(private val proxy: SettlementProxy) : AudiencePort {
        val disconnects = mutableListOf<PlayerId>()
        val disconnectResult = CompletableFuture<Boolean>()
        var holdDisconnectOpen = false

        override fun broadcast(target: ServerId, miniMessage: String, placeholders: Map<String, String>) = Unit
        override fun disconnect(playerId: PlayerId, miniMessage: String, placeholders: Map<String, String>) {
            disconnects += playerId
            proxy.playersOnTarget.remove(playerId)
        }
        override fun disconnectAndAwait(
            playerId: PlayerId,
            miniMessage: String,
            placeholders: Map<String, String>,
        ): CompletionStage<Boolean> {
            disconnects += playerId
            if (!holdDisconnectOpen) {
                proxy.playersOnTarget.remove(playerId)
                return CompletableFuture.completedFuture(true)
            }
            return disconnectResult
        }
        override fun playSound(target: ServerId, cue: SoundCue) = Unit
    }

    private class SettlementMessaging : MessagingPort {
        var restarts = 0
        override fun sendDrainRequest(target: ServerId) = Unit
        override fun sendRestartNow(
            target: ServerId,
            deliveryId: UUID,
            mode: RestartMode,
            argument: String,
            delaySeconds: Int,
        ) {
            restarts++
        }
        override fun sendRestartCancel(target: ServerId, deliveryId: UUID) = Unit
        override fun onDrainAck(handler: (ServerId, Int) -> Unit) = Unit
        override fun onCheckHacksResult(handler: (ServerId, PlayerId, CheckOutcome) -> Unit) = Unit
    }

    private class SettlementQueue : QueuePort {
        override fun enqueue(serverId: ServerId, playerId: PlayerId, weight: Int) = Unit
        override fun remove(playerId: PlayerId) = Unit
    }
}
