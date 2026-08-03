package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.common.protocol.CheckOutcome
import com.badgersmc.queuerestart.common.protocol.RestartMode
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
import com.badgersmc.queuerestart.velocity.domain.coordinator.RestartState
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.rank.RankLadder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

/**
 * REQ-001, REQ-010, REQ-012, REQ-020, REQ-040.
 *
 * Drives every coordinator's state machine forward on each tick.
 */
class RestartOrchestratorTest {

    private val survival = ServerId("survival")
    private val hub = ServerId("lobby")

    private fun pid(name: String) = PlayerId(UUID.nameUUIDFromBytes(name.toByteArray()))

    private class FakeProxy(
        var playersOnTarget: MutableSet<PlayerId> = mutableSetOf(),
        var reachable: MutableSet<ServerId> = mutableSetOf(),
        val perms: MutableMap<PlayerId, Set<String>> = mutableMapOf(),
        val transfers: MutableList<Pair<PlayerId, ServerId>> = mutableListOf(),
        var transferSucceeds: Boolean = true,
    ) : ProxyPort {
        override fun isOnline(playerId: PlayerId) = true
        override fun permissionsOf(playerId: PlayerId) = perms[playerId] ?: emptySet()
        override fun isReachable(serverId: ServerId) = serverId in reachable
        override fun playersOn(serverId: ServerId) = playersOnTarget.toSet()
        override fun transferPlayer(playerId: PlayerId, target: ServerId) {
            transfers += playerId to target
            if (transferSucceeds) playersOnTarget.remove(playerId)
        }
        override fun registeredServerIds(): Set<ServerId> = reachable.toSet()
        override fun pingForSchedule(serverId: ServerId): com.badgersmc.queuerestart.common.schedule.BackendSchedule? = null
    }

    private class FakeMessaging : MessagingPort {
        data class RestartCall(val server: ServerId, val mode: RestartMode, val arg: String, val delaySeconds: Int)
        val drainSent = mutableListOf<ServerId>()
        val restartSent = mutableListOf<RestartCall>()
        val cancelSent = mutableListOf<ServerId>()
        var checkResultHandler: ((ServerId, PlayerId, CheckOutcome) -> Unit)? = null
        var drainAckHandler: ((ServerId, Int) -> Unit)? = null
        override fun sendDrainRequest(target: ServerId) { drainSent += target }
        override fun sendRestartNow(target: ServerId, mode: RestartMode, argument: String, delaySeconds: Int) {
            restartSent += RestartCall(target, mode, argument, delaySeconds)
        }
        override fun sendRestartCancel(target: ServerId) { cancelSent += target }
        override fun onDrainAck(handler: (ServerId, Int) -> Unit) { drainAckHandler = handler }
        override fun onCheckHacksResult(handler: (ServerId, PlayerId, CheckOutcome) -> Unit) {
            checkResultHandler = handler
        }
    }

    private class FakeAudience : AudiencePort {
        data class DisconnectCall(
            val playerId: PlayerId,
            val message: String,
            val placeholders: Map<String, String>,
        )
        val broadcasts = mutableListOf<String>()
        val disconnects = mutableListOf<DisconnectCall>()
        var onDisconnect: (PlayerId) -> Unit = {}
        override fun broadcast(target: ServerId, miniMessage: String, placeholders: Map<String, String>) {
            broadcasts += miniMessage
        }
        override fun disconnect(playerId: PlayerId, miniMessage: String, placeholders: Map<String, String>) {
            disconnects += DisconnectCall(playerId, miniMessage, placeholders)
            onDisconnect(playerId)
        }
        override fun playSound(target: ServerId, cue: SoundCue) {}
    }

    private class FakeQueue : QueuePort {
        override fun enqueue(serverId: ServerId, playerId: PlayerId, weight: Int) {}
        override fun remove(playerId: PlayerId) {}
    }

    private fun config(
        drainLead: Int = 30,
        forceTimeout: Int = 120,
        batchSize: Int = 10,
        batchInterval: Int = 40,
    ) = QueueRestartConfig(
        hubServer = hub,
        fallbackHubs = emptyList(),
        drain = DrainConfig(batchSize, batchInterval, drainLead, forceTimeout, DrainOrder.PRIORITY_ASC),
        rejoin = RejoinConfig(true, true, true, 60, true, 3),
        countdown = CountdownConfig(listOf(60, 30, 10, 5, 1), "<gold>warn", "<red>now", "<green>cancel"),
        sounds = emptyMap(),
        rankLadder = emptyMap(),
        rankDefault = 0,
    )

    private fun setup(
        cfg: QueueRestartConfig = config(),
        proxy: FakeProxy = FakeProxy(reachable = mutableSetOf(hub)),
        messaging: FakeMessaging = FakeMessaging(),
        audience: FakeAudience = FakeAudience(),
        pendingArmStore: com.badgersmc.queuerestart.velocity.application.arm.PendingArmStore =
            com.badgersmc.queuerestart.velocity.application.arm.PendingArmStore(),
    ): Pair<RestartOrchestrator, Bundle> {
        audience.onDisconnect = { proxy.playersOnTarget.remove(it) }
        val registry = CoordinatorRegistry()
        val broadcaster = CountdownBroadcaster(
            audience = audience,
            messageTemplate = cfg.countdown.message,
            t0Template = cfg.countdown.messageT0,
            soundResolver = { null },
        )
        val gate = CheckGate(timeoutSeconds = 60, releaseOnTimeout = true)
        val rejoin = RejoinService(proxy, FakeQueue(), RankLadder(emptyMap(), 0), gate)
        val orch = RestartOrchestrator(
            registry = registry,
            proxy = proxy,
            messaging = messaging,
            audience = audience,
            broadcaster = broadcaster,
            planner = DrainPlanner(),
            hubResolver = HubResolver(proxy),
            rejoin = rejoin,
            gate = gate,
            rankLadder = RankLadder(emptyMap(), 0),
            configSupplier = { cfg },
            restartMode = RestartMode.SHUTDOWN,
            restartArg = "",
            pendingArmStore = pendingArmStore,
        )
        orch.start()
        return orch to Bundle(registry, proxy, messaging, audience, gate, pendingArmStore)
    }

    private data class Bundle(
        val registry: CoordinatorRegistry,
        val proxy: FakeProxy,
        val messaging: FakeMessaging,
        val audience: FakeAudience,
        val gate: CheckGate,
        val pendingArmStore: com.badgersmc.queuerestart.velocity.application.arm.PendingArmStore,
    )

    private fun cohort(vararg names: String) = Cohort(names.map { CohortMember(pid(it)) }.toSet())

    @Test
    fun `armed tick does not schedule backend shutdown before T-0`() {
        val (orch, b) = setup()
        b.registry.get(survival).arm(cohort("alice"), durationSeconds = 60)

        val now = Instant.parse("2026-01-01T00:00:00Z")
        orch.tick(now)

        assertThat(b.pendingArmStore.peek(survival, now = now)).isNull()
        assertThat(b.messaging.restartSent).isEmpty()
        assertThat(b.registry.get(survival).state).isEqualTo(RestartState.COUNTDOWN)
    }

    @Test
    fun `cancel before T-0 publishes only a cancellation tombstone`() {
        val (orch, b) = setup()
        b.registry.get(survival).arm(cohort("alice"), durationSeconds = 60)
        val now = Instant.parse("2026-01-01T00:00:00Z")
        orch.tick(now)

        orch.cancel(survival, now)

        assertThat(b.pendingArmStore.peek(survival, now = now)).isNull()
        assertThat(b.pendingArmStore.consumeDelivery(survival, now = now))
            .isEqualTo(com.badgersmc.queuerestart.velocity.application.arm.PendingArmStore.Delivery.Cancel)
        assertThat(b.messaging.cancelSent).containsExactly(survival)
    }

    @Test
    fun `COUNTDOWN tick fires broadcaster at marks without draining early`() {
        val proxy = FakeProxy(
            playersOnTarget = mutableSetOf(pid("alice")),
            reachable = mutableSetOf(hub),
        )
        val (orch, b) = setup(cfg = config(drainLead = 30), proxy = proxy)
        b.registry.get(survival).arm(cohort("alice"), durationSeconds = 60)

        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        orch.tick(t0)
        assertThat(b.audience.broadcasts).hasSize(1)

        orch.tick(t0.plusSeconds(30))
        assertThat(b.audience.broadcasts).hasSize(2)
        assertThat(proxy.transfers).isEmpty()
        assertThat(b.messaging.restartSent).isEmpty()
        assertThat(b.registry.get(survival).state).isEqualTo(RestartState.COUNTDOWN)

        orch.tick(t0.plusSeconds(31))
        assertThat(b.audience.broadcasts).hasSize(2)
    }

    @Test
    fun `COUNTDOWN begins draining exactly at T-0 regardless of legacy drain lead`() {
        val proxy = FakeProxy(
            playersOnTarget = mutableSetOf(pid("alice"), pid("lurker")),
            reachable = mutableSetOf(hub),
        )
        proxy.perms[pid("lurker")] = setOf("queuerestart.bypass.drain")
        val (orch, b) = setup(cfg = config(drainLead = 30), proxy = proxy)
        b.registry.get(survival).arm(cohort("alice"), durationSeconds = 60)

        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        orch.tick(t0)
        orch.tick(t0.plusSeconds(59))
        assertThat(b.registry.get(survival).state).isEqualTo(RestartState.COUNTDOWN)
        assertThat(proxy.transfers).isEmpty()

        orch.tick(t0.plusSeconds(60))
        assertThat(b.registry.get(survival).state).isEqualTo(RestartState.DRAINING)
        assertThat(proxy.transfers.map { it.first }).containsExactly(pid("alice"))

        orch.tick(t0.plusSeconds(62))
        assertThat(b.audience.disconnects.map { it.playerId }).containsExactly(pid("lurker"))
    }

    @Test
    fun `T-0 drain transfers players in configured batches`() {
        val proxy = FakeProxy(
            playersOnTarget = mutableSetOf(pid("a"), pid("b"), pid("c")),
            reachable = mutableSetOf(hub),
        )
        val (orch, b) = setup(cfg = config(drainLead = 30, batchSize = 2), proxy = proxy)
        b.registry.get(survival).arm(cohort("a", "b", "c"), durationSeconds = 60)

        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        orch.tick(t0)
        orch.tick(t0.plusSeconds(60))

        assertThat(proxy.transfers).hasSize(2)
        assertThat(proxy.transfers.map { it.second }).allMatch { it == hub }
        assertThat(b.messaging.restartSent).isEmpty()
    }

    @Test
    fun `empty target receives immediate restart only after T-0`() {
        val proxy = FakeProxy(reachable = mutableSetOf(hub))
        val (orch, b) = setup(cfg = config(drainLead = 30), proxy = proxy)
        b.registry.get(survival).arm(Cohort(emptySet()), durationSeconds = 60)

        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        orch.tick(t0)
        assertThat(b.messaging.restartSent).isEmpty()

        val restartAt = t0.plusSeconds(60)
        orch.tick(restartAt)

        val restart = b.messaging.restartSent.single()
        assertThat(restart.server).isEqualTo(survival)
        assertThat(restart.delaySeconds).isZero()
        assertThat(b.pendingArmStore.peek(survival, restartAt))
            .isEqualTo(com.badgersmc.queuerestart.common.schedule.PendingArm(0, RestartMode.SHUTDOWN, ""))
        assertThat(b.registry.get(survival).state).isEqualTo(RestartState.RESTART_SENT)
    }

    @Test
    fun `failed transfers use custom disconnect after one settle interval then restart`() {
        val proxy = FakeProxy(
            playersOnTarget = mutableSetOf(pid("stuck")),
            reachable = mutableSetOf(hub),
            transferSucceeds = false,
        )
        val (orch, b) = setup(cfg = config(batchInterval = 40), proxy = proxy)
        b.registry.get(survival).arm(cohort("stuck"), durationSeconds = 10)

        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        orch.tick(t0)
        orch.tick(t0.plusSeconds(10))
        assertThat(b.audience.disconnects).isEmpty()

        orch.tick(t0.plusSeconds(11))
        assertThat(b.audience.disconnects).isEmpty()

        orch.tick(t0.plusSeconds(12))
        val disconnect = b.audience.disconnects.single()
        assertThat(disconnect.playerId).isEqualTo(pid("stuck"))
        assertThat(disconnect.message).contains("restarting")
        assertThat(disconnect.placeholders["server"]).isEqualTo("survival")

        orch.tick(t0.plusSeconds(13))
        assertThat(b.messaging.restartSent.single().delaySeconds).isZero()
        assertThat(b.registry.get(survival).state).isEqualTo(RestartState.RESTART_SENT)
    }

    @Test
    fun `force timeout disconnects remaining players before immediate restart`() {
        val proxy = FakeProxy(
            playersOnTarget = mutableSetOf(pid("stuck")),
            reachable = mutableSetOf(hub),
            transferSucceeds = false,
        )
        val (orch, b) = setup(
            cfg = config(forceTimeout = 60, batchInterval = 4_000),
            proxy = proxy,
        )
        b.registry.get(survival).arm(cohort("stuck"), durationSeconds = 10)

        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        orch.tick(t0)
        orch.tick(t0.plusSeconds(10))
        orch.tick(t0.plusSeconds(70))

        assertThat(b.audience.disconnects.map { it.playerId }).containsExactly(pid("stuck"))
        assertThat(b.messaging.restartSent.single().delaySeconds).isZero()
        assertThat(b.registry.get(survival).state).isEqualTo(RestartState.RESTART_SENT)
    }

    @Test
    fun `cancel during ARMED returns to IDLE and broadcasts cancel-message`() {
        val (orch, b) = setup()
        b.registry.get(survival).arm(cohort("a"), durationSeconds = 60)

        orch.cancel(survival)

        assertThat(b.registry.get(survival).state).isEqualTo(RestartState.IDLE)
        assertThat(b.audience.broadcasts).anyMatch { it.contains("cancel") }
    }

    @Test
    fun `cancel during COUNTDOWN returns to IDLE`() {
        val (orch, b) = setup()
        b.registry.get(survival).arm(cohort("a"), durationSeconds = 60)
        orch.tick(Instant.parse("2026-01-01T00:00:00Z"))

        orch.cancel(survival)

        assertThat(b.registry.get(survival).state).isEqualTo(RestartState.IDLE)
    }

    @Test
    fun `cancel during IDLE is a no-op`() {
        val (orch, b) = setup()
        orch.cancel(survival) // should not throw
        assertThat(b.registry.get(survival).state).isEqualTo(RestartState.IDLE)
    }

    @Test
    fun `start subscribes onCheckHacksResult so verdicts reach the gate (REQ-040)`() {
        val (_, b) = setup()
        b.gate.register(pid("alice"), hasBypass = false, nowSeconds = 0)
        assertThat(b.gate.isPending(pid("alice"))).isTrue()

        // The orchestrator now forwards inbound verdicts to RejoinService,
        // which guards on the player actually being on the source backend
        // (REQ-090 finding B). The fake proxy has no players on survival,
        // so the source-bound verdict is dropped before reaching the
        // gate — re-bind alice into the FakeProxy's roster first.
        b.proxy.playersOnTarget.add(pid("alice"))
        b.messaging.checkResultHandler!!.invoke(survival, pid("alice"), CheckOutcome.CLEAN)

        assertThat(b.gate.isPending(pid("alice"))).isFalse()
    }

    @Test
    fun `persisted server cancellation before coordinator arm announces once`() {
        val (orch, b) = setup()

        orch.cancelPlan(survival, silent = false)
        orch.cancelPlan(survival, silent = true)

        assertThat(b.audience.broadcasts.count { it.contains("cancel") }).isEqualTo(1)
        assertThat(b.messaging.cancelSent).containsExactly(survival, survival)
    }

    @Test
    fun `direct cancellation remains idempotent and announces once`() {
        val (orch, b) = setup()
        b.registry.get(survival).arm(cohort("a"), durationSeconds = 60)
        val now = Instant.parse("2026-01-01T00:00:00Z")
        orch.tick(now)

        orch.cancel(survival, now)
        orch.cancel(survival, now)

        assertThat(b.audience.broadcasts.count { it.contains("cancel") }).isEqualTo(1)
        assertThat(b.messaging.cancelSent).containsExactly(survival)
    }

}
