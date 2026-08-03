package com.badgersmc.queuerestart.velocity.application.network

import com.badgersmc.queuerestart.velocity.application.ports.ExternalRestartExecutor
import com.badgersmc.queuerestart.velocity.application.ports.NetworkControlPort
import com.badgersmc.queuerestart.velocity.application.ports.NetworkRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.PowerActionResult
import com.badgersmc.queuerestart.velocity.application.ports.RestartNotice
import com.badgersmc.queuerestart.velocity.application.ports.RestartPlanStore
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.application.ports.TransferSummary
import com.badgersmc.queuerestart.velocity.application.schedule.SchedCommandResult
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.plan.PlanState
import com.badgersmc.queuerestart.velocity.domain.plan.PlanType
import com.badgersmc.queuerestart.velocity.domain.plan.RestartPlan
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class NetworkRestartReliabilityTest {
    private val hub = ServerId("HUB")
    private val smp = ServerId("SMP")
    private val oneMinute = SoundCue("one-minute", 0.5f, 1.0f)
    private val tick = SoundCue("tick", 0.5f, 1.0f)
    private val t0 = SoundCue("t0", 0.5f, 1.0f)

    @Test
    fun `recovered server countdown is rearmed exactly once with remaining time`() {
        val store = MemoryStore()
        val originalArms = mutableListOf<Int>()
        val base = Instant.now()
        val first = service(store = store, backendArm = { server, seconds, _ ->
            originalArms += seconds
            SchedCommandResult.Armed(server, seconds)
        })
        val plan = first.createManual(
            PlanType.SERVER,
            setOf(smp),
            base.plusSeconds(600),
            base,
            "",
            "console",
            false,
        )
        first.tick(base)
        assertThat(plan.state).isEqualTo(PlanState.COUNTING_DOWN)
        assertThat(originalArms).containsExactly(600)

        val recoveredArms = mutableListOf<Int>()
        val recovered = service(store = store, backendArm = { server, seconds, _ ->
            recoveredArms += seconds
            SchedCommandResult.Armed(server, seconds)
        })
        recovered.tick(base.plusSeconds(100))
        recovered.tick(base.plusSeconds(101))

        val restored = recovered.allPlans().single()
        assertThat(recoveredArms).containsExactly(500)
        assertThat(restored.backendArmAccepted).isTrue()
        assertThat(restored.state).isEqualTo(PlanState.COUNTING_DOWN)
    }

    @Test
    fun `slightly late first server tick preserves a 60 second warning`() {
        val arms = mutableListOf<Int>()
        val base = Instant.now()
        val service = service(backendArm = { server, seconds, _ ->
            arms += seconds
            SchedCommandResult.Armed(server, seconds)
        })
        service.createManual(PlanType.SERVER, setOf(smp), base.plusSeconds(60), base, "", "console", false)

        service.tick(base.plusMillis(250))

        assertThat(arms).containsExactly(60)
    }

    @Test
    fun `past recovered server plan is missed rather than replayed or completed`() {
        val now = Instant.now()
        val store = MemoryStore().apply {
            save(
                listOf(
                    RestartPlan(
                        type = PlanType.SERVER,
                        targets = setOf(smp),
                        createdAt = now.minusSeconds(120),
                        executionAt = now.minusSeconds(10),
                        warningAt = now.minusSeconds(70),
                        creator = "console",
                        state = PlanState.COUNTING_DOWN,
                        backendArmAccepted = true,
                    ),
                ),
            )
        }
        val arms = mutableListOf<Int>()
        val recovered = service(store = store, backendArm = { server, seconds, _ ->
            arms += seconds
            SchedCommandResult.Armed(server, seconds)
        })

        recovered.tick(now)

        assertThat(recovered.allPlans().single().state).isEqualTo(PlanState.MISSED)
        assertThat(arms).isEmpty()
    }

    @Test
    fun `delayed proxy ticks emit newest mark and global sounds without duplicates`() {
        val control = FakeControl()
        val base = Instant.now()
        val service = service(
            control = control,
            soundResolver = { seconds -> when (seconds) {
                60L -> oneMinute
                in 1L..10L -> tick
                0L -> t0
                else -> null
            } },
        )
        service.createManual(PlanType.PROXY, emptySet(), base.plusSeconds(61), base, "", "console", false)

        service.tick(base)
        service.tick(base.plusSeconds(2)) // 61 -> 59 crosses 60
        service.tick(base.plusSeconds(52)) // 59 -> 9 crosses 30 and 10; only 10 is emitted
        service.tick(base.plusSeconds(52))
        service.tick(base.plusSeconds(62))

        assertThat(control.broadcasts.map { it.detail }).anyMatch { it.contains("1 minute") }
        assertThat(control.broadcasts.map { it.detail }).anyMatch { it.contains("10 seconds") }
        assertThat(control.sounds).containsExactly(oneMinute, tick, t0)
    }

    @Test
    fun `full network final announcement uses global t0 sound`() {
        val control = FakeControl()
        val base = Instant.now()
        val service = service(control = control, soundResolver = { if (it == 0L) t0 else null })
        service.createManual(
            PlanType.NETWORK,
            setOf(hub, smp),
            base.plusSeconds(1),
            base,
            "",
            "console",
            false,
        )

        service.tick(base.plusSeconds(2))

        assertThat(control.sounds).containsExactly(t0)
        assertThat(control.broadcasts.count { it.heading == "RESTARTING NOW" }).isEqualTo(1)
    }

    @Test
    fun `missing sound cue and silent plans produce no sound`() {
        val base = Instant.now()
        val missing = FakeControl()
        service(control = missing, soundResolver = { null }).apply {
            createManual(PlanType.PROXY, emptySet(), base.plusSeconds(1), base, "", "console", false)
            tick(base.plusSeconds(2))
        }
        val silent = FakeControl()
        service(control = silent, soundResolver = { t0 }).apply {
            createManual(PlanType.PROXY, emptySet(), base.plusSeconds(1), base, "", "console", true)
            tick(base.plusSeconds(2))
        }

        assertThat(missing.sounds).isEmpty()
        assertThat(silent.sounds).isEmpty()
        assertThat(silent.broadcasts).isEmpty()
    }

    @Test
    fun `server cancellation callback owns one notice before and during arm`() {
        val base = Instant.now()
        val ownerCalls = mutableListOf<Pair<ServerId, Boolean>>()
        val before = service(serverCancellationOwner = { server, silent -> ownerCalls += server to silent })
        before.createManual(PlanType.SERVER, setOf(smp), base.plusSeconds(600), base.plusSeconds(300), "", "console", false)
        assertThat(before.cancel(smp)).isTrue()
        assertThat(before.cancel(smp)).isFalse()

        val during = service(serverCancellationOwner = { server, silent -> ownerCalls += server to silent })
        during.createManual(PlanType.SERVER, setOf(smp), base.plusSeconds(600), base, "", "console", false)
        during.tick(base)
        assertThat(during.cancel(smp)).isTrue()

        assertThat(ownerCalls).containsExactly(smp to false, smp to false)
    }

    @Test
    fun `proxy and network cancellation broadcast once and remain idempotent`() {
        val base = Instant.now()
        val proxyControl = FakeControl()
        val proxyService = service(control = proxyControl)
        proxyService.createManual(PlanType.PROXY, emptySet(), base.plusSeconds(600), base, "", "console", false)
        assertThat(proxyService.cancel(PlanType.PROXY)).isTrue()
        assertThat(proxyService.cancel(PlanType.PROXY)).isFalse()

        val networkControl = FakeControl()
        val networkService = service(control = networkControl)
        networkService.createManual(PlanType.NETWORK, setOf(hub, smp), base.plusSeconds(600), base, "", "console", false)
        assertThat(networkService.cancel(PlanType.NETWORK)).isTrue()
        assertThat(networkService.cancel(PlanType.NETWORK)).isFalse()

        assertThat(proxyControl.broadcasts).hasSize(1)
        assertThat(networkControl.broadcasts).hasSize(1)
    }

    @Test
    fun `command and tick cancellation race has one terminal cancellation`() {
        val base = Instant.now()
        val ownerCalls = mutableListOf<Pair<ServerId, Boolean>>()
        val service = service(serverCancellationOwner = { server, silent ->
            synchronized(ownerCalls) { ownerCalls += server to silent }
        })
        val plan = service.createManual(
            PlanType.SERVER,
            setOf(smp),
            base.plusSeconds(600),
            base,
            "",
            "console",
            false,
        )
        val start = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(2)
        val cancel = pool.submit<Boolean> { start.await(); service.cancel(smp) }
        val tickRun = pool.submit { start.await(); service.tick(base) }
        start.countDown()
        cancel.get()
        tickRun.get()
        pool.shutdownNow()

        assertThat(plan.state).isEqualTo(PlanState.CANCELLED)
        assertThat(ownerCalls).containsExactly(smp to false)
    }

    private fun service(
        control: FakeControl = FakeControl(),
        store: RestartPlanStore = MemoryStore(),
        backendArm: (ServerId, Int, Boolean) -> SchedCommandResult = { server, seconds, _ ->
            SchedCommandResult.Armed(server, seconds)
        },
        serverCancellationOwner: ((ServerId, Boolean) -> Unit)? = null,
        soundResolver: (Long) -> SoundCue? = { null },
        backendBoots: MutableMap<ServerId, UUID> = mutableMapOf(
            hub to UUID.fromString("40000000-0000-0000-0000-000000000001"),
            smp to UUID.fromString("40000000-0000-0000-0000-000000000002"),
        ),
    ): NetworkRestartService {
        val config = NetworkRestartConfig.disabled().copy(
            enabled = true,
            serverIds = mapOf(hub to "hub1234", smp to "smp1234"),
            proxyServerId = "proxy1234",
            members = listOf(hub, smp),
            hubServers = listOf(hub),
            announcementPointsSeconds = listOf(60, 30, 10),
            finalCountdownSeconds = 5,
            backendHeadStartSeconds = 0,
        )
        return NetworkRestartService(
            config = { config },
            schedules = { emptyList() },
            executor = ImmediateExecutor(),
            control = control,
            store = store,
            backendArm = backendArm,
            backendCancel = {},
            audit = { _, _ -> },
            serverCancellationOwner = serverCancellationOwner,
            soundResolver = soundResolver,
            backendIdentity = { backendBoots[it] },
            prepareBackendHandoff = { _, _ -> true },
            currentProxyBootId = UUID.fromString("40000000-0000-0000-0000-000000000010"),
        )
    }

    private class ImmediateExecutor : ExternalRestartExecutor {
        override val name = "fake"
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
    }

    private class FakeControl : NetworkControlPort {
        val broadcasts = mutableListOf<RestartNotice>()
        val sounds = mutableListOf<SoundCue>()
        override fun broadcast(notice: RestartNotice) { broadcasts += notice }
        override fun playSound(cue: SoundCue) { sounds += cue }
        override fun disconnectAll(notice: RestartNotice) = Unit
        override fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary> =
            CompletableFuture.completedFuture(TransferSummary(0, 0, 0))
        override fun setMaintenance(enabled: Boolean, duration: Duration) = Unit
        override fun maintenanceActive(): Boolean = false
    }

    private class MemoryStore : RestartPlanStore {
        private var saved = emptyList<RestartPlan>()
        override fun load(): List<RestartPlan> = saved.map(::snapshot)
        override fun save(plans: Collection<RestartPlan>) { saved = plans.map(::snapshot) }
        private fun snapshot(plan: RestartPlan): RestartPlan = plan.copy(
            announcedSeconds = ConcurrentHashMap.newKeySet<Long>().also { it += plan.announcedSeconds },
            targetResults = ConcurrentHashMap(plan.targetResults),
            dispatchedActionKeys = ConcurrentHashMap.newKeySet<String>().also { it += plan.dispatchedActionKeys },
            acceptedActionKeys = ConcurrentHashMap.newKeySet<String>().also { it += plan.acceptedActionKeys },
            baselineBootIds = ConcurrentHashMap(plan.baselineBootIds),
        )
    }
}
