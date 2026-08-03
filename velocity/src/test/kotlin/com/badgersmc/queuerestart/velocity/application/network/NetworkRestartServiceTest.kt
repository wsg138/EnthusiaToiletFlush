package com.badgersmc.queuerestart.velocity.application.network

import com.badgersmc.queuerestart.velocity.application.ports.ExternalRestartExecutor
import com.badgersmc.queuerestart.velocity.application.ports.ConfiguredRestartSchedule
import com.badgersmc.queuerestart.velocity.application.ports.NetworkControlPort
import com.badgersmc.queuerestart.velocity.application.ports.NetworkRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.PowerActionResult
import com.badgersmc.queuerestart.velocity.application.ports.RestartNotice
import com.badgersmc.queuerestart.velocity.application.ports.RestartPlanStore
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
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap

class NetworkRestartServiceTest {
    private val hub = ServerId("HUB")
    private val smp = ServerId("SMP")

    @Test
    fun `initial proxy warning does not repeat its matching threshold`() {
        val control = FakeControl()
        val service = service(control)
        val now = Instant.now()
        service.createManual(PlanType.PROXY, emptySet(), now.plusSeconds(7200), now, "", "console", false)

        service.tick(now)

        assertThat(control.broadcasts).hasSize(1)
    }

    @Test
    fun `manual plan replaces an overlapping automatic plan`() {
        val service = service(FakeControl())
        val now = Instant.now()
        val automatic = RestartPlan(
            type = PlanType.NETWORK,
            targets = setOf(hub, smp),
            createdAt = now,
            executionAt = now.plusSeconds(120),
            warningAt = now,
            reason = "",
            creator = "AUTOMATIC:nightly",
            automaticKey = "nightly@${now.plusSeconds(120)}",
        )
        service.schedule(automatic)

        service.createManual(PlanType.PROXY, emptySet(), now.plusSeconds(180), now, "", "console", false)

        assertThat(automatic.state).isEqualTo(PlanState.CANCELLED)
        assertThat(service.allPlans().count { it.active() }).isEqualTo(1)
    }

    @Test
    fun `proxy disconnect happens before its external restart request`() {
        val events = mutableListOf<String>()
        val control = FakeControl(events)
        val service = service(control, FakeExecutor(events))
        val now = Instant.now()
        service.createManual(PlanType.PROXY, emptySet(), now.plusSeconds(1), now, "", "console", false)

        service.tick(now.plusSeconds(2))

        assertThat(events).containsSubsequence("disconnect", "restart:proxy")
        assertThat(control.maintenanceEnables).isEqualTo(1)
        assertThat(control.maintenanceDisables).isEqualTo(1) // startup reconciliation only
        assertThat(service.allPlans().single().state).isEqualTo(PlanState.DISPATCHING)
    }

    @Test
    fun `active dispatch refreshes maintenance before its failure expiry`() {
        val control = FakeControl()
        val service = service(control, HangingExecutor())
        val now = Instant.now()
        service.createManual(PlanType.PROXY, emptySet(), now.plusSeconds(1), now, "", "console", false)

        service.tick(now.plusSeconds(2))
        service.tick(now.plusSeconds(70))

        assertThat(control.maintenanceEnables).isGreaterThanOrEqualTo(2)
    }

    @Test
    fun `recovered dispatch never sends a second power action`() {
        val store = MemoryStore()
        val executor = HangingExecutor()
        val now = Instant.now()
        service(FakeControl(), executor, store).apply {
            createManual(PlanType.PROXY, emptySet(), now.plusSeconds(1), now, "", "console", false)
            tick(now.plusSeconds(2))
        }

        val recovered = service(FakeControl(), executor, store)

        assertThat(executor.requests).isEqualTo(1)
        assertThat(recovered.allPlans().single().state).isEqualTo(PlanState.NEEDS_REVIEW)
    }

    @Test
    fun `failed dispatch clears maintenance`() {
        val control = FakeControl()
        val service = service(control, FailingExecutor())
        val now = Instant.now()
        service.createManual(PlanType.PROXY, emptySet(), now.plusSeconds(1), now, "", "console", false)

        service.tick(now.plusSeconds(2))

        assertThat(control.maintenanceDisables).isGreaterThanOrEqualTo(2)
    }

    @Test
    fun `cancelling a server target clears its persistent plan and permits rescheduling`() {
        val cancelled = mutableListOf<ServerId>()
        val service = service(FakeControl(), backendCancel = cancelled::add)
        val now = Instant.now()
        val first = service.createManual(PlanType.SERVER, setOf(smp), now.plusSeconds(600), now, "", "console", false)
        service.tick(now)

        assertThat(service.cancel(smp)).isTrue()
        assertThat(first.state).isEqualTo(PlanState.CANCELLED)
        assertThat(cancelled).containsExactly(smp)

        val replacement = service.createManual(PlanType.SERVER, setOf(smp), now.plusSeconds(900), now, "", "console", false)
        assertThat(replacement.state).isEqualTo(PlanState.SCHEDULED)
    }

    @Test
    fun `regular server cancel stops an active daily occurrence without recreating it`() {
        val cancelled = mutableListOf<ServerId>()
        val daily = ConfiguredRestartSchedule(
            name = "daily-smp",
            type = "SERVER",
            targets = listOf(smp),
            time = "00:00",
            days = emptySet(),
            warningWindowSeconds = 7200,
            timezone = "America/Indiana/Indianapolis",
            reason = "Daily restart",
            silent = false,
            enabled = true,
        )
        val service = service(FakeControl(), backendCancel = cancelled::add, schedules = listOf(daily))
        val warningStart = nextDailyWarningStart()

        service.tick(warningStart)
        val occurrence = service.allPlans().single()
        assertThat(occurrence.state).isEqualTo(PlanState.COUNTING_DOWN)

        assertThat(service.cancel(smp)).isTrue()
        service.tick(warningStart.plusSeconds(60))

        assertThat(occurrence.state).isEqualTo(PlanState.CANCELLED)
        assertThat(service.allPlans()).containsExactly(occurrence)
        assertThat(service.activePublicPlans()).isEmpty()
        assertThat(cancelled).containsExactly(smp)
    }

    @Test
    fun `dry run completes without transfers disconnects maintenance or power requests`() {
        val control = FakeControl()
        val executor = NonDestructiveExecutor()
        val service = service(control, executor)
        val now = Instant.now()
        val plan = service.createManual(
            PlanType.NETWORK,
            setOf(hub, smp),
            now.plusSeconds(1),
            now,
            "validation",
            "console",
            false,
        )

        service.tick(now.plusSeconds(2))

        assertThat(plan.state).isEqualTo(PlanState.COMPLETED)
        assertThat(control.transfers).isZero()
        assertThat(control.disconnects).isZero()
        assertThat(control.maintenanceEnables).isZero()
        assertThat(executor.calls).isZero()
        assertThat(service.lastCompletedProxyRestart()).isNull()
        assertThat(service.lastCompletedServerRestart(smp)).isNull()
    }

    @Test
    fun `executor snapshot stays stable when configuration changes during preflight`() {
        val control = FakeControl()
        val executor = ReloadDuringPreflightExecutor()
        val service = service(control, executor)
        val now = Instant.now()
        val plan = service.createManual(
            PlanType.PROXY,
            emptySet(),
            now.plusSeconds(1),
            now,
            "reload race",
            "console",
            false,
        )

        service.tick(now.plusSeconds(2))

        assertThat(plan.state).isEqualTo(PlanState.DISPATCHING)
        assertThat(executor.realRestartCalls).isEqualTo(1)
        assertThat(executor.dryRunRestartCalls).isZero()
    }

    @Test
    fun `rejected backend restart aborts before hub disconnect and proxy restart`() {
        val control = FakeControl()
        val executor = SelectiveExecutor(rejectedPanelId = "smp1234")
        val service = service(control, executor)
        val now = Instant.now()
        val plan = service.createManual(
            PlanType.NETWORK,
            setOf(hub, smp),
            now.plusSeconds(1),
            now,
            "failure test",
            "console",
            false,
        )

        service.tick(now.plusSeconds(2))

        assertThat(plan.state).isEqualTo(PlanState.FAILED)
        assertThat(executor.restartIds).containsExactly("smp1234")
        assertThat(control.disconnects).isZero()
        assertThat(plan.failure).contains("SMP").contains("rejected")
    }

    @Test
    fun `last completed restart queries include proxy network and target history`() {
        val store = MemoryStore()
        val now = Instant.now()
        val smpRestart = RestartPlan(
            type = PlanType.SERVER, targets = setOf(smp), createdAt = now.minusSeconds(300),
            executionAt = now.minusSeconds(60), warningAt = now.minusSeconds(360), creator = "console",
            state = PlanState.COMPLETED, completedAt = now.minusSeconds(120),
        )
        val networkRestart = RestartPlan(
            type = PlanType.NETWORK, targets = setOf(hub, smp), createdAt = now.minusSeconds(600),
            executionAt = now.minusSeconds(180), warningAt = now.minusSeconds(660), creator = "console",
            state = PlanState.COMPLETED, completedAt = now.minusSeconds(30),
        )
        store.save(listOf(smpRestart, networkRestart))
        val recovered = service(FakeControl(), store = store)

        assertThat(recovered.lastCompletedServerRestart(smp)?.id).isEqualTo(networkRestart.id)
        assertThat(recovered.lastCompletedProxyRestart()?.id).isEqualTo(networkRestart.id)
        assertThat(recovered.lastCompletedServerRestart(hub)?.id).isEqualTo(networkRestart.id)
    }

    @Test
    fun `accepted proxy action completes only in a replacement Velocity process`() {
        val store = MemoryStore()
        val executor = FakeExecutor(mutableListOf())
        val oldBoot = UUID.fromString("10000000-0000-0000-0000-000000000001")
        val newBoot = UUID.fromString("10000000-0000-0000-0000-000000000002")
        val now = Instant.now()
        val first = service(FakeControl(), executor, store, proxyBootId = oldBoot)
        val plan = first.createManual(PlanType.PROXY, emptySet(), now.plusSeconds(1), now, "", "console", false)

        first.tick(now.plusSeconds(2))
        assertThat(plan.state).isEqualTo(PlanState.DISPATCHING)

        val replacement = service(FakeControl(), executor, store, proxyBootId = newBoot)
        replacement.tick(now.plusSeconds(3))
        assertThat(replacement.allPlans().single().state).isEqualTo(PlanState.COMPLETED)
    }

    @Test
    fun `full network completion waits for every backend and proxy boot identity`() {
        val store = MemoryStore()
        val boots = mutableMapOf(
            hub to UUID.fromString("20000000-0000-0000-0000-000000000001"),
            smp to UUID.fromString("20000000-0000-0000-0000-000000000002"),
        )
        val oldProxy = UUID.fromString("20000000-0000-0000-0000-000000000010")
        val newProxy = UUID.fromString("20000000-0000-0000-0000-000000000011")
        val now = Instant.now()
        val first = service(FakeControl(), store = store, backendBoots = boots, proxyBootId = oldProxy)
        first.createManual(PlanType.NETWORK, setOf(hub, smp), now.plusSeconds(1), now, "", "console", false)
        first.tick(now.plusSeconds(2))

        val replacement = service(FakeControl(), store = store, backendBoots = boots, proxyBootId = newProxy)
        replacement.tick(now.plusSeconds(3))
        assertThat(replacement.allPlans().single().state).isEqualTo(PlanState.DISPATCHING)

        boots[hub] = UUID.fromString("20000000-0000-0000-0000-000000000101")
        replacement.tick(now.plusSeconds(4))
        assertThat(replacement.allPlans().single().state).isEqualTo(PlanState.DISPATCHING)

        boots[smp] = UUID.fromString("20000000-0000-0000-0000-000000000102")
        replacement.tick(now.plusSeconds(5))
        assertThat(replacement.allPlans().single().state).isEqualTo(PlanState.COMPLETED)
    }

    @Test
    fun `server completion requires published handoff and changed companion boot id`() {
        val boots = mutableMapOf(
            hub to UUID.fromString("30000000-0000-0000-0000-000000000001"),
            smp to UUID.fromString("30000000-0000-0000-0000-000000000002"),
        )
        val now = Instant.now()
        val service = service(FakeControl(), backendBoots = boots)
        val plan = service.createManual(PlanType.SERVER, setOf(smp), now.plusSeconds(1), now, "", "console", false)
        service.tick(now)
        service.tick(now.plusSeconds(2))
        assertThat(plan.state).isEqualTo(PlanState.DISPATCHING)
        assertThat(plan.actionStarted).isFalse()

        assertThat(service.markBackendHandoffPublished(smp, boots.getValue(smp))).isTrue()
        service.tick(now.plusSeconds(3))
        assertThat(plan.state).isEqualTo(PlanState.DISPATCHING)

        boots[smp] = UUID.fromString("30000000-0000-0000-0000-000000000102")
        service.tick(now.plusSeconds(4))
        assertThat(plan.state).isEqualTo(PlanState.COMPLETED)
    }

    @Test
    fun `needs review remains fail closed and blocks overlapping schedules`() {
        val store = MemoryStore()
        val now = Instant.now()
        val review = RestartPlan(
            type = PlanType.PROXY,
            targets = emptySet(),
            createdAt = now.minusSeconds(30),
            executionAt = now.minusSeconds(10),
            warningAt = now.minusSeconds(40),
            creator = "console",
            state = PlanState.NEEDS_REVIEW,
            maintenanceEnabled = false,
            failure = "uncertain",
        )
        store.save(listOf(review))
        val control = FakeControl()
        val service = service(control, store = store)

        service.tick(now.plusSeconds(120))
        assertThat(control.maintenanceEnables).isGreaterThanOrEqualTo(2)
        org.assertj.core.api.Assertions.assertThatThrownBy {
            service.createManual(PlanType.PROXY, emptySet(), now.plusSeconds(600), now, "", "console", false)
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    private fun service(
        control: FakeControl,
        executor: ExternalRestartExecutor = FakeExecutor(mutableListOf()),
        store: RestartPlanStore = MemoryStore(),
        backendCancel: (ServerId) -> Unit = {},
        schedules: List<ConfiguredRestartSchedule> = emptyList(),
        backendBoots: MutableMap<ServerId, UUID> = mutableMapOf(
            hub to UUID.fromString("00000000-0000-0000-0000-000000000001"),
            smp to UUID.fromString("00000000-0000-0000-0000-000000000002"),
        ),
        proxyBootId: UUID = UUID.fromString("00000000-0000-0000-0000-000000000010"),
        prepareBackendHandoff: (ServerId, UUID) -> Boolean = { _, _ -> true },
    ): NetworkRestartService {
        val config = NetworkRestartConfig.disabled().copy(
            enabled = true,
            serverIds = mapOf(hub to "hub1234", smp to "smp1234"),
            proxyServerId = "proxy1234",
            members = listOf(hub, smp),
            hubServers = listOf(hub),
            backendHeadStartSeconds = 0,
        )
        return NetworkRestartService(
            config = { config },
            schedules = { schedules },
            executor = executor,
            control = control,
            store = store,
            backendArm = { server, seconds, _ -> SchedCommandResult.Armed(server, seconds) },
            backendCancel = backendCancel,
            audit = { _, _ -> },
            backendIdentity = { backendBoots[it] },
            prepareBackendHandoff = prepareBackendHandoff,
            currentProxyBootId = proxyBootId,
        )
    }

    private class FakeExecutor(private val events: MutableList<String>) : ExternalRestartExecutor {
        override val name = "fake"
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
            events += "restart:${actionKey.substringAfterLast(':')}"
            return CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        }
    }

    private class HangingExecutor : ExternalRestartExecutor {
        override val name = "hanging"
        var requests = 0
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
            requests++
            return CompletableFuture()
        }
    }

    private class FailingExecutor : ExternalRestartExecutor {
        override val name = "failing"
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(false, "rejected"))
    }

    private class NonDestructiveExecutor : ExternalRestartExecutor {
        override val name = "DRY_RUN"
        override val performsPowerActions = false
        var calls = 0
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> {
            calls++
            return CompletableFuture.completedFuture(PowerActionResult(true, "unexpected"))
        }
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
            calls++
            return CompletableFuture.completedFuture(PowerActionResult(true, "unexpected"))
        }
    }

    private class ReloadDuringPreflightExecutor : ExternalRestartExecutor {
        var realRestartCalls = 0
        var dryRunRestartCalls = 0

        private val dryRun = object : ExternalRestartExecutor {
            override val name = "DRY_RUN"
            override val performsPowerActions = false
            override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
                CompletableFuture.completedFuture(PowerActionResult(true, "dry-run"))
            override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
                dryRunRestartCalls++
                return CompletableFuture.completedFuture(PowerActionResult(true, "dry-run"))
            }
        }

        private val real = object : ExternalRestartExecutor {
            override val name = "PTERODACTYL"
            override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> {
                current = dryRun
                return CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
            }
            override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
                realRestartCalls++
                return CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
            }
        }

        @Volatile private var current: ExternalRestartExecutor = real
        override val name: String get() = current.name
        override val performsPowerActions: Boolean get() = current.performsPowerActions
        override fun snapshot(): ExternalRestartExecutor = current
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> = current.preflight(panelServerId)
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
            current.restart(actionKey, panelServerId)
    }

    private class SelectiveExecutor(private val rejectedPanelId: String) : ExternalRestartExecutor {
        override val name = "selective"
        val restartIds = mutableListOf<String>()
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
            restartIds += panelServerId
            val accepted = panelServerId != rejectedPanelId
            return CompletableFuture.completedFuture(PowerActionResult(accepted, if (accepted) "ok" else "rejected"))
        }
    }

    private fun nextDailyWarningStart(): Instant {
        val zone = ZoneId.of("America/Indiana/Indianapolis")
        val now = Instant.now()
        val tomorrow = ZonedDateTime.of(LocalDate.ofInstant(now, zone).plusDays(1), LocalTime.MIDNIGHT, zone).toInstant()
        val following = tomorrow.plus(Duration.ofDays(1))
        return (if (tomorrow.minusSeconds(7200).isAfter(now)) tomorrow else following).minusSeconds(7200)
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

    private class FakeControl(private val events: MutableList<String> = mutableListOf()) : NetworkControlPort {
        val broadcasts = mutableListOf<RestartNotice>()
        var maintenanceEnables = 0
        var maintenanceDisables = 0
        var disconnects = 0
        var transfers = 0
        override fun broadcast(notice: RestartNotice) { broadcasts += notice }
        override fun disconnectAll(notice: RestartNotice) { disconnects++; events += "disconnect" }
        override fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary> {
            transfers++
            return CompletableFuture.completedFuture(TransferSummary(0, 0, 0))
        }
        override fun setMaintenance(enabled: Boolean, duration: Duration) {
            if (enabled) maintenanceEnables++
            else maintenanceDisables++
        }
        override fun maintenanceActive(): Boolean = false
    }
}
