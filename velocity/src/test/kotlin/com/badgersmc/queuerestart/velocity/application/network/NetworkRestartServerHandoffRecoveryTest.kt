package com.badgersmc.queuerestart.velocity.application.network

import com.badgersmc.queuerestart.velocity.application.ports.ExternalRestartExecutor
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
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap

class NetworkRestartServerHandoffRecoveryTest {
    private val hub = ServerId("HUB")
    private val smp = ServerId("SMP")
    private val boot = UUID.fromString("50000000-0000-0000-0000-000000000001")

    @Test
    fun `v2 prepared handoff interrupted before dispatch commit fails open safely`() {
        val now = Instant.now()
        val plan = dispatchingServerPlan(now).apply {
            actionStarted = false
            executionDeadlineAt = now.plusSeconds(20) // proxy died during heartbeat retry window
            targetResults[NetworkRestartService.SERVER_HANDOFF_PROTOCOL_KEY] =
                NetworkRestartService.SERVER_HANDOFF_PREPARED_V2
        }

        val service = service(MemoryStore(listOf(plan)), mutableMapOf(smp to boot))
        val recovered = service.allPlans().single()

        assertThat(recovered.state).isEqualTo(PlanState.FAILED)
        assertThat(recovered.failure).contains("before restart delivery dispatch was committed")
        assertThat(recovered.actionStarted).isFalse()
        assertThat(recovered.executionDeadlineAt).isNull()
        assertThat(service.blocksBackendAccess(smp)).isFalse()
    }

    @Test
    fun `legacy unmarked unpublished dispatch remains fail closed because old ordering was ambiguous`() {
        val now = Instant.now()
        val plan = dispatchingServerPlan(now).apply {
            actionStarted = false
            executionDeadlineAt = now.plusSeconds(20)
            targetResults.clear()
        }

        val service = service(MemoryStore(listOf(plan)), mutableMapOf(smp to boot))
        val recovered = service.allPlans().single()

        assertThat(recovered.state).isEqualTo(PlanState.NEEDS_REVIEW)
        assertThat(recovered.failure).contains("missing durable action acceptance")
        assertThat(service.blocksBackendAccess(smp)).isTrue()
    }

    @Test
    fun `v2 committed dispatch resumes lifecycle verification after proxy restart`() {
        val now = Instant.now()
        val replacementBoot = UUID.fromString("50000000-0000-0000-0000-000000000002")
        val boots = mutableMapOf(smp to boot)
        val plan = dispatchingServerPlan(now).apply {
            actionStarted = true
            executionDeadlineAt = now.plusSeconds(300)
            targetResults[NetworkRestartService.SERVER_HANDOFF_PROTOCOL_KEY] =
                NetworkRestartService.SERVER_HANDOFF_COMMITTED_V2
            targetResults[smp.value] = "authenticated restart delivery dispatch committed"
        }

        val service = service(MemoryStore(listOf(plan)), boots)
        assertThat(service.allPlans().single().state).isEqualTo(PlanState.DISPATCHING)
        assertThat(service.blocksBackendAccess(smp)).isTrue()

        boots[smp] = replacementBoot
        service.tick(now.plusSeconds(1))

        val completed = service.allPlans().single()
        assertThat(completed.state).isEqualTo(PlanState.COMPLETED)
        assertThat(completed.targetResults["verification"]).contains("boot identity changed")
        assertThat(service.blocksBackendAccess(smp)).isFalse()
    }

    private fun dispatchingServerPlan(now: Instant) = RestartPlan(
        type = PlanType.SERVER,
        targets = setOf(smp),
        createdAt = now.minusSeconds(60),
        executionAt = now.minusSeconds(10),
        warningAt = now.minusSeconds(120),
        creator = "AUTOMATIC:daily-smp",
        state = PlanState.DISPATCHING,
        baselineBootIds = ConcurrentHashMap<ServerId, UUID>().also { it[smp] = boot },
    )

    private fun service(
        store: RestartPlanStore,
        boots: MutableMap<ServerId, UUID>,
    ): NetworkRestartService {
        val config = NetworkRestartConfig.disabled().copy(
            enabled = true,
            serverIds = mapOf(hub to "hub1234", smp to "smp1234"),
            proxyServerId = "proxy1234",
            members = listOf(hub, smp),
            hubServers = listOf(hub),
        )
        return NetworkRestartService(
            config = { config },
            schedules = { emptyList() },
            executor = NoopExecutor(),
            control = NoopControl(),
            store = store,
            backendArm = { server, seconds, _ -> SchedCommandResult.Armed(server, seconds) },
            backendCancel = {},
            audit = { _, _ -> },
            backendIdentity = { boots[it] },
            prepareBackendHandoff = { _, _ -> true },
            currentProxyBootId = UUID.fromString("50000000-0000-0000-0000-000000000010"),
            executionTimeout = { Duration.ofSeconds(300) },
            handoffRetryDelay = { Duration.ofSeconds(20) },
        )
    }

    private class MemoryStore(initial: List<RestartPlan>) : RestartPlanStore {
        private var saved = initial.map(::snapshot)
        override fun load(): List<RestartPlan> = saved.map(::snapshot)
        override fun save(plans: Collection<RestartPlan>) {
            saved = plans.map(::snapshot)
        }

        private fun snapshot(plan: RestartPlan): RestartPlan = plan.copy(
            announcedSeconds = ConcurrentHashMap.newKeySet<Long>().also { it += plan.announcedSeconds },
            targetResults = ConcurrentHashMap(plan.targetResults),
            dispatchedActionKeys = ConcurrentHashMap.newKeySet<String>().also { it += plan.dispatchedActionKeys },
            acceptedActionKeys = ConcurrentHashMap.newKeySet<String>().also { it += plan.acceptedActionKeys },
            baselineBootIds = ConcurrentHashMap(plan.baselineBootIds),
        )
    }

    private class NoopExecutor : ExternalRestartExecutor {
        override val name = "noop"
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
    }

    private class NoopControl : NetworkControlPort {
        override fun broadcast(notice: RestartNotice) = Unit
        override fun disconnectAll(notice: RestartNotice) = Unit
        override fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary> =
            CompletableFuture.completedFuture(TransferSummary(0, 0, 0))
        override fun setMaintenance(enabled: Boolean, duration: Duration) = Unit
        override fun maintenanceActive(): Boolean = false
    }
}
