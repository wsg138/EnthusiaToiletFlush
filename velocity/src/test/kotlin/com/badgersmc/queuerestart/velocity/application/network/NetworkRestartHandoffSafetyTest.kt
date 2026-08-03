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

class NetworkRestartHandoffSafetyTest {
    private val hub = ServerId("HUB")
    private val smp = ServerId("SMP")

    @Test
    fun `mismatched published handoff baseline fails closed before action starts`() {
        val expectedBoot = UUID.fromString("40000000-0000-0000-0000-000000000002")
        val boots = mutableMapOf(
            hub to UUID.fromString("40000000-0000-0000-0000-000000000001"),
            smp to expectedBoot,
        )
        val warningAt = Instant.now().plusSeconds(10)
        val service = service(boots)
        val plan = service.createManual(
            PlanType.SERVER,
            setOf(smp),
            warningAt.plusSeconds(1),
            warningAt,
            "baseline safety",
            "console",
            false,
        )

        service.tick(warningAt)
        service.tick(warningAt.plusSeconds(2))
        assertThat(plan.state).isEqualTo(PlanState.DISPATCHING)
        assertThat(plan.actionStarted).isFalse()

        assertThat(service.markBackendHandoffPublished(smp, UUID.randomUUID())).isFalse()
        assertThat(plan.state).isEqualTo(PlanState.NEEDS_REVIEW)
        assertThat(plan.actionStarted).isFalse()
        assertThat(plan.failure).contains("baseline")
    }

    private fun service(boots: MutableMap<ServerId, UUID>): NetworkRestartService {
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
            schedules = { emptyList() },
            executor = FakeExecutor(),
            control = FakeControl(),
            store = MemoryStore(),
            backendArm = { server, seconds, _ -> SchedCommandResult.Armed(server, seconds) },
            backendCancel = {},
            audit = { _, _ -> },
            backendIdentity = { boots[it] },
            prepareBackendHandoff = { _, _ -> true },
            currentProxyBootId = UUID.fromString("40000000-0000-0000-0000-000000000010"),
        )
    }

    private class FakeExecutor : ExternalRestartExecutor {
        override val name = "fake"
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
    }

    private class MemoryStore : RestartPlanStore {
        override fun load(): List<RestartPlan> = emptyList()
        override fun save(plans: Collection<RestartPlan>) = Unit
    }

    private class FakeControl : NetworkControlPort {
        override fun broadcast(notice: RestartNotice) = Unit
        override fun disconnectAll(notice: RestartNotice) = Unit
        override fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary> =
            CompletableFuture.completedFuture(TransferSummary(0, 0, 0))
        override fun setMaintenance(enabled: Boolean, duration: Duration) = Unit
        override fun maintenanceActive(): Boolean = false
    }
}
