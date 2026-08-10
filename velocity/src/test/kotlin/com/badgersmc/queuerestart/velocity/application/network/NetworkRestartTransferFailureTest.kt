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

class NetworkRestartTransferFailureTest {
    private val hub = ServerId("HUB")
    private val smp = ServerId("SMP")

    @Test
    fun `failed player drain aborts before any network power action`() {
        val control = FailingTransferControl()
        val executor = RecordingExecutor()
        val now = Instant.now()
        val config = NetworkRestartConfig.disabled().copy(
            enabled = true,
            serverIds = mapOf(hub to "hub1234", smp to "smp1234"),
            proxyServerId = "proxy1234",
            members = listOf(hub, smp),
            hubServers = listOf(hub),
            backendHeadStartSeconds = 0,
        )
        val service = NetworkRestartService(
            config = { config },
            schedules = { emptyList() },
            executor = executor,
            control = control,
            store = MemoryStore(),
            backendArm = { server, seconds, _ -> SchedCommandResult.Armed(server, seconds) },
            backendCancel = {},
            audit = { _, _ -> },
            backendIdentity = { UUID.nameUUIDFromBytes(it.value.toByteArray()) },
            currentProxyBootId = UUID.randomUUID(),
        )
        val plan = service.createManual(
            type = PlanType.NETWORK,
            targets = setOf(hub, smp),
            executionAt = now.plusSeconds(1),
            warningAt = now,
            reason = "drain failure test",
            creator = "console",
            silent = true,
        )

        service.tick(now.plusSeconds(2))

        assertThat(plan.state).isEqualTo(PlanState.FAILED)
        assertThat(plan.failure).contains("player remained connected")
        assertThat(executor.restartCalls).isZero()
        assertThat(control.disconnectAllCalls).isZero()
    }

    private class FailingTransferControl : NetworkControlPort {
        var disconnectAllCalls = 0
        override fun broadcast(notice: RestartNotice) = Unit
        override fun disconnectAll(notice: RestartNotice) { disconnectAllCalls++ }
        override fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary> =
            CompletableFuture.failedFuture(IllegalStateException("player remained connected after drain settlement"))
        override fun setMaintenance(enabled: Boolean, duration: Duration) = Unit
        override fun maintenanceActive(): Boolean = false
    }

    private class RecordingExecutor : ExternalRestartExecutor {
        override val name = "PTERODACTYL"
        var restartCalls = 0
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
            restartCalls++
            return CompletableFuture.completedFuture(PowerActionResult(true, "unexpected"))
        }
    }

    private class MemoryStore : RestartPlanStore {
        private var plans = emptyList<RestartPlan>()
        override fun load(): List<RestartPlan> = plans
        override fun save(plans: Collection<RestartPlan>) { this.plans = plans.toList() }
    }
}
