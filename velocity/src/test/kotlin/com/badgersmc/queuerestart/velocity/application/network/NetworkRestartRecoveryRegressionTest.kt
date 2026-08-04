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

class NetworkRestartRecoveryRegressionTest {
    private val hub = ServerId("HUB")
    private val smp = ServerId("SMP")

    @Test
    fun `terminal destructive plans remain terminal across repeated recovery`() {
        val store = MemoryStore()
        val now = Instant.now()
        val completed = plan(
            PlanType.NETWORK,
            PlanState.COMPLETED,
            now,
            actionStarted = true,
            maintenanceEnabled = true,
            completedAt = now.minusSeconds(30),
        )
        store.save(listOf(completed))

        val first = service(FakeControl(), store).allPlans().single()
        val secondControl = FakeControl()
        val second = service(secondControl, store).allPlans().single()

        assertThat(first.state).isEqualTo(PlanState.COMPLETED)
        assertThat(second.state).isEqualTo(PlanState.COMPLETED)
        assertThat(second.maintenanceEnabled).isFalse()
        assertThat(secondControl.maintenanceEnables).isZero()
    }

    @Test
    fun `legacy completed review restores its completed state`() {
        val store = MemoryStore()
        val now = Instant.now()
        val regressed = plan(
            PlanType.PROXY,
            PlanState.NEEDS_REVIEW,
            now,
            actionStarted = true,
            maintenanceEnabled = true,
            completedAt = now.minusSeconds(30),
            failure = LEGACY_FAILURE,
        )
        store.save(listOf(regressed))
        val control = FakeControl()

        val recovered = service(control, store).allPlans().single()

        assertThat(recovered.state).isEqualTo(PlanState.COMPLETED)
        assertThat(recovered.failure).isEmpty()
        assertThat(recovered.maintenanceEnabled).isFalse()
        assertThat(recovered.targetResults["recovery"]).contains("legacy recovery regression")
        assertThat(control.maintenanceEnables).isZero()
    }

    @Test
    fun `legacy review without durable dispatch evidence closes as failed`() {
        val store = MemoryStore()
        val now = Instant.now()
        val regressed = plan(
            PlanType.NETWORK,
            PlanState.NEEDS_REVIEW,
            now,
            actionStarted = true,
            maintenanceEnabled = true,
            failure = LEGACY_FAILURE,
        )
        store.save(listOf(regressed))
        val control = FakeControl()

        val recovered = service(control, store).allPlans().single()

        assertThat(recovered.state).isEqualTo(PlanState.FAILED)
        assertThat(recovered.failure).isEqualTo("legacy recovery regression reconciled")
        assertThat(recovered.maintenanceEnabled).isFalse()
        assertThat(control.maintenanceEnables).isZero()
    }

    @Test
    fun `operator resolved review cannot return on later recovery`() {
        val store = MemoryStore()
        val now = Instant.now()
        val review = plan(
            PlanType.NETWORK,
            PlanState.NEEDS_REVIEW,
            now,
            actionStarted = true,
            failure = "uncertain",
        )
        store.save(listOf(review))
        val first = service(FakeControl(), store)
        assertThat(first.resolveReview(review.id.toString().take(8))).isTrue()

        val recovered = service(FakeControl(), store).allPlans().single()

        assertThat(recovered.state).isEqualTo(PlanState.FAILED)
        assertThat(recovered.failure).startsWith("operator reconciled:")
        assertThat(recovered.maintenanceEnabled).isFalse()
    }

    @Test
    fun `every durable execution evidence field keeps review fail closed`() {
        val now = Instant.now()
        val evidence = listOf<Pair<String, (RestartPlan) -> Unit>>(
            "dispatch key" to { it.dispatchedActionKeys += "${it.id}:proxy" },
            "acceptance key" to { it.acceptedActionKeys += "${it.id}:proxy" },
            "backend baseline" to { it.baselineBootIds[smp] = UUID.randomUUID() },
            "proxy baseline" to { it.proxyBaselineBootId = UUID.randomUUID() },
            "result" to { it.targetResults["proxy"] = "accepted" },
            "execution deadline" to { it.executionDeadlineAt = now.plusSeconds(60) },
        )

        evidence.forEach { (name, addEvidence) ->
            val store = MemoryStore()
            val review = plan(
                PlanType.PROXY,
                PlanState.NEEDS_REVIEW,
                now,
                actionStarted = true,
                maintenanceEnabled = true,
                failure = LEGACY_FAILURE,
            )
            addEvidence(review)
            store.save(listOf(review))
            val control = FakeControl()

            val recovered = service(control, store).allPlans().single()

            assertThat(recovered.state).describedAs(name).isEqualTo(PlanState.NEEDS_REVIEW)
            assertThat(recovered.maintenanceEnabled).describedAs(name).isTrue()
            assertThat(control.maintenanceEnables).describedAs(name).isGreaterThanOrEqualTo(1)
        }
    }

    @Test
    fun `legacy failure without actionStarted remains unresolved`() {
        val store = MemoryStore()
        val now = Instant.now()
        val review = plan(
            PlanType.PROXY,
            PlanState.NEEDS_REVIEW,
            now,
            actionStarted = false,
            maintenanceEnabled = true,
            failure = LEGACY_FAILURE,
        )
        store.save(listOf(review))

        val recovered = service(FakeControl(), store).allPlans().single()

        assertThat(recovered.state).isEqualTo(PlanState.NEEDS_REVIEW)
    }

    @Test
    fun `server review is never cleared by proxy network legacy migration`() {
        val store = MemoryStore()
        val now = Instant.now()
        val review = plan(
            PlanType.SERVER,
            PlanState.NEEDS_REVIEW,
            now,
            actionStarted = true,
            failure = LEGACY_FAILURE,
        )
        store.save(listOf(review))

        val recovered = service(FakeControl(), store).allPlans().single()

        assertThat(recovered.state).isEqualTo(PlanState.NEEDS_REVIEW)
    }

    private fun plan(
        type: PlanType,
        state: PlanState,
        now: Instant,
        actionStarted: Boolean = false,
        maintenanceEnabled: Boolean = false,
        completedAt: Instant? = null,
        failure: String = "",
    ): RestartPlan = RestartPlan(
        type = type,
        targets = when (type) {
            PlanType.PROXY -> emptySet()
            PlanType.SERVER -> setOf(smp)
            PlanType.NETWORK -> setOf(hub, smp)
        },
        createdAt = now.minusSeconds(120),
        executionAt = now.minusSeconds(60),
        warningAt = now.minusSeconds(180),
        creator = "console",
        state = state,
        actionStarted = actionStarted,
        maintenanceEnabled = maintenanceEnabled,
        completedAt = completedAt,
        failure = failure,
    )

    private fun service(control: FakeControl, store: RestartPlanStore): NetworkRestartService {
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
            executor = FakeExecutor(),
            control = control,
            store = store,
            backendArm = { server, seconds, _ -> SchedCommandResult.Armed(server, seconds) },
            backendCancel = {},
            audit = { _, _ -> },
        )
    }

    private class FakeExecutor : ExternalRestartExecutor {
        override val name = "fake"
        override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))

        override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
            CompletableFuture.completedFuture(PowerActionResult(true, "ok"))
    }

    private class FakeControl : NetworkControlPort {
        var maintenanceEnables = 0
        override fun broadcast(notice: RestartNotice) = Unit
        override fun disconnectAll(notice: RestartNotice) = Unit
        override fun transferAll(from: ServerId, destinations: List<ServerId>): CompletionStage<TransferSummary> =
            CompletableFuture.completedFuture(TransferSummary(0, 0, 0))

        override fun setMaintenance(enabled: Boolean, duration: Duration) {
            if (enabled) maintenanceEnables++
        }

        override fun maintenanceActive(): Boolean = false
    }

    private class MemoryStore : RestartPlanStore {
        private var saved = emptyList<RestartPlan>()
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

    private companion object {
        const val LEGACY_FAILURE =
            "execution was interrupted after a destructive action may have started"
    }
}
