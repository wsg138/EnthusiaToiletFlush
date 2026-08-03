package com.badgersmc.queuerestart.velocity.infrastructure.persistence

import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.plan.PlanState
import com.badgersmc.queuerestart.velocity.domain.plan.PlanType
import com.badgersmc.queuerestart.velocity.domain.plan.RestartPlan
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class AtomicRestartPlanStoreTest {
    @TempDir lateinit var temp: Path

    @Test
    fun `lifecycle verification fields round trip`() {
        val path = temp.resolve("network-restarts.state")
        val target = ServerId("SMP")
        val baseline = UUID.randomUUID()
        val proxy = UUID.randomUUID()
        val now = Instant.now()
        val plan = RestartPlan(
            type = PlanType.SERVER,
            targets = setOf(target),
            createdAt = now.minusSeconds(5),
            executionAt = now,
            warningAt = now.minusSeconds(10),
            creator = "console",
            state = PlanState.DISPATCHING,
            actionStarted = true,
            maintenanceEnabled = false,
            baselineBootIds = ConcurrentHashMap(mapOf(target to baseline)),
            proxyBaselineBootId = proxy,
            executionDeadlineAt = now.plusSeconds(600),
        ).also {
            it.dispatchedActionKeys += "action"
            it.acceptedActionKeys += "action"
            it.targetResults["action"] = "accepted"
        }
        val store = AtomicRestartPlanStore(path) {}
        store.save(listOf(plan))

        val restored = store.load().single()
        assertThat(restored.baselineBootIds).containsEntry(target, baseline)
        assertThat(restored.proxyBaselineBootId).isEqualTo(proxy)
        assertThat(restored.executionDeadlineAt).isEqualTo(now.plusSeconds(600))
        assertThat(restored.acceptedActionKeys).containsExactly("action")
    }

    @Test
    fun `proxy replacement completes a durably dispatched plan when its callback was lost`() {
        val path = temp.resolve("network-restarts.state")
        val now = Instant.now()
        val plan = RestartPlan(
            type = PlanType.PROXY,
            targets = emptySet(),
            createdAt = now.minusSeconds(120),
            executionAt = now.minusSeconds(60),
            warningAt = now.minusSeconds(180),
            creator = "console",
            state = PlanState.NEEDS_REVIEW,
            actionStarted = true,
            maintenanceEnabled = true,
            proxyBaselineBootId = UUID.randomUUID(),
            executionDeadlineAt = now.minusSeconds(1),
            failure = "persisted execution is missing durable action acceptance",
        ).also {
            it.dispatchedActionKeys += "${it.id}:proxy"
        }
        val warnings = mutableListOf<String>()
        val store = AtomicRestartPlanStore(path, warnings::add)
        store.save(listOf(plan))

        val restored = store.load().single()
        val proxyKey = "${restored.id}:proxy"
        assertThat(restored.state).isEqualTo(PlanState.COMPLETED)
        assertThat(restored.acceptedActionKeys).contains(proxyKey)
        assertThat(restored.targetResults["verification"]).contains("durably dispatched")
        assertThat(restored.maintenanceEnabled).isFalse()
        assertThat(restored.executionDeadlineAt).isNull()
        assertThat(restored.failure).isEmpty()
        assertThat(restored.completedAt).isNotNull()
        assertThat(warnings).anyMatch {
            it.contains(restored.id.toString()) && it.contains("completed")
        }
    }

    @Test
    fun `network replacement resumes backend verification when only proxy callback was lost`() {
        val path = temp.resolve("network-restarts.state")
        val hub = ServerId("HUB")
        val smp = ServerId("SMP")
        val now = Instant.now()
        val plan = RestartPlan(
            type = PlanType.NETWORK,
            targets = setOf(hub, smp),
            createdAt = now.minusSeconds(120),
            executionAt = now.minusSeconds(60),
            warningAt = now.minusSeconds(180),
            creator = "console",
            state = PlanState.NEEDS_REVIEW,
            actionStarted = true,
            maintenanceEnabled = true,
            baselineBootIds = ConcurrentHashMap(
                mapOf(hub to UUID.randomUUID(), smp to UUID.randomUUID()),
            ),
            proxyBaselineBootId = UUID.randomUUID(),
            executionDeadlineAt = now.minusSeconds(1),
            failure = "persisted execution is missing durable action acceptance",
        ).also {
            val hubKey = "${it.id}:${hub.value}"
            val smpKey = "${it.id}:${smp.value}"
            it.dispatchedActionKeys += setOf(hubKey, smpKey, "${it.id}:proxy")
            it.acceptedActionKeys += setOf(hubKey, smpKey)
        }
        val warnings = mutableListOf<String>()
        val store = AtomicRestartPlanStore(path, warnings::add)
        store.save(listOf(plan))

        val beforeLoad = Instant.now()
        val restored = store.load().single()
        val proxyKey = "${restored.id}:proxy"
        assertThat(restored.state).isEqualTo(PlanState.DISPATCHING)
        assertThat(restored.acceptedActionKeys).contains(proxyKey)
        assertThat(restored.maintenanceEnabled).isTrue()
        assertThat(restored.failure).isEmpty()
        assertThat(restored.executionDeadlineAt).isAfter(beforeLoad.plusSeconds(500))
        assertThat(warnings).anyMatch {
            it.contains(restored.id.toString()) && it.contains("resumed")
        }
    }

    @Test
    fun `prepared but undispatched proxy plan remains unresolved`() {
        val path = temp.resolve("network-restarts.state")
        val now = Instant.now()
        val plan = RestartPlan(
            type = PlanType.PROXY,
            targets = emptySet(),
            createdAt = now.minusSeconds(120),
            executionAt = now.minusSeconds(60),
            warningAt = now.minusSeconds(180),
            creator = "console",
            state = PlanState.NEEDS_REVIEW,
            actionStarted = true,
            maintenanceEnabled = true,
            proxyBaselineBootId = UUID.randomUUID(),
            executionDeadlineAt = now.minusSeconds(1),
            failure = "restart handoff was prepared but publication was not durably confirmed",
        )
        val store = AtomicRestartPlanStore(path) {}
        store.save(listOf(plan))

        val restored = store.load().single()
        assertThat(restored.state).isEqualTo(PlanState.NEEDS_REVIEW)
        assertThat(restored.acceptedActionKeys).isEmpty()
        assertThat(restored.maintenanceEnabled).isTrue()
    }

    @Test
    fun `corrupt state fails every startup and preserves original evidence`() {
        val path = temp.resolve("network-restarts.state")
        Files.writeString(path, "corrupt-state\n")
        val warnings = mutableListOf<String>()
        val store = AtomicRestartPlanStore(path, warnings::add)

        repeat(2) {
            assertThatThrownBy(store::load).isInstanceOf(IllegalStateException::class.java)
            assertThat(Files.readString(path)).isEqualTo("corrupt-state\n")
        }
        assertThat(warnings).allMatch { it.contains("corrupt") }
    }

    @Test
    fun `duplicate persisted plan ids fail closed`() {
        val path = temp.resolve("network-restarts.state")
        val now = Instant.now()
        val plan = RestartPlan(
            type = PlanType.PROXY,
            targets = emptySet(),
            createdAt = now.minusSeconds(5),
            executionAt = now.plusSeconds(60),
            warningAt = now,
            creator = "console",
        )
        val store = AtomicRestartPlanStore(path) {}
        store.save(listOf(plan))
        val line = Files.readString(path)
        Files.writeString(path, line + line)

        assertThatThrownBy(store::load)
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("refusing to start")
    }
}
