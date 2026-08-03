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
            baselineBootIds = java.util.concurrent.ConcurrentHashMap(mapOf(target to baseline)),
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
