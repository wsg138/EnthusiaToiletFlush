package com.badgersmc.queuerestart.paper

import com.badgersmc.queuerestart.common.protocol.RestartMode
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.UUID

class RestartExecutorTest {
    private class FakeControl : ServerControl {
        var shutdownCalls = 0
        override fun shutdown() { shutdownCalls++ }
    }

    private class CapturingScheduler : RestartScheduler {
        data class Run(val delay: Int, val action: () -> Unit, var cancelled: Boolean = false)
        val queued = mutableListOf<Run>()
        override fun runAfterSeconds(delaySeconds: Int, action: () -> Unit): ScheduledHandle {
            val run = Run(delaySeconds, action)
            queued += run
            return object : ScheduledHandle { override fun cancel() { run.cancelled = true } }
        }
        fun fireAll() {
            val pending = queued.toList()
            queued.clear()
            pending.filterNot(Run::cancelled).forEach { it.action() }
        }
    }

    @Test
    fun `shutdown delivery executes once`() {
        val control = FakeControl()
        val id = UUID.randomUUID()
        val executor = RestartExecutor(control)
        assertThat(executor.execute(id, RestartMode.SHUTDOWN, "", 0)).isTrue()
        assertThat(executor.execute(id, RestartMode.SHUTDOWN, "", 0)).isFalse()
        assertThat(control.shutdownCalls).isEqualTo(1)
        assertThat(executor.abort(UUID.randomUUID())).isFalse()
    }

    @Test
    fun `non shutdown modes and arguments are rejected`() {
        val executor = RestartExecutor(FakeControl())
        assertThatThrownBy { executor.execute(UUID.randomUUID(), RestartMode.COMMAND, "restart", 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { executor.execute(UUID.randomUUID(), RestartMode.EXIT_CODE, "42", 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { executor.execute(UUID.randomUUID(), RestartMode.SHUTDOWN, "restart", 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `negative delay is rejected before id is consumed`() {
        val id = UUID.randomUUID()
        val executor = RestartExecutor(FakeControl())
        assertThatThrownBy { executor.execute(id, RestartMode.SHUTDOWN, "", -1) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThat(executor.execute(id, RestartMode.SHUTDOWN, "", 0)).isTrue()
    }

    @Test
    fun `cancellation is idempotent and prevents scheduled shutdown`() {
        val control = FakeControl()
        val scheduler = CapturingScheduler()
        val executor = RestartExecutor(control, scheduler)
        executor.execute(UUID.randomUUID(), RestartMode.SHUTDOWN, "", 60)
        val cancelId = UUID.randomUUID()
        assertThat(executor.abort(cancelId)).isTrue()
        assertThat(executor.abort(cancelId)).isFalse()
        scheduler.fireAll()
        assertThat(control.shutdownCalls).isZero()
    }

    @Test
    fun `processed restart survives executor recreation`(@TempDir temp: Path) {
        val file = temp.resolve("processed.state")
        val id = UUID.randomUUID()
        val scheduler = CapturingScheduler()
        RestartExecutor(FakeControl(), scheduler, ProcessedDeliveryStore(file)).execute(id, RestartMode.SHUTDOWN, "", 60)
        val recreated = RestartExecutor(FakeControl(), CapturingScheduler(), ProcessedDeliveryStore(file))
        assertThat(recreated.execute(id, RestartMode.SHUTDOWN, "", 60)).isFalse()
    }

    @Test
    fun `scheduler failure rolls back idempotency record`() {
        val id = UUID.randomUUID()
        val failing = RestartScheduler { _, _ -> error("scheduler unavailable") }
        val processed = ProcessedDeliveryStore()
        val executor = RestartExecutor(FakeControl(), failing, processed)
        assertThatThrownBy { executor.execute(id, RestartMode.SHUTDOWN, "", 0) }
            .isInstanceOf(IllegalStateException::class.java)
        assertThat(RestartExecutor(FakeControl(), processedDeliveries = processed).execute(id, RestartMode.SHUTDOWN, "", 0))
            .isTrue()
    }
}
