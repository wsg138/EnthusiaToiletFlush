package com.badgersmc.queuerestart.velocity.application.arm

import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.common.schedule.PendingArm
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.UUID

class PendingArmStoreTest {
    private val server = ServerId("survival")
    private val t0 = Instant.parse("2026-05-09T12:00:00Z")
    private val boot = UUID.randomUUID()

    @Test
    fun `polling peeks repeatedly and only matching authenticated acknowledgement removes delivery`() {
        val store = PendingArmStore(ttl = Duration.ofSeconds(60))
        val id = store.put(server, RestartMode.SHUTDOWN, "", 0, boot, t0)
        assertThat(store.peekDelivery(server, t0)?.id).isEqualTo(id)
        assertThat(store.peekDelivery(server, t0)?.id).isEqualTo(id)
        assertThat(store.acknowledge(server, UUID.randomUUID(), t0)).isFalse()
        assertThat(store.peekDelivery(server, t0)).isNotNull
        assertThat(store.acknowledge(server, id, t0)).isTrue()
        assertThat(store.peekDelivery(server, t0)).isNull()
    }

    @Test
    fun `expired delivery is removed without acknowledgement`() {
        val store = PendingArmStore(ttl = Duration.ofSeconds(10))
        store.put(server, RestartMode.SHUTDOWN, "", 0, boot, t0)
        assertThat(store.peekDelivery(server, t0.plusSeconds(11))).isNull()
    }

    @Test
    fun `replacement JVM cannot receive an arm targeted at the previous boot`() {
        val store = PendingArmStore(ttl = Duration.ofSeconds(60))
        val id = store.put(server, RestartMode.SHUTDOWN, "", 0, boot, t0)

        assertThat(store.peekDeliveryForBoot(server, UUID.randomUUID(), t0)?.id).isNull()
        assertThat(store.peekDelivery(server, t0)).isNull()
        assertThat(store.acknowledge(server, id, t0)).isFalse()
    }

    @Test
    fun `cancel replaces arm and is acknowledged by its own id`() {
        val store = PendingArmStore()
        store.put(server, RestartMode.SHUTDOWN, "", 0, boot, t0)
        val cancelId = store.cancel(server, boot, t0.plusSeconds(1))
        val pending = store.peekDelivery(server, t0.plusSeconds(2))
        assertThat(pending?.id).isEqualTo(cancelId)
        assertThat(pending?.delivery).isEqualTo(PendingArmStore.Delivery.Cancel)
        assertThat(store.acknowledge(server, cancelId, t0.plusSeconds(2))).isTrue()
    }

    @Test
    fun `pending delivery survives proxy restart`(@TempDir temp: Path) {
        val file = temp.resolve("pending.state")
        val first = PendingArmStore(persistencePath = file)
        val id = UUID.randomUUID()
        val arm = PendingArm(id, 1, RestartMode.SHUTDOWN, "")
        first.put(server, arm, boot, t0)

        val restored = PendingArmStore(persistencePath = file)
        val pending = restored.peekDelivery(server, t0.plusSeconds(1))
        assertThat(pending?.id).isEqualTo(id)
        assertThat(pending?.expectedBootId).isEqualTo(boot)
        assertThat(pending?.delivery).isEqualTo(PendingArmStore.Delivery.Arm(arm))
    }
}
