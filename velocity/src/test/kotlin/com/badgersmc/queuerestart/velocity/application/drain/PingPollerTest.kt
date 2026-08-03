package com.badgersmc.queuerestart.velocity.application.drain

import com.badgersmc.queuerestart.common.schedule.CompanionCapabilities
import com.badgersmc.queuerestart.velocity.application.companion.CompanionRegistry
import com.badgersmc.queuerestart.velocity.application.schedule.CoordinatorRegistry
import com.badgersmc.queuerestart.velocity.domain.cohort.Cohort
import com.badgersmc.queuerestart.velocity.domain.coordinator.RestartState
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID

class PingPollerTest {
    private val server = ServerId("survival")

    private fun prime(registry: CoordinatorRegistry, baseline: UUID) {
        registry.get(server).apply {
            arm(Cohort(emptySet()), 60)
            beginCountdown()
            beginDrain()
            restartSent(baseline)
        }
    }

    private fun poller(
        registry: CoordinatorRegistry,
        companions: CompanionRegistry,
        finished: MutableList<ServerId>,
        timeouts: MutableList<String> = mutableListOf(),
    ) = PingPoller(
        registry = registry,
        companions = companions,
        onReady = { target, _ -> finished += target },
        heartbeatTimeout = { Duration.ofSeconds(10) },
        executionTimeout = { Duration.ofSeconds(60) },
        onTimeout = { _, reason -> timeouts += reason },
    )

    @Test
    fun `fresh same boot id does not fabricate a down transition`() {
        val boot = UUID.randomUUID()
        val registry = CoordinatorRegistry().also { prime(it, boot) }
        val companions = CompanionRegistry()
        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        companions.record(server, boot, CompanionCapabilities.REQUIRED, t0)
        val finished = mutableListOf<ServerId>()

        poller(registry, companions, finished).tick(t0.plusSeconds(1))

        assertThat(registry.get(server).state).isEqualTo(RestartState.RESTART_SENT)
        assertThat(finished).isEmpty()
    }

    @Test
    fun `stale heartbeat proves down then new boot proves up`() {
        val boot1 = UUID.randomUUID()
        val boot2 = UUID.randomUUID()
        val registry = CoordinatorRegistry().also { prime(it, boot1) }
        val companions = CompanionRegistry()
        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        companions.record(server, boot1, CompanionCapabilities.REQUIRED, t0)
        val finished = mutableListOf<ServerId>()
        val poller = poller(registry, companions, finished)

        poller.tick(t0)
        poller.tick(t0.plusSeconds(11))
        assertThat(registry.get(server).state).isEqualTo(RestartState.SERVER_DOWN)

        companions.record(server, boot2, CompanionCapabilities.REQUIRED, t0.plusSeconds(12))
        poller.tick(t0.plusSeconds(12))
        assertThat(finished).containsExactly(server)
    }

    @Test
    fun `immediate authenticated boot change proves restart even if downtime was shorter than poll cadence`() {
        val baseline = UUID.randomUUID()
        val registry = CoordinatorRegistry().also { prime(it, baseline) }
        val companions = CompanionRegistry()
        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        companions.record(server, baseline, CompanionCapabilities.REQUIRED, t0)
        val finished = mutableListOf<ServerId>()
        val poller = poller(registry, companions, finished)
        poller.tick(t0)
        companions.record(server, UUID.randomUUID(), CompanionCapabilities.REQUIRED, t0.plusSeconds(2))
        poller.tick(t0.plusSeconds(2))
        assertThat(finished).containsExactly(server)
    }

    @Test
    fun `same boot returning after outage is not accepted as restart completion`() {
        val boot = UUID.randomUUID()
        val registry = CoordinatorRegistry().also { prime(it, boot) }
        val companions = CompanionRegistry()
        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        companions.record(server, boot, CompanionCapabilities.REQUIRED, t0)
        val finished = mutableListOf<ServerId>()
        val poller = poller(registry, companions, finished)
        poller.tick(t0)
        poller.tick(t0.plusSeconds(11))
        companions.record(server, boot, CompanionCapabilities.REQUIRED, t0.plusSeconds(12))
        poller.tick(t0.plusSeconds(12))
        assertThat(registry.get(server).state).isEqualTo(RestartState.SERVER_DOWN)
        assertThat(finished).isEmpty()
    }

    @Test
    fun `no boot change within execution timeout reports a stuck restart`() {
        val boot = UUID.randomUUID()
        val registry = CoordinatorRegistry().also { prime(it, boot) }
        val companions = CompanionRegistry()
        val t0 = Instant.parse("2026-01-01T00:00:00Z")
        companions.record(server, boot, CompanionCapabilities.REQUIRED, t0)
        val finished = mutableListOf<ServerId>()
        val timeouts = mutableListOf<String>()
        val poller = poller(registry, companions, finished, timeouts)

        poller.tick(t0)
        poller.tick(t0.plusSeconds(61))

        assertThat(timeouts).hasSize(1)
        assertThat(timeouts.single()).contains("execution timeout")
        assertThat(finished).isEmpty()
    }
}
