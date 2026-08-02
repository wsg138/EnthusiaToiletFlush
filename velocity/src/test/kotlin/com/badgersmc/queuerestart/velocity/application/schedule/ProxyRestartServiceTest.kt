package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.application.ports.ProxyLifecyclePort
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.domain.coordinator.RestartState
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class ProxyRestartServiceTest {

    private class RecordingLifecycle : ProxyLifecyclePort {
        data class Broadcast(val message: String, val placeholders: Map<String, String>)

        val broadcasts = mutableListOf<Broadcast>()
        val sounds = mutableListOf<SoundCue>()
        val shutdownReasons = mutableListOf<String>()

        override fun broadcast(miniMessage: String, placeholders: Map<String, String>) {
            broadcasts += Broadcast(miniMessage, placeholders)
        }

        override fun playSound(cue: SoundCue) {
            sounds += cue
        }

        override fun shutdown(reason: String) {
            shutdownReasons += reason
        }
    }

    private val cue = SoundCue("ui.button.click", 0.5f, 1.0f)

    private fun service(lifecycle: RecordingLifecycle) = ProxyRestartService(
        lifecycle = lifecycle,
        marksSupplier = { listOf(60, 30) },
        warningMessageSupplier = { "<gold>Server <server> restarts in <time></gold>" },
        hubSupplier = { ServerId("lobby") },
        soundResolver = { cue },
    )

    @Test
    fun `armed proxy countdown broadcasts globally and cleanly shuts down at T-0`() {
        val lifecycle = RecordingLifecycle()
        val service = service(lifecycle)
        val start = Instant.parse("2026-08-02T04:00:00Z")

        assertThat(service.arm(60)).isTrue()
        assertThat(service.state).isEqualTo(RestartState.ARMED)

        service.tick(start)
        assertThat(service.state).isEqualTo(RestartState.COUNTDOWN)
        assertThat(lifecycle.broadcasts.single().placeholders)
            .containsEntry("server", "proxy")
            .containsEntry("time", "1m")

        service.tick(start.plusSeconds(30))
        assertThat(lifecycle.broadcasts.last().placeholders["time"]).isEqualTo("30s")

        service.tick(start.plusSeconds(60))
        assertThat(service.state).isEqualTo(RestartState.RESTART_SENT)
        assertThat(lifecycle.broadcasts.last().message).isEqualTo(ProxyRestartService.T0_MESSAGE)
        assertThat(lifecycle.shutdownReasons)
            .containsExactly(ProxyRestartService.SHUTDOWN_REASON)
        assertThat(lifecycle.sounds).hasSize(3)

        service.tick(start.plusSeconds(61))
        assertThat(lifecycle.shutdownReasons).hasSize(1)
    }

    @Test
    fun `cancel returns to idle and prevents shutdown`() {
        val lifecycle = RecordingLifecycle()
        val service = service(lifecycle)
        val start = Instant.parse("2026-08-02T04:00:00Z")

        service.arm(60)
        service.tick(start)

        assertThat(service.cancel()).isTrue()
        assertThat(service.state).isEqualTo(RestartState.IDLE)
        assertThat(lifecycle.broadcasts.last().message)
            .isEqualTo(ProxyRestartService.CANCEL_MESSAGE)

        service.tick(start.plusSeconds(60))
        assertThat(lifecycle.shutdownReasons).isEmpty()
        assertThat(service.cancel()).isFalse()
    }

    @Test
    fun `second arm is rejected while proxy countdown is active`() {
        val lifecycle = RecordingLifecycle()
        val service = service(lifecycle)

        assertThat(service.arm(60)).isTrue()
        assertThat(service.arm(120)).isFalse()
        assertThat(service.state).isEqualTo(RestartState.ARMED)
    }
}
