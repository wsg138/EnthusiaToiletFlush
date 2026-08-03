package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.application.ports.AudiencePort
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.domain.countdown.CountdownSchedule
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * REQ-003, REQ-004.
 *
 * Drives `CountdownSchedule.fireAt` and dispatches the configured chat
 * message + sound for every mark including T-0. Idempotent: ticking
 * the same second twice fires nothing the second time.
 */
class CountdownBroadcasterTest {

    private val target = ServerId("survival")
    private val hub = ServerId("lobby")

    private class RecordingAudience : AudiencePort {
        data class Broadcast(val server: ServerId, val message: String, val placeholders: Map<String, String>)
        data class Sound(val server: ServerId, val cue: SoundCue)
        val broadcasts = mutableListOf<Broadcast>()
        val sounds = mutableListOf<Sound>()
        override fun broadcast(target: ServerId, miniMessage: String, placeholders: Map<String, String>) {
            broadcasts += Broadcast(target, miniMessage, placeholders)
        }
        override fun disconnect(playerId: PlayerId, miniMessage: String, placeholders: Map<String, String>) = Unit
        override fun playSound(target: ServerId, cue: SoundCue) {
            sounds += Sound(target, cue)
        }
    }

    private val schedule = CountdownSchedule(listOf(60, 30, 10, 5, 1))

    private val tick = SoundCue("ui.button.click", 0.5f, 1.0f)
    private val t0Sound = SoundCue("block.beacon.deactivate", 0.5f, 1.0f)
    private val sounds: (Int) -> SoundCue? = { sec ->
        when {
            sec == 0 -> t0Sound
            sec <= 10 -> tick
            else -> null
        }
    }

    private fun broadcaster(audience: AudiencePort = RecordingAudience()): CountdownBroadcaster =
        CountdownBroadcaster(
            audience = audience,
            messageTemplate = "<gold>Server <yellow><server></yellow> in <yellow><time></yellow>",
            t0Template = "<red>Sending you to <yellow><hub></yellow>",
            soundResolver = sounds,
        )

    @Test
    fun `mark second fires broadcast and sound`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)
        b.register(target, schedule, hub)

        b.tick(target, secondsRemaining = 10)

        assertThat(audience.broadcasts).hasSize(1)
        val msg = audience.broadcasts[0]
        assertThat(msg.server).isEqualTo(target)
        assertThat(msg.placeholders["server"]).isEqualTo("survival")
        assertThat(msg.placeholders["time"]).isEqualTo("10 seconds")
        assertThat(audience.sounds).containsExactly(RecordingAudience.Sound(target, tick))
    }

    @Test
    fun `non-mark second fires nothing`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)
        b.register(target, schedule, hub)

        b.tick(target, secondsRemaining = 11)

        assertThat(audience.broadcasts).isEmpty()
        assertThat(audience.sounds).isEmpty()
    }

    @Test
    fun `T-0 uses t0 template and t0 sound`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)
        b.register(target, schedule, hub)

        b.tick(target, secondsRemaining = 0)

        assertThat(audience.broadcasts).hasSize(1)
        assertThat(audience.broadcasts[0].message).contains("Sending you to")
        assertThat(audience.broadcasts[0].placeholders["hub"]).isEqualTo("lobby")
        assertThat(audience.sounds).containsExactly(RecordingAudience.Sound(target, t0Sound))
    }

    @Test
    fun `repeated tick at same second is idempotent`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)
        b.register(target, schedule, hub)

        b.tick(target, secondsRemaining = 60)
        b.tick(target, secondsRemaining = 60)
        b.tick(target, secondsRemaining = 60)

        assertThat(audience.broadcasts).hasSize(1)
        assertThat(audience.sounds).hasSize(0) // 60s has no mapped sound in this fixture
    }

    @Test
    fun `cancel stops further fires`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)
        b.register(target, schedule, hub)

        b.cancel(target)
        b.tick(target, secondsRemaining = 10)

        assertThat(audience.broadcasts).isEmpty()
    }

    @Test
    fun `tick for unregistered target is a no-op`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)

        b.tick(target, secondsRemaining = 10)

        assertThat(audience.broadcasts).isEmpty()
    }

    @Test
    fun `multiple targets are tracked independently`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)
        val creative = ServerId("creative")
        b.register(target, schedule, hub)
        b.register(creative, schedule, hub)

        b.tick(target, 10)
        b.tick(creative, 30)

        assertThat(audience.broadcasts.map { it.server })
            .containsExactly(target, creative)
        assertThat(audience.broadcasts.map { it.placeholders["time"] })
            .containsExactly("10 seconds", "30 seconds")
    }

    @Test
    fun `time placeholder formats human-readable durations`() {
        val audience = RecordingAudience()
        val schedWithMinutes = CountdownSchedule(listOf(60, 120, 300))
        val b = broadcaster(audience)
        b.register(target, schedWithMinutes, hub)

        b.tick(target, 300)
        b.tick(target, 120)
        b.tick(target, 60)

        val times = audience.broadcasts.map { it.placeholders["time"] }
        assertThat(times).containsExactly("5 minutes", "2 minutes", "1 minute")
    }

    @Test
    fun `late first observation still fires initial configured mark`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)
        b.register(target, schedule, hub, startingSeconds = 60)

        b.tick(target, 59)

        assertThat(audience.broadcasts.single().placeholders["time"]).isEqualTo("1 minute")
    }

    @Test
    fun `crossing 10 second mark emits it once`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)
        b.register(target, schedule, hub, startingSeconds = 11)

        b.tick(target, 11)
        b.tick(target, 9)
        b.tick(target, 9)

        assertThat(audience.broadcasts.map { it.placeholders["time"] }).containsExactly("10 seconds")
        assertThat(audience.sounds).containsExactly(RecordingAudience.Sound(target, tick))
    }

    @Test
    fun `large jump consumes stale marks and emits only newest due mark`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)
        b.register(target, schedule, hub, startingSeconds = 61)

        b.tick(target, 61)
        b.tick(target, 9)
        b.tick(target, 8)

        assertThat(audience.broadcasts.map { it.placeholders["time"] }).containsExactly("10 seconds")
    }

    @Test
    fun `T-0 fires exactly once after delayed ticks`() {
        val audience = RecordingAudience()
        val b = broadcaster(audience)
        b.register(target, schedule, hub, startingSeconds = 2)

        b.tick(target, 1)
        b.tick(target, 0)
        b.tick(target, 0)

        assertThat(audience.broadcasts.count { it.message.contains("Sending you") }).isEqualTo(1)
        assertThat(audience.sounds.count { it.cue == t0Sound }).isEqualTo(1)
    }

}
