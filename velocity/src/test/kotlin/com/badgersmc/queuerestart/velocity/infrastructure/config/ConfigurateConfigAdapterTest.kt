package com.badgersmc.queuerestart.velocity.infrastructure.config

import com.badgersmc.queuerestart.velocity.application.drain.DrainOrder
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.LocalTime
import java.time.ZoneId

/**
 * REQ-006 + impl §8.
 *
 * Covers:
 *  - canonical sample loads to a fully populated [QueueRestartConfig]
 *  - proxy restart times, time zone, and warning lead are parsed
 *  - sound volume > 1.0 rejected with a clear error
 *  - sound volume > 0.8 (but ≤ 1.0) emits a warning via the supplied logger
 *  - malformed YAML rejected
 */
class ConfigurateConfigAdapterTest {

    private val canonical = """
        hub-server: lobby
        fallback-hubs: [lobby2, lobby3]
        drain:
          batch-size: 10
          batch-interval-ticks: 40
          drain-lead-seconds: 30
          force-drain-timeout-seconds: 120
          drain-order: priority-asc
        rejoin:
          enabled: true
          enqueue-on-server-up: true
          release-on-checkhacks-cleared: true
          check-gate-timeout-seconds: 60
          release-on-timeout: true
          ping-poll-seconds: 3
        rank-ladder:
          group.owner: 1000
          group.vip: 100
          default: 0
        countdown:
          marks-seconds: [60, 30, 10, 5, 1]
          message: "<gold>warning"
          message-t0: "<red>now"
          cancel-message: "<green>cancelled"
        sounds:
          warn:  { key: block.note_block.bell, volume: 0.4, pitch: 1.0 }
          tick:  { key: ui.button.click,       volume: 0.7, pitch: 1.0 }
        proxy-restart:
          restart-times: ["04:00", "16:30"]
          time-zone: "America/New_York"
          warn-minutes: 20
    """.trimIndent()

    private fun yaml(@TempDir dir: Path, content: String): Path {
        val file = dir.resolve("config.yml")
        Files.writeString(file, content)
        return file
    }

    @Test
    fun `canonical config loads fully`(@TempDir dir: Path) {
        val warnings = mutableListOf<String>()
        val cfg = ConfigurateConfigAdapter(yaml(dir, canonical), warner = warnings::add)
            .snapshot()

        assertThat(cfg.hubServer).isEqualTo(ServerId("lobby"))
        assertThat(cfg.fallbackHubs).containsExactly(ServerId("lobby2"), ServerId("lobby3"))
        assertThat(cfg.drain.batchSize).isEqualTo(10)
        assertThat(cfg.drain.drainOrder).isEqualTo(DrainOrder.PRIORITY_ASC)
        assertThat(cfg.rejoin.checkGateTimeoutSeconds).isEqualTo(60)
        assertThat(cfg.countdown.marksSeconds).containsExactly(60, 30, 10, 5, 1)
        assertThat(cfg.sounds).containsKey("warn").containsKey("tick")
        assertThat(cfg.sounds["tick"]!!.volume).isEqualTo(0.7f)
        assertThat(cfg.rankLadder["group.owner"]).isEqualTo(1000)
        assertThat(cfg.rankDefault).isEqualTo(0)
        assertThat(cfg.proxyRestart.restartTimes)
            .containsExactly(LocalTime.of(4, 0), LocalTime.of(16, 30))
        assertThat(cfg.proxyRestart.zone).isEqualTo(ZoneId.of("America/New_York"))
        assertThat(cfg.proxyRestart.warnMinutes).isEqualTo(20)
    }

    @Test
    fun `missing proxy restart block defaults to disabled schedule`(@TempDir dir: Path) {
        val withoutProxy = canonical.replace(
            Regex("(?ms)^proxy-restart:\n(?:  .*\n?)*$"),
            "",
        )

        val cfg = ConfigurateConfigAdapter(yaml(dir, withoutProxy), warner = {}).snapshot()

        assertThat(cfg.proxyRestart.restartTimes).isEmpty()
        assertThat(cfg.proxyRestart.warnMinutes).isEqualTo(20)
    }

    @Test
    fun `invalid proxy restart time is rejected`(@TempDir dir: Path) {
        val bad = canonical.replace("\"04:00\"", "\"not-a-time\"")

        assertThatThrownBy {
            ConfigurateConfigAdapter(yaml(dir, bad), warner = {}).snapshot()
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("restart-time")
    }

    @Test
    fun `invalid proxy restart time zone is rejected`(@TempDir dir: Path) {
        val bad = canonical.replace("America/New_York", "Not/A_Zone")

        assertThatThrownBy {
            ConfigurateConfigAdapter(yaml(dir, bad), warner = {}).snapshot()
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("time-zone")
    }

    @Test
    fun `volume above 1_0 is rejected (REQ-006)`(@TempDir dir: Path) {
        val bad = canonical.replace("volume: 0.4", "volume: 1.5")
        assertThatThrownBy {
            ConfigurateConfigAdapter(yaml(dir, bad), warner = {}).snapshot()
        }.isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("volume")
    }

    @Test
    fun `volume above 0_8 emits a warning naming the offending key (REQ-006)`(@TempDir dir: Path) {
        val warned = canonical.replace("volume: 0.7", "volume: 0.95")
        val warnings = mutableListOf<String>()

        ConfigurateConfigAdapter(yaml(dir, warned), warner = warnings::add).snapshot()

        assertThat(warnings).anyMatch { it.contains("tick") && it.contains("0.95") }
    }

    @Test
    fun `volume at exactly 0_8 does not warn`(@TempDir dir: Path) {
        val edge = canonical.replace("volume: 0.7", "volume: 0.8")
        val warnings = mutableListOf<String>()

        ConfigurateConfigAdapter(yaml(dir, edge), warner = warnings::add).snapshot()

        assertThat(warnings).noneMatch { it.contains("tick") }
    }

    @Test
    fun `missing required field is rejected`(@TempDir dir: Path) {
        // valid YAML, but `hub-server` is missing — adapter must reject
        val incomplete = canonical.replace(Regex("(?m)^hub-server: lobby\n"), "")
        assertThatThrownBy {
            ConfigurateConfigAdapter(yaml(dir, incomplete), warner = {}).snapshot()
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `reload reparses the file`(@TempDir dir: Path) {
        val file = yaml(dir, canonical)
        val adapter = ConfigurateConfigAdapter(file, warner = {})

        assertThat(adapter.snapshot().hubServer).isEqualTo(ServerId("lobby"))

        Files.writeString(file, canonical.replace("hub-server: lobby", "hub-server: hub2"))
        adapter.reload()

        assertThat(adapter.snapshot().hubServer).isEqualTo(ServerId("hub2"))
    }
}
