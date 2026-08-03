package com.badgersmc.queuerestart.velocity.infrastructure.config

import com.badgersmc.queuerestart.velocity.application.drain.DrainOrder
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * REQ-006 + impl §8.
 *
 * Covers:
 *  - canonical sample loads to a fully populated [QueueRestartConfig]
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
        access-messages:
          backend-restarting: "<red><server> restart"
          backend-whitelisted: "<yellow><server> whitelist"
          drain-disconnect: "<red><server> disconnected"
          network-maintenance: "<red>network maintenance"
        sounds:
          warn:  { key: block.note_block.bell, volume: 0.4, pitch: 1.0 }
          tick:  { key: ui.button.click,       volume: 0.7, pitch: 1.0 }
        schedules:
          nightly:
            server: survival
            cron: "0 4 * * *"
            warn-minutes: 20
        network-restart:
          enabled: true
          executor: DRY_RUN
          panel-url: https://panel.example.com
          api-key: ${'$'}{PTERODACTYL_API_KEY}
          proxy-server-id: proxy1234
          servers:
            lobby: lobby1234
            survival: survival1234
          full-network:
            members: [lobby, survival]
            hub-servers: [lobby]
          announcement-points-seconds: [7200, 3600, 60, 10]
        automatic-schedules:
          nightly:
            enabled: true
            type: SERVER
            targets: [survival]
            time: "00:00"
            days: [MONDAY]
            warning-window: 2h
            timezone: America/Indiana/Indianapolis
            reason: Nightly restart
            silent: false
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
        assertThat(cfg.accessMessages.backendRestarting).contains("<server>").contains("restart")
        assertThat(cfg.accessMessages.backendWhitelisted).contains("whitelist")
        assertThat(cfg.sounds).containsKey("warn").containsKey("tick")
        assertThat(cfg.sounds["tick"]!!.volume).isEqualTo(0.7f)
        assertThat(cfg.rankLadder["group.owner"]).isEqualTo(1000)
        assertThat(cfg.rankDefault).isEqualTo(0)
        assertThat(cfg.networkRestart.members).containsExactly(ServerId("lobby"), ServerId("survival"))
        assertThat(cfg.schedules.single().name).isEqualTo("nightly")
    }

    @Test
    fun `missing access messages use safe defaults`(@TempDir dir: Path) {
        val withoutMessages = canonical.replace(
            Regex("(?m)^access-messages:\n(?:  .+\n){4}"),
            "",
        )

        val cfg = ConfigurateConfigAdapter(yaml(dir, withoutMessages), warner = {}).snapshot()

        assertThat(cfg.accessMessages.backendRestarting).contains("<server>")
        assertThat(cfg.accessMessages.backendWhitelisted.lowercase()).contains("whitelist")
        assertThat(cfg.accessMessages.networkMaintenance).contains("Network restart")
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
