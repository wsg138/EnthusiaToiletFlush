package com.badgersmc.queuerestart.velocity.infrastructure.config

import com.badgersmc.queuerestart.velocity.application.drain.DrainOrder
import com.badgersmc.queuerestart.velocity.application.ports.ConfigPort
import com.badgersmc.queuerestart.velocity.application.ports.CountdownConfig
import com.badgersmc.queuerestart.velocity.application.ports.DrainConfig
import com.badgersmc.queuerestart.velocity.application.ports.ProxyRestartScheduleConfig
import com.badgersmc.queuerestart.velocity.application.ports.QueueRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.RejoinConfig
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.spongepowered.configurate.ConfigurationNode
import org.spongepowered.configurate.yaml.YamlConfigurationLoader
import java.nio.file.Path
import java.time.LocalTime
import java.time.ZoneId
import java.time.format.DateTimeParseException

/**
 * REQ-006 + impl §8.
 *
 * Loads `config.yml` via Configurate-YAML. Sound volumes > 1.0 are
 * rejected; volumes > 0.8 (but ≤ 1.0) emit a warning through [warner],
 * naming the offending key.
 */
class ConfigurateConfigAdapter(
    private val path: Path,
    private val warner: (String) -> Unit,
) : ConfigPort {

    private var current: QueueRestartConfig = parse()

    override fun snapshot(): QueueRestartConfig = current

    override fun reload() {
        current = parse()
    }

    private fun parse(): QueueRestartConfig {
        val loader = YamlConfigurationLoader.builder().path(path).build()
        val root = loader.load()

        return QueueRestartConfig(
            hubServer = ServerId(root.node("hub-server").requireString()),
            fallbackHubs = root.node("fallback-hubs").childrenList()
                .map { ServerId(it.requireString()) },
            drain = parseDrain(root.node("drain")),
            rejoin = parseRejoin(root.node("rejoin")),
            countdown = parseCountdown(root.node("countdown")),
            sounds = parseSounds(root.node("sounds")),
            rankLadder = parseRankLadder(root.node("rank-ladder")),
            rankDefault = root.node("rank-ladder", "default").getInt(0),
            proxyRestart = parseProxyRestart(root.node("proxy-restart")),
        )
    }

    private fun parseDrain(node: ConfigurationNode) = DrainConfig(
        batchSize = node.node("batch-size").requireInt(),
        batchIntervalTicks = node.node("batch-interval-ticks").requireInt(),
        drainLeadSeconds = node.node("drain-lead-seconds").requireInt(),
        forceDrainTimeoutSeconds = node.node("force-drain-timeout-seconds").requireInt(),
        drainOrder = parseDrainOrder(node.node("drain-order").requireString()),
    )

    private fun parseRejoin(node: ConfigurationNode) = RejoinConfig(
        enabled = node.node("enabled").getBoolean(true),
        enqueueOnServerUp = node.node("enqueue-on-server-up").getBoolean(true),
        releaseOnCheckhacksCleared = node.node("release-on-checkhacks-cleared").getBoolean(true),
        checkGateTimeoutSeconds = node.node("check-gate-timeout-seconds").requireInt(),
        releaseOnTimeout = node.node("release-on-timeout").getBoolean(true),
        pingPollSeconds = node.node("ping-poll-seconds").requireInt(),
    )

    private fun parseCountdown(node: ConfigurationNode) = CountdownConfig(
        marksSeconds = node.node("marks-seconds").childrenList().map { it.requireInt() },
        message = node.node("message").requireString(),
        messageT0 = node.node("message-t0").requireString(),
        cancelMessage = node.node("cancel-message").requireString(),
    )

    private fun parseProxyRestart(node: ConfigurationNode): ProxyRestartScheduleConfig {
        val times = node.node("restart-times").childrenList().map { child ->
            val raw = child.requireString().trim()
            try {
                LocalTime.parse(raw)
            } catch (e: DateTimeParseException) {
                throw IllegalArgumentException(
                    "invalid proxy restart-time '$raw' (expected HH:mm)",
                    e,
                )
            }
        }
        val zoneRaw = node.node("time-zone").string ?: ZoneId.systemDefault().id
        val zone = try {
            ZoneId.of(zoneRaw)
        } catch (e: Exception) {
            throw IllegalArgumentException("invalid proxy restart time-zone '$zoneRaw'", e)
        }
        val warnMinutes = node.node("warn-minutes").getInt(20)
        return ProxyRestartScheduleConfig(times, zone, warnMinutes)
    }

    private fun parseSounds(node: ConfigurationNode): Map<String, SoundCue> {
        val out = linkedMapOf<String, SoundCue>()
        for ((rawKey, child) in node.childrenMap()) {
            val key = rawKey.toString()
            val volume = child.node("volume").requireDouble().toFloat()
            require(volume <= 1.0f) {
                "sound volume must be ≤ 1.0; key '$key' is $volume"
            }
            if (volume > 0.8f) {
                warner("sound volume for key '$key' is $volume — exceeds 0.8 soft cap")
            }
            out[key] = SoundCue(
                key = child.node("key").requireString(),
                volume = volume,
                pitch = child.node("pitch").getDouble(1.0).toFloat(),
            )
        }
        return out
    }

    private fun parseRankLadder(node: ConfigurationNode): Map<String, Int> {
        val out = linkedMapOf<String, Int>()
        for ((rawKey, child) in node.childrenMap()) {
            val key = rawKey.toString()
            if (key == "default") continue
            out[key] = child.requireInt()
        }
        return out
    }

    private fun parseDrainOrder(raw: String): DrainOrder = when (raw) {
        "priority-asc" -> DrainOrder.PRIORITY_ASC
        "priority-desc" -> DrainOrder.PRIORITY_DESC
        else -> throw IllegalArgumentException("unknown drain-order: '$raw'")
    }

    private fun ConfigurationNode.requireString(): String =
        string ?: throw IllegalArgumentException("missing string at ${path().joinToString(".")}")

    private fun ConfigurationNode.requireInt(): Int = int

    private fun ConfigurationNode.requireDouble(): Double = double
}
