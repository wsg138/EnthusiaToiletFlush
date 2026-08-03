package com.badgersmc.queuerestart.velocity.infrastructure.config

import com.badgersmc.queuerestart.velocity.application.drain.DrainOrder
import com.badgersmc.queuerestart.velocity.application.ports.AccessMessagesConfig
import com.badgersmc.queuerestart.velocity.application.ports.ConfigPort
import com.badgersmc.queuerestart.velocity.application.ports.ConfiguredRestartSchedule
import com.badgersmc.queuerestart.velocity.application.ports.CountdownConfig
import com.badgersmc.queuerestart.velocity.application.ports.DrainConfig
import com.badgersmc.queuerestart.velocity.application.ports.QueueRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.NetworkRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.RejoinConfig
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.spongepowered.configurate.ConfigurationNode
import org.spongepowered.configurate.yaml.YamlConfigurationLoader
import java.nio.file.Path

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

    @Volatile private var current: QueueRestartConfig = parse()

    override fun snapshot(): QueueRestartConfig = current

    override fun reload() {
        current = parse()
    }

    private fun parse(): QueueRestartConfig {
        val loader = YamlConfigurationLoader.builder().path(path).build()
        val root = loader.load()

        val networkRestart = parseNetworkRestart(root.node("network-restart"))
        return QueueRestartConfig(
            hubServer = ServerId(root.node("hub-server").requireString()),
            fallbackHubs = root.node("fallback-hubs").childrenList()
                .map { ServerId(it.requireString()) },
            drain = parseDrain(root.node("drain")),
            rejoin = parseRejoin(root.node("rejoin")),
            countdown = parseCountdown(root.node("countdown")),
            accessMessages = parseAccessMessages(root.node("access-messages")),
            sounds = parseSounds(root.node("sounds")),
            rankLadder = parseRankLadder(root.node("rank-ladder")),
            rankDefault = root.node("rank-ladder", "default").getInt(0),
            networkRestart = networkRestart,
            schedules = parseSchedules(root.node("automatic-schedules")).also { validateSchedules(it, networkRestart) },
        )
    }

    private fun parseNetworkRestart(node: ConfigurationNode): NetworkRestartConfig {
        if (node.virtual()) return NetworkRestartConfig.disabled()
        val serverIds = linkedMapOf<ServerId, String>()
        for ((key, child) in node.node("servers").childrenMap()) {
            serverIds[ServerId(key.toString())] = child.requireString()
        }
        val configuredKey = node.node("api-key").string.orEmpty()
        val apiKey = expandEnvironment(configuredKey)
        return NetworkRestartConfig(
            enabled = node.node("enabled").getBoolean(false),
            timezone = node.node("timezone").getString("America/Indiana/Indianapolis"),
            executorType = node.node("executor").getString("DRY_RUN").uppercase(),
            panelUrl = node.node("panel-url").string.orEmpty().trimEnd('/'),
            apiKey = apiKey,
            proxyServerId = node.node("proxy-server-id").string.orEmpty(),
            serverIds = serverIds,
            members = node.node("full-network", "members").childrenList().map { ServerId(it.requireString()) },
            hubServers = node.node("full-network", "hub-servers").childrenList().map { ServerId(it.requireString()) },
            announcementPointsSeconds = node.node("announcement-points-seconds").childrenList().map { it.long },
            finalCountdownSeconds = node.node("final-countdown-seconds").getInt(5),
            transferTimeoutSeconds = node.node("transfer-timeout-seconds").getLong(10),
            backendHeadStartSeconds = node.node("backend-head-start-seconds").getLong(3),
            maintenanceFailureExpirySeconds = node.node("maintenance-failure-expiry-seconds").getLong(60),
            connectTimeoutSeconds = node.node("connect-timeout-seconds").getLong(5),
            requestTimeoutSeconds = node.node("request-timeout-seconds").getLong(10),
            maximumRetries = node.node("maximum-retries").getInt(2),
            maxConcurrentActions = node.node("max-concurrent-actions").getInt(4),
            allowInsecureHttp = node.node("allow-insecure-http").getBoolean(false),
        ).also(::validateNetworkRestart)
    }

    private fun parseSchedules(node: ConfigurationNode): List<ConfiguredRestartSchedule> =
        node.childrenMap().map { (rawName, child) ->
            ConfiguredRestartSchedule(
                name = rawName.toString(),
                type = child.node("type").getString("SERVER").uppercase(),
                targets = child.node("targets").childrenList().map { ServerId(it.requireString()) },
                time = child.node("time").requireString(),
                days = child.node("days").childrenList().map { it.requireString().uppercase() }.toSet(),
                warningWindowSeconds = parseDurationSeconds(child.node("warning-window").getString("2h")),
                timezone = child.node("timezone").getString("America/Indiana/Indianapolis"),
                reason = child.node("reason").getString(""),
                silent = child.node("silent").getBoolean(false),
                enabled = child.node("enabled").getBoolean(true),
            )
        }

    private fun validateNetworkRestart(cfg: NetworkRestartConfig) {
        if (!cfg.enabled) return
        runCatching { java.time.ZoneId.of(cfg.timezone) }
            .getOrElse { throw IllegalArgumentException("invalid network-restart.timezone '${cfg.timezone}'") }
        require(cfg.executorType in setOf("PTERODACTYL", "DRY_RUN")) { "network-restart.executor must be PTERODACTYL or DRY_RUN" }
        require(cfg.announcementPointsSeconds.isNotEmpty() && cfg.announcementPointsSeconds.all { it > 0 }) { "announcement points must be positive" }
        require(cfg.finalCountdownSeconds > 0) { "final-countdown-seconds must be positive" }
        require(cfg.transferTimeoutSeconds > 0) { "transfer-timeout-seconds must be positive" }
        require(cfg.backendHeadStartSeconds >= 0) { "backend-head-start-seconds must not be negative" }
        require(cfg.maintenanceFailureExpirySeconds > 0) { "maintenance-failure-expiry-seconds must be positive" }
        require(cfg.connectTimeoutSeconds > 0) { "connect-timeout-seconds must be positive" }
        require(cfg.requestTimeoutSeconds > 0) { "request-timeout-seconds must be positive" }
        require(cfg.maximumRetries in 0..5) { "maximum-retries must be between 0 and 5" }
        if (cfg.executorType == "PTERODACTYL") {
            val uri = java.net.URI.create(cfg.panelUrl)
            require(uri.isAbsolute && !uri.host.isNullOrBlank() && uri.userInfo == null && uri.query == null && uri.fragment == null) {
                "Pterodactyl panel URL must be an absolute origin URL"
            }
            val scheme = uri.scheme.lowercase()
            require(scheme == "https" || (scheme == "http" && cfg.allowInsecureHttp)) { "Pterodactyl panel URL must use HTTPS" }
            require(cfg.apiKey.isNotBlank()) { "PTERODACTYL_API_KEY is not available" }
            require(cfg.proxyServerId.matches(Regex("[A-Za-z0-9_-]{4,64}"))) { "invalid proxy server identifier" }
        }
        require(cfg.members.isNotEmpty()) { "full-network.members must not be empty" }
        require(cfg.members.all(cfg.serverIds::containsKey)) { "every full-network member requires a panel identifier" }
        require(cfg.serverIds.values.all { it.matches(Regex("[A-Za-z0-9_-]{4,64}")) }) { "invalid backend panel server identifier" }
        require(cfg.proxyServerId.matches(Regex("[A-Za-z0-9_-]{4,64}"))) { "invalid proxy server identifier" }
        require(cfg.serverIds.values.filter(String::isNotBlank).distinct().size == cfg.serverIds.values.count(String::isNotBlank)) { "duplicate panel server identifier" }
        require(cfg.proxyServerId !in cfg.serverIds.values) { "proxy server identifier must not match a backend identifier" }
        require(cfg.hubServers.isNotEmpty()) { "at least one full-network hub server is required" }
        require(cfg.hubServers.all(cfg.members::contains)) { "hub servers must be full-network members" }
        require(cfg.maxConcurrentActions in 1..32) { "max-concurrent-actions must be 1..32" }
    }

    private fun validateSchedules(schedules: List<ConfiguredRestartSchedule>, network: NetworkRestartConfig) {
        schedules.filter(ConfiguredRestartSchedule::enabled).forEach { schedule ->
            require(schedule.type in setOf("SERVER", "PROXY", "NETWORK")) { "schedule '${schedule.name}' has an invalid type" }
            runCatching { java.time.LocalTime.parse(schedule.time) }
                .getOrElse { throw IllegalArgumentException("schedule '${schedule.name}' has an invalid time") }
            runCatching { java.time.ZoneId.of(schedule.timezone) }
                .getOrElse { throw IllegalArgumentException("schedule '${schedule.name}' has an invalid timezone") }
            require(schedule.warningWindowSeconds > 0) { "schedule '${schedule.name}' warning-window must be positive" }
            require(schedule.days.all { it in java.time.DayOfWeek.entries.map(java.time.DayOfWeek::name) }) {
                "schedule '${schedule.name}' contains an invalid day"
            }
            when (schedule.type) {
                "SERVER" -> require(schedule.targets.size == 1 && network.serverIds.containsKey(schedule.targets.single())) {
                    "schedule '${schedule.name}' requires one configured backend target"
                }
                "PROXY" -> require(schedule.targets.isEmpty()) { "schedule '${schedule.name}' must not define backend targets" }
                "NETWORK" -> require(schedule.targets.isEmpty()) { "schedule '${schedule.name}' must not define backend targets" }
            }
        }
    }

    private fun expandEnvironment(value: String): String {
        val match = Regex("^\\$\\{([A-Z0-9_]+)}$").matchEntire(value) ?: return value
        return System.getenv(match.groupValues[1]).orEmpty()
    }

    private fun parseDurationSeconds(value: String): Long {
        val match = Regex("^(\\d+)([hms])$").matchEntire(value.trim().lowercase())
            ?: throw IllegalArgumentException("invalid duration '$value'")
        val amount = match.groupValues[1].toLong()
        return amount * when (match.groupValues[2]) { "h" -> 3600; "m" -> 60; else -> 1 }
    }

    private fun parseDrain(node: ConfigurationNode): DrainConfig {
        val legacyLead = node.node("drain-lead-seconds").getInt(0)
        if (legacyLead != 0) {
            warner(
                "drain.drain-lead-seconds=$legacyLead is deprecated and ignored; " +
                    "player draining now begins at T-0",
            )
        }
        return DrainConfig(
            batchSize = node.node("batch-size").requireInt(),
            batchIntervalTicks = node.node("batch-interval-ticks").requireInt(),
            drainLeadSeconds = 0,
            forceDrainTimeoutSeconds = node.node("force-drain-timeout-seconds").requireInt(),
            drainOrder = parseDrainOrder(node.node("drain-order").requireString()),
        )
    }

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


    private fun parseAccessMessages(node: ConfigurationNode): AccessMessagesConfig {
        val defaults = AccessMessagesConfig.defaults()
        if (node.virtual()) return defaults
        return AccessMessagesConfig(
            backendRestarting = node.node("backend-restarting").getString(defaults.backendRestarting),
            backendWhitelisted = node.node("backend-whitelisted").getString(defaults.backendWhitelisted),
            drainDisconnect = node.node("drain-disconnect").getString(defaults.drainDisconnect),
            networkMaintenance = node.node("network-maintenance").getString(defaults.networkMaintenance),
        )
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
