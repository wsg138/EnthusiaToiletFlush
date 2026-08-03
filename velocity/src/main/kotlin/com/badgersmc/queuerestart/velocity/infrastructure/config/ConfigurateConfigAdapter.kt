package com.badgersmc.queuerestart.velocity.infrastructure.config

import com.badgersmc.queuerestart.common.security.ControlAuthenticator
import com.badgersmc.queuerestart.velocity.application.drain.DrainOrder
import com.badgersmc.queuerestart.velocity.application.ports.AccessMessagesConfig
import com.badgersmc.queuerestart.velocity.application.ports.ConfigPort
import com.badgersmc.queuerestart.velocity.application.ports.ConfiguredRestartSchedule
import com.badgersmc.queuerestart.velocity.application.ports.ControlSecurityConfig
import com.badgersmc.queuerestart.velocity.application.ports.CountdownConfig
import com.badgersmc.queuerestart.velocity.application.ports.DrainConfig
import com.badgersmc.queuerestart.velocity.application.ports.NetworkRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.QueueRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.RejoinConfig
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.spongepowered.configurate.ConfigurationNode
import org.spongepowered.configurate.yaml.YamlConfigurationLoader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

/** Transactional Configurate-YAML adapter with strict release-safety validation. */
class ConfigurateConfigAdapter(
    private val path: Path,
    private val warner: (String) -> Unit,
) : ConfigPort {
    @Volatile private var current: QueueRestartConfig = parse()

    override fun snapshot(): QueueRestartConfig = current

    override fun reload() {
        val updated = parse()
        require(updated.controlSecurity.secret == current.controlSecurity.secret) {
            "control-security.secret is startup-bound; restart Velocity to rotate it"
        }
        require(updated.controlSecurity.maximumClockSkewSeconds == current.controlSecurity.maximumClockSkewSeconds) {
            "control-security.maximum-clock-skew-seconds is startup-bound; restart Velocity to change it"
        }
        current = updated
    }

    private fun parse(): QueueRestartConfig {
        validateRawConfiguration()
        val root = YamlConfigurationLoader.builder().path(path).build().load()
        val networkRestart = parseNetworkRestart(root.node("network-restart"))
        val controlSecurity = parseControlSecurity(root.node("control-security"))
        return QueueRestartConfig(
            hubServer = ServerId(root.node("hub-server").requireString()),
            fallbackHubs = root.node("fallback-hubs").childrenList().map { ServerId(it.requireString()) },
            drain = parseDrain(root.node("drain")),
            rejoin = parseRejoin(root.node("rejoin")),
            countdown = parseCountdown(root.node("countdown")),
            accessMessages = parseAccessMessages(root.node("access-messages")),
            sounds = parseSounds(root.node("sounds")),
            rankLadder = parseRankLadder(root.node("rank-ladder")),
            rankDefault = root.node("rank-ladder", "default").getInt(0),
            controlSecurity = controlSecurity,
            networkRestart = networkRestart,
            schedules = parseSchedules(root.node("automatic-schedules")).also {
                validateSchedules(it, networkRestart)
            },
        ).also(::validateRoot)
    }

    private fun validateRawConfiguration() {
        require(Files.size(path) <= MAX_CONFIG_BYTES) { "config.yml exceeds the 1 MiB safety limit" }
        val raw = Files.readString(path, StandardCharsets.UTF_8)
        require('\u0000' !in raw) { "config.yml may not contain NUL" }
    }

    private fun parseControlSecurity(node: ConfigurationNode): ControlSecurityConfig {
        val secret = expandEnvironment(node.node("secret").string.orEmpty())
        ControlAuthenticator.validateSecret(secret)
        return ControlSecurityConfig(
            secret = secret,
            heartbeatTimeoutSeconds = node.node("heartbeat-timeout-seconds").getLong(20),
            maximumClockSkewSeconds = node.node("maximum-clock-skew-seconds").getLong(45),
            backendExecutionTimeoutSeconds = node.node("backend-execution-timeout-seconds").getLong(600),
        ).also { cfg ->
            require(cfg.heartbeatTimeoutSeconds in 10..300) {
                "control-security.heartbeat-timeout-seconds must be 10..300"
            }
            require(cfg.maximumClockSkewSeconds in 10..300) {
                "control-security.maximum-clock-skew-seconds must be 10..300"
            }
            require(cfg.backendExecutionTimeoutSeconds in 60..3600) {
                "control-security.backend-execution-timeout-seconds must be 60..3600"
            }
        }
    }

    private fun parseNetworkRestart(node: ConfigurationNode): NetworkRestartConfig {
        if (node.virtual()) return NetworkRestartConfig.disabled()
        val serverIds = linkedMapOf<ServerId, String>()
        for ((key, child) in node.node("servers").childrenMap()) {
            serverIds[ServerId(key.toString())] = child.requireString()
        }
        val cfg = NetworkRestartConfig(
            enabled = node.node("enabled").getBoolean(false),
            timezone = node.node("timezone").getString("America/Indiana/Indianapolis"),
            executorType = node.node("executor").getString("DRY_RUN").uppercase(),
            panelUrl = node.node("panel-url").string.orEmpty().trimEnd('/'),
            apiKey = expandEnvironment(node.node("api-key").string.orEmpty()),
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
        )
        validateNetworkRestart(cfg)
        return cfg
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
        require(cfg.executorType in setOf("PTERODACTYL", "DRY_RUN")) {
            "network-restart.executor must be PTERODACTYL or DRY_RUN"
        }
        require(cfg.announcementPointsSeconds.isNotEmpty()) { "announcement points must not be empty" }
        require(cfg.announcementPointsSeconds.all { it in 1..86_400 }) { "announcement points must be 1..86400" }
        require(cfg.announcementPointsSeconds.distinct().size == cfg.announcementPointsSeconds.size) {
            "announcement points must not contain duplicates"
        }
        require(cfg.finalCountdownSeconds in 1..60) { "final-countdown-seconds must be 1..60" }
        require(cfg.transferTimeoutSeconds in 1..300) { "transfer-timeout-seconds must be 1..300" }
        require(cfg.backendHeadStartSeconds in 0..300) { "backend-head-start-seconds must be 0..300" }
        require(cfg.maintenanceFailureExpirySeconds in 30..3600) {
            "maintenance-failure-expiry-seconds must be 30..3600"
        }
        require(cfg.connectTimeoutSeconds in 1..60) { "connect-timeout-seconds must be 1..60" }
        require(cfg.requestTimeoutSeconds in 1..120) { "request-timeout-seconds must be 1..120" }
        require(cfg.maximumRetries in 0..5) { "maximum-retries must be between 0 and 5" }
        require(cfg.maxConcurrentActions in 1..32) { "max-concurrent-actions must be 1..32" }

        if (cfg.executorType == "PTERODACTYL") {
            val uri = java.net.URI.create(cfg.panelUrl)
            require(
                uri.isAbsolute && !uri.host.isNullOrBlank() && uri.userInfo == null &&
                    uri.query == null && uri.fragment == null && (uri.path.isNullOrBlank() || uri.path == "/"),
            ) { "Pterodactyl panel URL must be an absolute origin URL" }
            val scheme = uri.scheme.lowercase()
            require(scheme == "https" || (scheme == "http" && cfg.allowInsecureHttp)) {
                "Pterodactyl panel URL must use HTTPS"
            }
            require(cfg.apiKey.isNotBlank()) { "PTERODACTYL_API_KEY is not available" }
        }

        require(cfg.members.isNotEmpty() && cfg.members.size <= 128) { "full-network.members must contain 1..128 servers" }
        require(cfg.members.distinct().size == cfg.members.size) { "full-network.members contains duplicates" }
        require(cfg.members.all(cfg.serverIds::containsKey)) { "every full-network member requires a panel identifier" }
        require(cfg.serverIds.keys.all { it.value.matches(SERVER_ID_REGEX) }) { "invalid backend server name" }
        require(cfg.serverIds.values.all { it.matches(PANEL_ID_REGEX) }) { "invalid backend panel server identifier" }
        require(cfg.proxyServerId.matches(PANEL_ID_REGEX)) { "invalid proxy server identifier" }
        require(cfg.serverIds.values.distinct().size == cfg.serverIds.values.size) { "duplicate panel server identifier" }
        require(cfg.proxyServerId !in cfg.serverIds.values) { "proxy server identifier must not match a backend identifier" }
        require(cfg.hubServers.isNotEmpty()) { "at least one full-network hub server is required" }
        require(cfg.hubServers.distinct().size == cfg.hubServers.size) { "full-network.hub-servers contains duplicates" }
        require(cfg.hubServers.all(cfg.members::contains)) { "hub servers must be full-network members" }
    }

    private fun validateSchedules(schedules: List<ConfiguredRestartSchedule>, network: NetworkRestartConfig) {
        require(schedules.map { it.name }.distinct().size == schedules.size) { "automatic schedule names must be unique" }
        schedules.filter(ConfiguredRestartSchedule::enabled).forEach { schedule ->
            require(schedule.name.matches(Regex("[A-Za-z0-9_.-]{1,64}"))) { "invalid schedule name '${schedule.name}'" }
            require(schedule.type in setOf("SERVER", "PROXY", "NETWORK")) {
                "schedule '${schedule.name}' has an invalid type"
            }
            runCatching { java.time.LocalTime.parse(schedule.time) }
                .getOrElse { throw IllegalArgumentException("schedule '${schedule.name}' has an invalid time") }
            runCatching { java.time.ZoneId.of(schedule.timezone) }
                .getOrElse { throw IllegalArgumentException("schedule '${schedule.name}' has an invalid timezone") }
            require(schedule.warningWindowSeconds in 1..86_400) {
                "schedule '${schedule.name}' warning-window must be 1s..24h"
            }
            require(schedule.days.all { it in java.time.DayOfWeek.entries.map(java.time.DayOfWeek::name) }) {
                "schedule '${schedule.name}' contains an invalid day"
            }
            when (schedule.type) {
                "SERVER" -> require(schedule.targets.size == 1 && network.serverIds.containsKey(schedule.targets.single())) {
                    "schedule '${schedule.name}' requires one configured backend target"
                }
                "PROXY", "NETWORK" -> require(schedule.targets.isEmpty()) {
                    "schedule '${schedule.name}' must not define backend targets"
                }
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
        return Math.multiplyExact(
            amount,
            when (match.groupValues[2]) {
                "h" -> 3600L
                "m" -> 60L
                else -> 1L
            },
        )
    }

    private fun parseDrain(node: ConfigurationNode): DrainConfig {
        val legacyLead = node.node("drain-lead-seconds").getInt(0)
        if (legacyLead != 0) {
            warner("drain.drain-lead-seconds=$legacyLead is deprecated and ignored; player draining begins at T-0")
        }
        return DrainConfig(
            batchSize = node.node("batch-size").requireInt(),
            batchIntervalTicks = node.node("batch-interval-ticks").requireInt(),
            drainLeadSeconds = 0,
            forceDrainTimeoutSeconds = node.node("force-drain-timeout-seconds").requireInt(),
            drainOrder = parseDrainOrder(node.node("drain-order").requireString()),
        ).also { cfg ->
            require(cfg.batchSize in 1..500) { "drain.batch-size must be 1..500" }
            require(cfg.batchIntervalTicks in 1..1200) { "drain.batch-interval-ticks must be 1..1200" }
            require(cfg.forceDrainTimeoutSeconds in 5..900) { "drain.force-drain-timeout-seconds must be 5..900" }
        }
    }

    private fun parseRejoin(node: ConfigurationNode) = RejoinConfig(
        enabled = node.node("enabled").getBoolean(true),
        enqueueOnServerUp = node.node("enqueue-on-server-up").getBoolean(true),
        releaseOnCheckhacksCleared = node.node("release-on-checkhacks-cleared").getBoolean(true),
        checkGateTimeoutSeconds = node.node("check-gate-timeout-seconds").requireInt(),
        releaseOnTimeout = node.node("release-on-timeout").getBoolean(false),
        pingPollSeconds = node.node("ping-poll-seconds").requireInt(),
    ).also { cfg ->
        require(cfg.checkGateTimeoutSeconds in 5..600) { "rejoin.check-gate-timeout-seconds must be 5..600" }
        require(cfg.pingPollSeconds in 1..60) { "rejoin.ping-poll-seconds must be 1..60" }
    }

    private fun parseCountdown(node: ConfigurationNode) = CountdownConfig(
        marksSeconds = node.node("marks-seconds").childrenList().map { it.requireInt() },
        message = node.node("message").requireString(),
        messageT0 = node.node("message-t0").requireString(),
        cancelMessage = node.node("cancel-message").requireString(),
    ).also { cfg ->
        require(cfg.marksSeconds.isNotEmpty()) { "countdown.marks-seconds must not be empty" }
        require(cfg.marksSeconds.all { it in 1..86_400 }) { "countdown marks must be 1..86400 seconds" }
        require(cfg.marksSeconds.distinct().size == cfg.marksSeconds.size) { "countdown marks must not contain duplicates" }
    }

    private fun validateRoot(cfg: QueueRestartConfig) {
        require(cfg.fallbackHubs.size <= 16) { "fallback-hubs must contain at most 16 servers" }
        require(cfg.fallbackHubs.distinct().size == cfg.fallbackHubs.size) { "fallback-hubs must not contain duplicates" }
        require(cfg.hubServer !in cfg.fallbackHubs) { "fallback-hubs must not repeat hub-server" }
        require(cfg.rankDefault in -1_000_000..1_000_000) { "rank-ladder.default is outside the supported range" }
        val messages = listOf(
            cfg.countdown.message,
            cfg.countdown.messageT0,
            cfg.countdown.cancelMessage,
            cfg.accessMessages.backendRestarting,
            cfg.accessMessages.backendWhitelisted,
            cfg.accessMessages.drainDisconnect,
            cfg.accessMessages.networkMaintenance,
        )
        require(messages.all { it.length <= 8192 && '\u0000' !in it }) {
            "configured messages must be at most 8192 characters and may not contain NUL"
        }
    }

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
        require(node.childrenMap().size <= 64) { "sounds must contain at most 64 entries" }
        val out = linkedMapOf<String, SoundCue>()
        for ((rawKey, child) in node.childrenMap()) {
            val key = rawKey.toString()
            val volume = child.node("volume").requireDouble().toFloat()
            require(volume in 0.0f..1.0f) { "sound volume must be in [0.0, 1.0]; key '$key' is $volume" }
            if (volume > 0.8f) warner("sound volume for key '$key' is $volume — exceeds 0.8 soft cap")
            val soundKey = child.node("key").requireString()
            val pitch = child.node("pitch").getDouble(1.0).toFloat()
            require(soundKey.matches(Regex("[a-z0-9_.-]+:[a-z0-9_./-]+"))) { "invalid sound key '$soundKey'" }
            require(pitch in 0.5f..2.0f) { "sound pitch must be in [0.5, 2.0]; key '$key' is $pitch" }
            out[key] = SoundCue(soundKey, volume, pitch)
        }
        return out
    }

    private fun parseRankLadder(node: ConfigurationNode): Map<String, Int> {
        require(node.childrenMap().size <= 129) { "rank-ladder must contain at most 128 permissions plus default" }
        val out = linkedMapOf<String, Int>()
        for ((rawKey, child) in node.childrenMap()) {
            val key = rawKey.toString()
            if (key == "default") continue
            require(key.matches(Regex("[A-Za-z0-9_.+:-]{1,128}"))) { "invalid rank permission '$key'" }
            val weight = child.requireInt()
            require(weight in -1_000_000..1_000_000) { "rank weight for '$key' is outside the supported range" }
            out[key] = weight
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

    companion object {
        private const val MAX_CONFIG_BYTES = 1024L * 1024L
        private val SERVER_ID_REGEX = Regex("[A-Za-z0-9_.-]{1,64}")
        private val PANEL_ID_REGEX = Regex("[A-Za-z0-9_-]{4,64}")
    }
}
