package com.badgersmc.queuerestart.velocity.application.ports

import com.badgersmc.queuerestart.velocity.application.drain.DrainOrder
import com.badgersmc.queuerestart.velocity.domain.id.ServerId

/**
 * Outbound port — typed access to `config.yml` after parsing. Adapter:
 * `infrastructure/config/ConfigurateConfigAdapter`.
 */
interface ConfigPort {
    fun snapshot(): QueueRestartConfig
    fun reload()
}

data class QueueRestartConfig(
    val hubServer: ServerId,
    val fallbackHubs: List<ServerId>,
    val drain: DrainConfig,
    val rejoin: RejoinConfig,
    val countdown: CountdownConfig,
    val accessMessages: AccessMessagesConfig = AccessMessagesConfig.defaults(),
    val sounds: Map<String, SoundCue>,
    val rankLadder: Map<String, Int>,
    val rankDefault: Int,
    val controlSecurity: ControlSecurityConfig = ControlSecurityConfig(),
    val networkRestart: NetworkRestartConfig = NetworkRestartConfig.disabled(),
    val schedules: List<ConfiguredRestartSchedule> = emptyList(),
)


data class ControlSecurityConfig(
    val secret: String = "",
    val heartbeatTimeoutSeconds: Long = 20,
    val maximumClockSkewSeconds: Long = 45,
    val backendExecutionTimeoutSeconds: Long = 600,
)

data class NetworkRestartConfig(
    val enabled: Boolean,
    val timezone: String,
    val executorType: String,
    val panelUrl: String,
    val apiKey: String,
    val proxyServerId: String,
    val serverIds: Map<ServerId, String>,
    val members: List<ServerId>,
    val hubServers: List<ServerId>,
    val announcementPointsSeconds: List<Long>,
    val finalCountdownSeconds: Int,
    val transferTimeoutSeconds: Long,
    val backendHeadStartSeconds: Long,
    val maintenanceFailureExpirySeconds: Long,
    val connectTimeoutSeconds: Long,
    val requestTimeoutSeconds: Long,
    val maximumRetries: Int,
    val maxConcurrentActions: Int,
    val allowInsecureHttp: Boolean,
) {
    companion object {
        fun disabled() = NetworkRestartConfig(
            false, "America/Indiana/Indianapolis", "DRY_RUN", "", "", "", emptyMap(), emptyList(), emptyList(),
            listOf(7200, 3600, 1800, 900, 600, 300, 60, 30, 10), 5,
            10, 3, 60, 5, 10, 2, 4, false,
        )
    }
}

data class ConfiguredRestartSchedule(
    val name: String,
    val type: String,
    val targets: List<ServerId>,
    val time: String,
    val days: Set<String>,
    val warningWindowSeconds: Long,
    val timezone: String,
    val reason: String,
    val silent: Boolean,
    val enabled: Boolean,
)

data class DrainConfig(
    val batchSize: Int,
    val batchIntervalTicks: Int,
    val drainLeadSeconds: Int,
    val forceDrainTimeoutSeconds: Int,
    val drainOrder: DrainOrder,
)

data class AccessMessagesConfig(
    val backendRestarting: String,
    val backendWhitelisted: String,
    val drainDisconnect: String,
    val networkMaintenance: String,
) {
    companion object {
        fun defaults() = AccessMessagesConfig(
            backendRestarting = "<red><bold><server> is restarting</bold></red>\n<gray>Please try again shortly.</gray>",
            backendWhitelisted = "<yellow><bold><server> is temporarily unavailable</bold></yellow>\n<gray>The server is currently whitelisted.</gray>",
            drainDisconnect = "<red><bold><server> is restarting</bold></red>\n<gray>You were disconnected because a lobby transfer was not available.</gray>",
            networkMaintenance = "<red><bold>Network restart in progress</bold></red>\n<gray>Please reconnect shortly.</gray>",
        )
    }
}

data class RejoinConfig(
    val enabled: Boolean,
    val enqueueOnServerUp: Boolean,
    val releaseOnCheckhacksCleared: Boolean,
    val checkGateTimeoutSeconds: Int,
    val releaseOnTimeout: Boolean,
    val pingPollSeconds: Int,
)

data class CountdownConfig(
    val marksSeconds: List<Int>,
    val message: String,
    val messageT0: String,
    val cancelMessage: String,
)

data class SoundCue(val key: String, val volume: Float, val pitch: Float) {
    init {
        require(volume in 0f..1f) {
            "sound volume must be in [0.0, 1.0]; got $volume (key=$key)"
        }
    }
}
