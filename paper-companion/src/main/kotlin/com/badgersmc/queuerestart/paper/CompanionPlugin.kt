package com.badgersmc.queuerestart.paper

import com.badgersmc.queuerestart.common.schedule.AuthenticatedPollProtocol
import com.badgersmc.queuerestart.common.security.AuthenticatedMessageCodec
import org.bukkit.event.EventPriority
import org.bukkit.event.Listener
import org.bukkit.plugin.EventExecutor
import org.bukkit.plugin.java.JavaPlugin
import java.util.UUID
import java.util.logging.Level

/**
 * Paper companion entrypoint. On `onEnable`:
 *  1. Register incoming + outgoing `qrestart:v1` plugin-message channels.
 *  2. Wire [ProxyMessageListener] over a [BukkitServerControl]-backed
 *     [RestartExecutor].
 *  3. Soft-depend on CheckHacks (REQ-040) — when present, install a typed
 *     reflective listener for `CheckCompletedEvent` and forward results to
 *     the proxy.
 *
 * implementation.md §1, §7.
 */
class CompanionPlugin : JavaPlugin() {

    private lateinit var listener: ProxyMessageListener
    private var armPoller: ProxyArmPoller? = null
    private val bridge = CheckHacksBridge()

    override fun onEnable() {
        saveDefaultConfig()

        val serverId = resolveServerId()
        val secret = resolveControlSecret()
        val maximumClockSkew = config.getLong("maximum-clock-skew-seconds", 45)
        require(maximumClockSkew in 10..300) {
            "queue-restart: maximum-clock-skew-seconds must be 10..300"
        }
        val authenticatedCodec = AuthenticatedMessageCodec(secret, maxClockSkewSeconds = maximumClockSkew)
        val processedDeliveries = ProcessedDeliveryStore(dataFolder.toPath().resolve("processed-deliveries.state"))

        val scheduler = RestartScheduler { delaySeconds, action ->
            // Bukkit ticks at 20 Hz. delay==0 still goes through runTaskLater
            // so the action runs on the main thread regardless of caller.
            val task = server.scheduler.runTaskLater(this, Runnable { action() }, delaySeconds * 20L)
            object : ScheduledHandle { override fun cancel() = task.cancel() }
        }
        val executor = RestartExecutor(BukkitServerControl(), scheduler, processedDeliveries)
        listener = ProxyMessageListener(this, serverId, executor, authenticatedCodec)

        server.messenger.registerIncomingPluginChannel(this, ProxyMessageListener.CHANNEL, listener)
        server.messenger.registerOutgoingPluginChannel(this, ProxyMessageListener.CHANNEL)

        installCheckHacksListener()

        rejectLegacyLocalTimer()
        startArmPoller(serverId, executor, secret, maximumClockSkew)

        logger.info("queue-restart-companion enabled. checkhacks=${bridge.isCheckHacksAvailable()}")
    }

    /**
     * REQ-022. Start the inverse-SLP poller that pulls pending arms from
     * the proxy. Disabled if `server-id` is missing — the proxy keys its
     * cache by server-id and a misconfigured backend would silently drop
     * any arm. Better to log + skip than fire on the wrong target.
     */
    private fun startArmPoller(
        serverId: String,
        executor: RestartExecutor,
        secret: String,
        maximumClockSkew: Long,
    ) {
        val host = config.getString("proxy-host", "127.0.0.1").orEmpty().trim()
        require(host.isNotEmpty() && host.length <= 253 && '\u0000' !in host) {
            "queue-restart: proxy-host must be a non-empty hostname or IP address"
        }
        val port = config.getInt("proxy-port", 25565)
        require(port in 1..65535) { "queue-restart: proxy-port must be 1..65535" }
        val every = config.getInt("arm-poll-seconds", 5)
        require(every in 1..60) { "queue-restart: arm-poll-seconds must be 1..60" }
        val bootId = UUID.randomUUID()
        armPoller = ProxyArmPoller(
            plugin = this,
            proxyHost = host,
            proxyPort = port,
            serverId = serverId,
            bootId = bootId,
            executor = executor,
            protocol = AuthenticatedPollProtocol(secret, maxClockSkewSeconds = maximumClockSkew),
            pollIntervalSeconds = every,
        ).also { it.start() }
    }

    override fun onDisable() {
        armPoller?.stop()
        armPoller = null
        server.messenger.unregisterIncomingPluginChannel(this)
        server.messenger.unregisterOutgoingPluginChannel(this)
    }

    private fun rejectLegacyLocalTimer() {
        val configured = config.getStringList("restart-times")
        require(configured.isEmpty()) {
            "queue-restart: restart-times is no longer supported on Paper; " +
                "move every entry to Velocity automatic-schedules before enabling this jar"
        }
    }


    private fun resolveServerId(): String {
        val configured = config.getString("server-id").orEmpty().trim()
        require(configured.matches(Regex("[A-Za-z0-9_.-]{1,64}"))) {
            "queue-restart: server-id must match [A-Za-z0-9_.-]{1,64}"
        }
        require(!configured.contains("CHANGE_ME", ignoreCase = true)) {
            "queue-restart: server-id is still a placeholder"
        }
        return configured
    }

    private fun resolveControlSecret(): String {
        val configured = config.getString("control-secret").orEmpty()
        val envMatch = Regex("^\\$\\{([A-Z0-9_]+)}$").matchEntire(configured)
        val resolved = if (envMatch == null) configured else System.getenv(envMatch.groupValues[1]).orEmpty()
        // Constructor validation enforces minimum length and rejects placeholders.
        com.badgersmc.queuerestart.common.security.ControlAuthenticator.validateSecret(resolved)
        return resolved
    }

    /**
     * REQ-040. Install a Bukkit listener for `me.branduzzo.checkHacks.api.CheckCompletedEvent`
     * if CheckHacks is present, pulling fields reflectively (the type is not
     * on the compile classpath — see paper-companion/build.gradle.kts).
     * The fork-PR (T-101) replaces this with a typed listener.
     */
    private fun installCheckHacksListener() {
        bridge.installListenerIfAvailable { eventClass ->
            val getPlayerId = eventClass.getMethod("getPlayerId")
            val isClean = runCatching { eventClass.getMethod("isClean") }.getOrNull()
            val isDetected = runCatching { eventClass.getMethod("isDetected") }.getOrNull()
            val isProtected = runCatching { eventClass.getMethod("isProtected") }
                .getOrElse { runCatching { eventClass.getMethod("isProtected_") }.getOrNull() }

            val executor = EventExecutor { _, event ->
                if (!eventClass.isInstance(event)) return@EventExecutor
                try {
                    val pid = getPlayerId.invoke(event) as UUID
                    val clean = (isClean?.invoke(event) as? Boolean) ?: false
                    val detected = (isDetected?.invoke(event) as? Boolean) ?: false
                    val protectedFlag = (isProtected?.invoke(event) as? Boolean) ?: false
                    val msg = bridge.translate(pid, clean, detected, protectedFlag)
                    listener.sendCheckHacksResult(msg)
                } catch (t: Throwable) {
                    logger.log(Level.WARNING, "queue-restart: CheckHacks bridge failure", t)
                }
            }

            @Suppress("UNCHECKED_CAST")
            server.pluginManager.registerEvent(
                eventClass as Class<out org.bukkit.event.Event>,
                EmptyListener,
                EventPriority.MONITOR,
                executor,
                this,
            )
        }
    }

    private object EmptyListener : Listener
}
