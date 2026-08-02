package com.badgersmc.queuerestart.velocity

import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.velocity.application.arm.PendingArmStore
import com.badgersmc.queuerestart.velocity.application.drain.DrainPlanner
import com.badgersmc.queuerestart.velocity.application.drain.HubResolver
import com.badgersmc.queuerestart.velocity.application.drain.PingPoller
import com.badgersmc.queuerestart.velocity.application.drain.RejoinService
import com.badgersmc.queuerestart.velocity.application.gate.CheckGate
import com.badgersmc.queuerestart.velocity.application.ports.AudiencePort
import com.badgersmc.queuerestart.velocity.application.ports.ConfigPort
import com.badgersmc.queuerestart.velocity.application.ports.MessagingPort
import com.badgersmc.queuerestart.velocity.application.ports.ProxyPort
import com.badgersmc.queuerestart.velocity.application.ports.ProxyRestartScheduleConfig
import com.badgersmc.queuerestart.velocity.application.ports.QueuePort
import com.badgersmc.queuerestart.velocity.application.ports.QueueRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.SchedulerPort
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.application.schedule.BackendScheduleCache
import com.badgersmc.queuerestart.velocity.application.schedule.BackendScheduleSync
import com.badgersmc.queuerestart.velocity.application.schedule.CoordinatorRegistry
import com.badgersmc.queuerestart.velocity.application.schedule.CountdownBroadcaster
import com.badgersmc.queuerestart.velocity.application.schedule.ProxyRestartService
import com.badgersmc.queuerestart.velocity.application.schedule.QRestartAdminCommandHandler
import com.badgersmc.queuerestart.velocity.application.schedule.RestartOrchestrator
import com.badgersmc.queuerestart.velocity.application.schedule.SchedRestartCommandHandler
import com.badgersmc.queuerestart.velocity.application.schedule.ScheduleDefinition
import com.badgersmc.queuerestart.velocity.application.schedule.ScheduleDiscoveryPoller
import com.badgersmc.queuerestart.velocity.application.schedule.ScheduleService
import com.badgersmc.queuerestart.velocity.domain.cohort.Cohort
import com.badgersmc.queuerestart.velocity.domain.cohort.CohortMember
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.rank.RankLadder
import com.badgersmc.queuerestart.velocity.infrastructure.audience.AdventureAudienceAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.clock.SystemClockAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.command.QRestartAdminCommand
import com.badgersmc.queuerestart.velocity.infrastructure.command.SchedRestartCommand
import com.badgersmc.queuerestart.velocity.infrastructure.config.ConfigurateConfigAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.messaging.PluginMessageAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.messaging.PluginMessageTransport
import com.badgersmc.queuerestart.velocity.infrastructure.messaging.VelocityChannelTransport
import com.badgersmc.queuerestart.velocity.infrastructure.schedule.CronUtilsScheduler
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.ProxyAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.ProxyPingArmResponder
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.QueueAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.VelocityProxyLifecycleAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.VelocityProxyServerBackend
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.VelocityQueueManagerBackend
import com.google.inject.Inject
import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent
import com.velocitypowered.api.plugin.Plugin
import com.velocitypowered.api.plugin.annotation.DataDirectory
import com.velocitypowered.api.proxy.ProxyServer
import org.slf4j.Logger
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Duration

/**
 * Velocity entrypoint. On `ProxyInitializeEvent`:
 *  1. Materialise default `config.yml` if missing.
 *  2. Build adapters, ports, and application services.
 *  3. Register channel `qrestart:v1` + plugin-message subscriber.
 *  4. Register Brigadier commands.
 *  5. Start the 1 Hz tick task feeding cron, orchestrator, ping poller.
 *
 * implementation.md §1, §4.
 */
@Plugin(
    id = "queue-restart",
    name = "EnthusiaToiletFlush",
    version = "0.1.0-SNAPSHOT",
    description = "Graceful scheduled restarts with drain + rejoin queue.",
    authors = ["BadgersMC"],
)
class QueueRestartPlugin @Inject constructor(
    private val proxy: ProxyServer,
    private val logger: Logger,
    @DataDirectory private val dataDirectory: Path,
) {

    @Subscribe
    fun onInit(@Suppress("UNUSED_PARAMETER") event: ProxyInitializeEvent) {
        logger.info("queue-restart 0.1.0-SNAPSHOT initialising...")

        val configPath = ensureDefaultConfig()
        val config: ConfigPort = ConfigurateConfigAdapter(configPath) { warning ->
            logger.warn("config: {}", warning)
        }
        val cfgSnapshot: () -> QueueRestartConfig = { config.snapshot() }

        // Probe set must include rank-ladder nodes so VelocityProxyServerBackend
        // can surface them via permissionsOf for the rank ladder (REQ-033).
        VelocityProxyServerBackend.withRankLadder(cfgSnapshot().rankLadder.keys)

        // ── infrastructure adapters ──────────────────────────────────────
        val clock = SystemClockAdapter()
        val audience: AudiencePort = AdventureAudienceAdapter(proxy)
        val proxyLifecycle = VelocityProxyLifecycleAdapter(proxy, logger)
        val proxyBackend = VelocityProxyServerBackend(proxy, logger)
        val proxyPort: ProxyPort = ProxyAdapter(proxyBackend)
        val queueBackend = VelocityQueueManagerBackend(proxy, logger)
        val queuePort: QueuePort = QueueAdapter(queueBackend)
        // Break the adapter↔transport construction cycle with a one-shot
        // forwarding indirection: adapter sends through a lambda that resolves
        // to the real transport once it's built.
        lateinit var channelTransport: VelocityChannelTransport
        val forwardingTransport = PluginMessageTransport { target, payload ->
            channelTransport.send(target, payload)
        }
        val pluginMessageAdapter = PluginMessageAdapter(forwardingTransport)
        channelTransport = VelocityChannelTransport(proxy, pluginMessageAdapter)
        val messaging: MessagingPort = pluginMessageAdapter
        proxy.channelRegistrar.register(channelTransport.channelIdentifier)
        proxy.eventManager.register(this, channelTransport)

        val cronScheduler = CronUtilsScheduler()
        val schedulerPort: SchedulerPort = cronScheduler

        // ── domain wiring ────────────────────────────────────────────────
        val rankLadder = RankLadder(cfgSnapshot().rankLadder, cfgSnapshot().rankDefault)
        val coordinatorRegistry = CoordinatorRegistry()
        val countdownBroadcaster = CountdownBroadcaster(
            audience = audience,
            messageTemplate = cfgSnapshot().countdown.message,
            t0Template = cfgSnapshot().countdown.messageT0,
            soundResolver = ::resolveSound.partial(cfgSnapshot()),
            onMark = { target, secondsRemaining, isT0 ->
                if (isT0) {
                    logger.info("queue-restart: {} T-0 reached — sending players to hub", target.value)
                } else {
                    logger.info(
                        "queue-restart: {} countdown mark fired (T-{}s)",
                        target.value, secondsRemaining,
                    )
                }
            },
        )
        val proxyRestart = ProxyRestartService(
            lifecycle = proxyLifecycle,
            marksSupplier = { cfgSnapshot().countdown.marksSeconds },
            warningMessageSupplier = { cfgSnapshot().countdown.message },
            hubSupplier = { cfgSnapshot().hubServer },
            soundResolver = { secondsRemaining -> resolveSound(cfgSnapshot(), secondsRemaining) },
            onMark = { secondsRemaining, isT0 ->
                if (isT0) {
                    logger.info("queue-restart: proxy T-0 reached — shutting down Velocity")
                } else {
                    logger.info("queue-restart: proxy countdown mark fired (T-{}s)", secondsRemaining)
                }
            },
        )
        val drainPlanner = DrainPlanner()
        val hubResolver = HubResolver(proxyPort)
        val checkGate = CheckGate(
            timeoutSeconds = cfgSnapshot().rejoin.checkGateTimeoutSeconds,
            releaseOnTimeout = cfgSnapshot().rejoin.releaseOnTimeout,
        )
        val rejoinService = RejoinService(proxyPort, queuePort, rankLadder, checkGate)

        // REQ-022. Pending arms published here are pulled by the
        // companion's ProxyArmPoller via the ProxyPingArmResponder below.
        val pendingArmStore = PendingArmStore()

        val orchestrator = RestartOrchestrator(
            registry = coordinatorRegistry,
            proxy = proxyPort,
            messaging = messaging,
            audience = audience,
            broadcaster = countdownBroadcaster,
            planner = drainPlanner,
            hubResolver = hubResolver,
            rejoin = rejoinService,
            gate = checkGate,
            rankLadder = rankLadder,
            configSupplier = cfgSnapshot,
            restartMode = RestartMode.SHUTDOWN,
            restartArg = "",
            pendingArmStore = pendingArmStore,
        )
        orchestrator.start()

        proxy.eventManager.register(this, ProxyPingArmResponder(pendingArmStore, clock, logger))

        val pingPoller = PingPoller(
            registry = coordinatorRegistry,
            proxy = proxyPort,
            onReady = orchestrator::finishRejoin,
            pingPollSeconds = cfgSnapshot().rejoin.pingPollSeconds,
        )

        val schedRestartHandler = SchedRestartCommandHandler(
            registry = coordinatorRegistry,
            hubServer = cfgSnapshot().hubServer,
            companionPresent = ::companionPresentFor,
            cohortFor = { target -> cohortFromCurrentRoster(target, proxyPort) },
            proxyRestart = proxyRestart,
        )
        val scheduleService = ScheduleService(
            scheduler = schedulerPort,
            onTrigger = { def -> schedRestartHandler.arm(def.target, def.warnMinutes) },
        )
        // Backend schedules are discovered from each companion's SLP sample.
        // Proxy schedules are local because the Velocity process has no Paper
        // companion to advertise them.
        val scheduleCache = BackendScheduleCache()
        val backendScheduleSync = BackendScheduleSync(
            cache = scheduleCache,
            scheduleService = scheduleService,
            additionalDefinitions = { proxyScheduleDefinitions(cfgSnapshot().proxyRestart) },
        )
        backendScheduleSync.start()
        val scheduleDiscoveryPoller = ScheduleDiscoveryPoller(proxyPort, scheduleCache)

        val adminHandler = QRestartAdminCommandHandler(
            config = config,
            scheduleService = scheduleService,
            schedRestartHandler = schedRestartHandler,
            onReload = {
                // REQ-090 (#6). Refresh the permission probe set so the
                // rank-ladder additions in the freshly parsed config
                // become resolvable via permissionsOf.
                VelocityProxyServerBackend.withRankLadder(cfgSnapshot().rankLadder.keys)
                // Proxy-owned restart times live in this config, unlike the
                // backend schedules discovered independently through SLP.
                backendScheduleSync.refresh()
            },
        )

        // ── commands ─────────────────────────────────────────────────────
        registerCommand(SchedRestartCommand.LITERAL, SchedRestartCommand(schedRestartHandler).build())
        registerCommand(QRestartAdminCommand.LITERAL, QRestartAdminCommand(adminHandler).build())

        // ── 1 Hz proxy tick ──────────────────────────────────────────────
        proxy.scheduler.buildTask(this, Runnable {
            val now = clock.now()
            try {
                cronScheduler.tick(now)
                orchestrator.tick(now)
                proxyRestart.tick(now)
                pingPoller.tick(now)
                scheduleDiscoveryPoller.tick(now)
                rejoinService.tick(now.epochSecond)
            } catch (t: Throwable) {
                logger.warn("queue-restart tick error", t)
            }
        }).repeat(Duration.ofSeconds(1)).schedule()

        logger.info(
            "queue-restart ready. hub={}, channel={}, proxy-target={}, proxy-schedules={} (backend schedules learned via SLP)",
            cfgSnapshot().hubServer.value,
            VelocityChannelTransport.CHANNEL,
            ProxyRestartService.TARGET.value,
            cfgSnapshot().proxyRestart.restartTimes.size,
        )
    }

    /** REQ-062. Best-effort companion presence: target reachable. A future
     *  enhancement could rely on a per-server hello frame on the channel. */
    private fun companionPresentFor(target: ServerId): Boolean =
        proxy.getServer(target.value).isPresent

    /** REQ-030. Snapshot the live roster of [target] into a [Cohort]. */
    private fun cohortFromCurrentRoster(target: ServerId, proxyPort: ProxyPort): Cohort =
        Cohort(proxyPort.playersOn(target).map(::CohortMember).toSet())

    private fun ensureDefaultConfig(): Path {
        Files.createDirectories(dataDirectory)
        val target = dataDirectory.resolve("config.yml")
        if (Files.notExists(target)) {
            javaClass.classLoader.getResourceAsStream("config.yml").use { stream ->
                if (stream == null) {
                    logger.warn("default config.yml not found in jar resources; emitting empty file")
                    Files.createFile(target)
                } else {
                    Files.copy(stream, target, StandardCopyOption.REPLACE_EXISTING)
                    logger.info("materialised default config.yml at {}", target)
                }
            }
        }
        return target
    }

    private fun registerCommand(literal: String, brigadier: com.velocitypowered.api.command.BrigadierCommand) {
        val mgr = proxy.commandManager
        val meta = mgr.metaBuilder(literal).plugin(this).build()
        mgr.register(meta, brigadier)
    }

    /** Maps configured proxy restart times to countdown-start cron entries. */
    private fun proxyScheduleDefinitions(cfg: ProxyRestartScheduleConfig): List<ScheduleDefinition> =
        cfg.restartTimes.map { restartAt ->
            val countdownAt = restartAt.minusMinutes(cfg.warnMinutes.toLong())
            ScheduleDefinition(
                name = "proxy-%02d:%02d".format(restartAt.hour, restartAt.minute),
                target = ProxyRestartService.TARGET,
                cronExpression = "${countdownAt.minute} ${countdownAt.hour} * * *",
                warnMinutes = cfg.warnMinutes,
                zone = cfg.zone,
            )
        }

    /** Maps `secondsRemaining` to the named [SoundCue] keyed in `config.yml`. */
    private fun resolveSound(cfg: QueueRestartConfig, secondsRemaining: Int): SoundCue? {
        val key = when (secondsRemaining) {
            0 -> "t0"
            in 1..10 -> "tick"
            30 -> "30s"
            60 -> "1m"
            120 -> "2m"
            300 -> "5m"
            600 -> "10m"
            1200 -> "20m"
            else -> return null
        }
        return cfg.sounds[key]
    }

    /** Bind the first arg of [resolveSound] for the broadcaster's expected `(Int) -> SoundCue?`. */
    private fun ((QueueRestartConfig, Int) -> SoundCue?).partial(cfg: QueueRestartConfig): (Int) -> SoundCue? =
        { s -> this(cfg, s) }
}
