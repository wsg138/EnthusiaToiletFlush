package com.badgersmc.queuerestart.velocity

import com.badgersmc.queuerestart.common.protocol.RestartMode
import com.badgersmc.queuerestart.common.schedule.AuthenticatedPollProtocol
import com.badgersmc.queuerestart.common.security.AuthenticatedMessageCodec
import com.badgersmc.queuerestart.common.security.ControlAuthenticator
import com.badgersmc.queuerestart.velocity.application.arm.PendingArmStore
import com.badgersmc.queuerestart.velocity.application.companion.CompanionRegistry
import com.badgersmc.queuerestart.velocity.application.drain.DrainPlanner
import com.badgersmc.queuerestart.velocity.application.drain.HubResolver
import com.badgersmc.queuerestart.velocity.application.drain.PingPoller
import com.badgersmc.queuerestart.velocity.application.drain.RejoinService
import com.badgersmc.queuerestart.velocity.application.gate.CheckGate
import com.badgersmc.queuerestart.velocity.application.ports.AudiencePort
import com.badgersmc.queuerestart.velocity.application.ports.ConfigPort
import com.badgersmc.queuerestart.velocity.application.ports.MessagingPort
import com.badgersmc.queuerestart.velocity.application.ports.ProxyPort
import com.badgersmc.queuerestart.velocity.application.ports.QueuePort
import com.badgersmc.queuerestart.velocity.application.ports.QueueRestartConfig
import com.badgersmc.queuerestart.velocity.application.schedule.BackendRestartOptions
import com.badgersmc.queuerestart.velocity.application.schedule.CoordinatorRegistry
import com.badgersmc.queuerestart.velocity.application.schedule.CountdownBroadcaster
import com.badgersmc.queuerestart.velocity.application.schedule.CountdownPresentation
import com.badgersmc.queuerestart.velocity.application.schedule.CountdownSoundPolicy
import com.badgersmc.queuerestart.velocity.application.schedule.QRestartAdminCommandHandler
import com.badgersmc.queuerestart.velocity.application.schedule.RestartOrchestrator
import com.badgersmc.queuerestart.velocity.application.schedule.SchedRestartCommandHandler
import com.badgersmc.queuerestart.velocity.application.network.NetworkRestartService
import com.badgersmc.queuerestart.velocity.domain.cohort.Cohort
import com.badgersmc.queuerestart.velocity.domain.cohort.CohortMember
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.rank.RankLadder
import com.badgersmc.queuerestart.velocity.infrastructure.audience.AdventureAudienceAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.clock.SystemClockAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.command.QRestartAdminCommand
import com.badgersmc.queuerestart.velocity.infrastructure.command.LastRestartCommand
import com.badgersmc.queuerestart.velocity.infrastructure.command.SchedRestartCommand
import com.badgersmc.queuerestart.velocity.infrastructure.command.PublicRestartStatusCommand
import com.badgersmc.queuerestart.velocity.infrastructure.config.ConfigurateConfigAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.messaging.PluginMessageAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.messaging.PluginMessageTransport
import com.badgersmc.queuerestart.velocity.infrastructure.messaging.VelocityChannelTransport
import com.badgersmc.queuerestart.velocity.infrastructure.executor.ConfiguredRestartExecutor
import com.badgersmc.queuerestart.velocity.infrastructure.persistence.AtomicRestartPlanStore
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.BackendAccessGuard
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.ProxyAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.ProxyPingArmResponder
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.QueueAdapter
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.VelocityProxyServerBackend
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.VelocityQueueManagerBackend
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.VelocityNetworkControl
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
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Velocity entrypoint. On `ProxyInitializeEvent`:
 *  1. Materialise default `config.yml` if missing.
 *  2. Build adapters, ports, and application services.
 *  3. Register channel `qrestart:v1` + plugin-message subscriber.
 *  4. Register Brigadier commands.
 *  5. Start the 1 Hz tick task feeding restart, transfer, and rejoin services.
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
        val controlSecret = cfgSnapshot().controlSecurity.secret
        ControlAuthenticator.validateSecret(controlSecret)
        val companionRegistry = CompanionRegistry()
        val proxyBootId = UUID.randomUUID()

        // Probe set must include rank-ladder nodes so VelocityProxyServerBackend
        // can surface them via permissionsOf for the rank ladder (REQ-033).
        VelocityProxyServerBackend.withRankLadder(cfgSnapshot().rankLadder.keys)

        // ── infrastructure adapters ──────────────────────────────────────
        val clock = SystemClockAdapter()
        val freshCompanionIdentity: (ServerId) -> UUID? = { target ->
            companionRegistry.compatibleHeartbeat(
                target,
                clock.now(),
                Duration.ofSeconds(cfgSnapshot().controlSecurity.heartbeatTimeoutSeconds),
            )?.bootId
        }
        val audience: AudiencePort = AdventureAudienceAdapter(proxy)
        val proxyBackend = VelocityProxyServerBackend(proxy, logger)
        val proxyPort: ProxyPort = ProxyAdapter(proxyBackend)
        val hubResolver = HubResolver(proxyPort)
        val queueBackend = VelocityQueueManagerBackend(proxy, logger)
        val queuePort: QueuePort = QueueAdapter(queueBackend)
        val networkControl = VelocityNetworkControl(
            proxy = proxy,
            accessMessages = { cfgSnapshot().accessMessages },
        )
        proxy.eventManager.register(this, networkControl)
        // Break the adapter↔transport construction cycle with a one-shot
        // forwarding indirection: adapter sends through a lambda that resolves
        // to the real transport once it's built.
        lateinit var channelTransport: VelocityChannelTransport
        val forwardingTransport = PluginMessageTransport { target, payload ->
            channelTransport.send(target, payload)
        }
        val pluginMessageAdapter = PluginMessageAdapter(
            forwardingTransport,
            AuthenticatedMessageCodec(
                controlSecret,
                maxClockSkewSeconds = cfgSnapshot().controlSecurity.maximumClockSkewSeconds,
            ),
        )
        channelTransport = VelocityChannelTransport(proxy, pluginMessageAdapter)
        val messaging: MessagingPort = pluginMessageAdapter
        proxy.channelRegistrar.register(channelTransport.channelIdentifier)
        proxy.eventManager.register(this, channelTransport)

        // ── domain wiring ────────────────────────────────────────────────
        val rankLadder = RankLadder(cfgSnapshot().rankLadder, cfgSnapshot().rankDefault)
        val coordinatorRegistry = CoordinatorRegistry()
        lateinit var networkService: NetworkRestartService
        val networkServiceReady = AtomicBoolean(false)
        proxy.eventManager.register(
            this,
            BackendAccessGuard(
                proxy,
                coordinatorRegistry,
                cfgSnapshot,
                hubResolver,
                additionalBlocked = { target ->
                    !networkServiceReady.get() || networkService.blocksBackendAccess(target)
                },
            ),
        )
        val countdownBroadcaster = CountdownBroadcaster(
            audience = audience,
            presentationSupplier = {
                val latest = cfgSnapshot()
                CountdownPresentation(
                    messageTemplate = latest.countdown.message,
                    t0Template = latest.countdown.messageT0,
                    soundResolver = { seconds -> CountdownSoundPolicy.resolve(latest.sounds, seconds.toLong()) },
                )
            },
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
        val drainPlanner = DrainPlanner()
        val checkGate = CheckGate(
            timeoutSeconds = { cfgSnapshot().rejoin.checkGateTimeoutSeconds },
            releaseOnTimeout = { cfgSnapshot().rejoin.releaseOnTimeout },
        )
        val rejoinService = RejoinService(
            proxyPort,
            queuePort,
            { RankLadder(cfgSnapshot().rankLadder, cfgSnapshot().rankDefault) },
            checkGate,
        )

        // REQ-022. Pending arms are durably persisted and pulled by the
        // companion's authenticated ProxyArmPoller. The delivery id and exact
        // target boot id survive a Velocity restart, so a committed shutdown is
        // neither silently lost nor replayed against a replacement JVM.
        val pendingArmStore = PendingArmStore(
            persistencePath = dataDirectory.resolve("pending-control.state"),
        )
        val backendOptions = BackendRestartOptions()

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
            options = backendOptions,
            companionIdentity = freshCompanionIdentity,
            onRestartDispatchCommitted = { target, baseline, now ->
                if (networkServiceReady.get()) {
                    networkService.commitBackendHandoffDispatch(target, baseline, now)
                } else {
                    false
                }
            },
        )
        orchestrator.start()

        val pollProtocol = AuthenticatedPollProtocol(
            controlSecret,
            maxClockSkewSeconds = cfgSnapshot().controlSecurity.maximumClockSkewSeconds,
        )
        proxy.eventManager.register(
            this,
            ProxyPingArmResponder(
                pendingArmStore,
                clock,
                logger,
                pollProtocol,
                companionRegistry,
                allowedServers = { cfgSnapshot().networkRestart.serverIds.keys },
            ),
        )

        val schedRestartHandler = SchedRestartCommandHandler(
            registry = coordinatorRegistry,
            hubServer = { cfgSnapshot().hubServer },
            companionPresent = { target ->
                companionRegistry.isCompatible(
                    target,
                    clock.now(),
                    Duration.ofSeconds(cfgSnapshot().controlSecurity.heartbeatTimeoutSeconds),
                )
            },
            cohortFor = { target -> cohortFromCurrentRoster(target, proxyPort) },
            options = backendOptions,
            cancelCoordinator = orchestrator::cancel,
        )
        val networkExecutor = ConfiguredRestartExecutor { cfgSnapshot().networkRestart }
        networkService = NetworkRestartService(
            config = { cfgSnapshot().networkRestart },
            schedules = { cfgSnapshot().schedules },
            executor = networkExecutor,
            control = networkControl,
            store = AtomicRestartPlanStore(dataDirectory.resolve("network-restarts.state")) { logger.warn("queue-restart: {}", it) },
            backendArm = { target, seconds, silent -> schedRestartHandler.armSeconds(target, seconds, silent) },
            backendCancel = { target -> orchestrator.cancel(target) },
            audit = { plan, event -> logger.info("network restart plan {}: {}", plan.id, event) },
            serverCancellationOwner = { target, silent -> orchestrator.cancelPlan(target, silent) },
            soundResolver = { seconds -> CountdownSoundPolicy.resolve(cfgSnapshot().sounds, seconds) },
            backendIdentity = freshCompanionIdentity,
            prepareBackendHandoff = orchestrator::prepareRestartHandoff,
            currentProxyBootId = proxyBootId,
            executionTimeout = { Duration.ofSeconds(cfgSnapshot().controlSecurity.backendExecutionTimeoutSeconds) },
            handoffRetryDelay = { Duration.ofSeconds(cfgSnapshot().controlSecurity.heartbeatTimeoutSeconds) },
            serverReviewResolver = { target -> orchestrator.resolveAfterManualReview(target) },
        )
        networkServiceReady.set(true)

        val pingPoller = PingPoller(
            registry = coordinatorRegistry,
            companions = companionRegistry,
            onReady = orchestrator::finishRejoin,
            heartbeatTimeout = { Duration.ofSeconds(cfgSnapshot().controlSecurity.heartbeatTimeoutSeconds) },
            executionTimeout = { Duration.ofSeconds(cfgSnapshot().controlSecurity.backendExecutionTimeoutSeconds) },
            onTimeout = { target, reason ->
                logger.error("queue-restart: restart observation timed out for {}: {}", target.value, reason)
                networkService.markBackendNeedsReview(target, reason)
            },
        )
        val adminHandler = QRestartAdminCommandHandler(
            config = config,
            triggerSchedule = { name -> runCatching { networkService.triggerConfiguredSchedule(name) != null }.getOrDefault(false) },
            resolveReview = networkService::resolveReview,
            onReload = {
                // REQ-090 (#6). Refresh the permission probe set so the
                // rank-ladder additions in the freshly parsed config
                // become resolvable via permissionsOf.
                VelocityProxyServerBackend.withRankLadder(cfgSnapshot().rankLadder.keys)
            },
        )

        // ── commands ─────────────────────────────────────────────────────
        registerCommand(SchedRestartCommand.LITERAL, SchedRestartCommand(schedRestartHandler, networkService, cfgSnapshot).build())
        registerCommand(QRestartAdminCommand.LITERAL, QRestartAdminCommand(adminHandler).build())
        registerSimpleCommand("nextrestart", PublicRestartStatusCommand(networkService, cfgSnapshot, false))
        registerSimpleCommand("restartschedule", PublicRestartStatusCommand(networkService, cfgSnapshot, true))
        registerSimpleCommand("lastrestart", LastRestartCommand(networkService, cfgSnapshot))

        // ── 1 Hz proxy tick ──────────────────────────────────────────────
        proxy.scheduler.buildTask(this, Runnable {
            val now = clock.now()
            try {
                // Durable plan authority prepares and persists T-0 state before
                // the orchestrator is allowed to publish a destructive delivery.
                networkService.tick(now)
                orchestrator.tick(now)
                pingPoller.tick(now)
                rejoinService.tick(now.epochSecond)
            } catch (t: Throwable) {
                logger.warn("queue-restart tick error", t)
            }
        }).repeat(Duration.ofSeconds(1)).schedule()

        logger.info(
            "queue-restart ready. hub={}, channel={} (recurring schedules are configured on Velocity)",
            cfgSnapshot().hubServer.value,
            VelocityChannelTransport.CHANNEL,
        )
    }


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

    private fun registerSimpleCommand(literal: String, command: com.velocitypowered.api.command.SimpleCommand) {
        val meta = proxy.commandManager.metaBuilder(literal).plugin(this).build()
        proxy.commandManager.register(meta, command)
    }

}
