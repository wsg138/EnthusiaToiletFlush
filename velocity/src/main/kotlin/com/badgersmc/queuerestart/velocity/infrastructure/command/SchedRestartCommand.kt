package com.badgersmc.queuerestart.velocity.infrastructure.command

import com.badgersmc.queuerestart.velocity.application.schedule.SchedCommandResult
import com.badgersmc.queuerestart.velocity.application.schedule.SchedRestartCommandHandler
import com.badgersmc.queuerestart.velocity.application.network.NetworkRestartService
import com.badgersmc.queuerestart.velocity.application.ports.QueueRestartConfig
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.plan.PlanType
import com.badgersmc.queuerestart.velocity.domain.plan.RestartTimes
import com.mojang.brigadier.arguments.IntegerArgumentType
import com.mojang.brigadier.arguments.StringArgumentType
import com.mojang.brigadier.context.CommandContext
import com.mojang.brigadier.tree.LiteralCommandNode
import com.velocitypowered.api.command.BrigadierCommand
import com.velocitypowered.api.command.CommandSource
import com.velocitypowered.api.proxy.Player
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

/**
 * Brigadier shim for `/schedrestart`. Parses the arg tree, gates on
 * `queuerestart.command.schedrestart`, forwards to
 * [SchedRestartCommandHandler], renders the [SchedCommandResult] back to
 * the source via Adventure.
 *
 * Tree:
 * ```
 *  /schedrestart cancel [server]
 *  /schedrestart status
 *  /schedrestart <minutes>
 *  /schedrestart <minutes> <server>
 * ```
 *
 * Default-server resolution: when no `[server]` arg is supplied, the
 * executor's current backend is used (Player). The console MUST supply
 * an explicit server.
 */
class SchedRestartCommand(
    private val handler: SchedRestartCommandHandler,
    private val network: NetworkRestartService? = null,
    private val config: (() -> QueueRestartConfig)? = null,
) {
    companion object {
        const val PERMISSION = "queuerestart.command.schedrestart"
        const val LITERAL = "schedrestart"
    }

    fun build(): BrigadierCommand {
        val root = BrigadierCommand.literalArgumentBuilder(LITERAL)
            .requires { it.hasPermission(PERMISSION) }
            .executes { ctx ->
                send(ctx, "&cUsage: /$LITERAL <minutes> [server] | cancel [server] | status")
                0
            }
            // /schedrestart status
            .then(
                BrigadierCommand.literalArgumentBuilder("status")
                    .executes { ctx ->
                        renderStatus(ctx, handler.status())
                        1
                    },
            )
            .then(networkBranch("proxy", PlanType.PROXY))
            .then(networkBranch("network", PlanType.NETWORK))
            .then(serverBranch())
            .then(
                BrigadierCommand.literalArgumentBuilder("at")
                    .then(atBranch("proxy", PlanType.PROXY))
                    .then(atBranch("network", PlanType.NETWORK))
                    .then(
                        BrigadierCommand.literalArgumentBuilder("server")
                            .then(
                                configuredServerArgument("server")
                                    .then(atTimeBranch(PlanType.SERVER, "server")),
                            ),
                    ),
            )
            // /schedrestart cancel [server]
            .then(
                BrigadierCommand.literalArgumentBuilder("cancel")
                    .executes { ctx ->
                        val target = resolveTarget(ctx, explicit = null) ?: return@executes 0
                        if (network?.cancel(target) == true) {
                            send(ctx, "<green>Cancelled restart for <white>${target.value}<green>.")
                            return@executes 1
                        }
                        renderResult(ctx, handler.cancel(target))
                        1
                    }
                    .then(
                        BrigadierCommand.literalArgumentBuilder("proxy")
                            .executes { ctx -> cancelNetwork(ctx, PlanType.PROXY) },
                    )
                    .then(
                        BrigadierCommand.literalArgumentBuilder("network")
                            .executes { ctx -> cancelNetwork(ctx, PlanType.NETWORK) },
                    )
                    .then(
                        configuredServerArgument("server")
                            .executes { ctx ->
                                val requested = StringArgumentType.getString(ctx, "server")
                                if (network?.cancel(requested) == true) {
                                    send(ctx, "<green>Cancelled restart plan <white>#$requested<green>.")
                                    return@executes 1
                                }
                                val target = resolveTarget(ctx, explicit = requested)
                                    ?: return@executes 0
                                if (network?.cancel(target) == true) {
                                    send(ctx, "<green>Cancelled restart for <white>${target.value}<green>.")
                                    return@executes 1
                                }
                                renderResult(ctx, handler.cancel(target))
                                1
                            },
                    ),
            )
            // /schedrestart <minutes> [server]
            .then(
                BrigadierCommand.requiredArgumentBuilder<Int>(
                    "minutes",
                    IntegerArgumentType.integer(1),
                )
                    .executes { ctx ->
                        val minutes = IntegerArgumentType.getInteger(ctx, "minutes")
                        val target = resolveTarget(ctx, explicit = null) ?: return@executes 0
                        val now = Instant.now()
                        create(ctx, PlanType.SERVER, setOf(target), now.plus(Duration.ofMinutes(minutes.toLong())), now, false, "")
                    }
                    .then(
                        configuredServerArgument("server")
                            .executes { ctx ->
                                val minutes = IntegerArgumentType.getInteger(ctx, "minutes")
                                val target = resolveTarget(ctx, explicit = StringArgumentType.getString(ctx, "server"))
                                    ?: return@executes 0
                                val now = Instant.now()
                                create(ctx, PlanType.SERVER, setOf(target), now.plus(Duration.ofMinutes(minutes.toLong())), now, false, "")
                            },
                    ),
            )

        root.then(
            configuredServerArgument("target")
                .then(durationBranch(PlanType.SERVER) { context ->
                    setOf(ServerId(StringArgumentType.getString(context, "target")))
                }),
        )
        val node: LiteralCommandNode<CommandSource> = root.build()
        return BrigadierCommand(node)
    }

    private fun networkBranch(literal: String, type: PlanType) =
        BrigadierCommand.literalArgumentBuilder(literal)
            .then(
                BrigadierCommand.requiredArgumentBuilder<String>("duration", StringArgumentType.word())
                    .executes { ctx -> schedule(ctx, type, emptySet(), StringArgumentType.getString(ctx, "duration"), false, "") }
                    .then(
                        BrigadierCommand.literalArgumentBuilder("--silent")
                            .executes { ctx -> schedule(ctx, type, emptySet(), StringArgumentType.getString(ctx, "duration"), true, "") }
                            .then(
                                BrigadierCommand.requiredArgumentBuilder<String>("reason", StringArgumentType.greedyString())
                                    .executes { ctx -> schedule(ctx, type, emptySet(), StringArgumentType.getString(ctx, "duration"), true, StringArgumentType.getString(ctx, "reason")) },
                            ),
                    )
                    .then(
                        BrigadierCommand.requiredArgumentBuilder<String>("reason", StringArgumentType.greedyString())
                            .executes { ctx -> schedule(ctx, type, emptySet(), StringArgumentType.getString(ctx, "duration"), false, StringArgumentType.getString(ctx, "reason")) },
                    ),
            )

    private fun atBranch(literal: String, type: PlanType) =
        BrigadierCommand.literalArgumentBuilder(literal).then(atTimeBranch(type, null))

    private fun serverBranch() =
        BrigadierCommand.literalArgumentBuilder("server")
            .then(
                configuredServerArgument("server")
                    .then(durationBranch(PlanType.SERVER) { context ->
                        setOf(ServerId(StringArgumentType.getString(context, "server")))
                    }),
            )

    private fun durationBranch(type: PlanType, targets: (CommandContext<CommandSource>) -> Set<ServerId>) =
        BrigadierCommand.requiredArgumentBuilder<String>("duration", StringArgumentType.word())
            .executes { ctx -> schedule(ctx, type, targets(ctx), StringArgumentType.getString(ctx, "duration"), false, "") }
            .then(
                BrigadierCommand.literalArgumentBuilder("--silent")
                    .executes { ctx -> schedule(ctx, type, targets(ctx), StringArgumentType.getString(ctx, "duration"), true, "") }
                    .then(
                        BrigadierCommand.requiredArgumentBuilder<String>("reason", StringArgumentType.greedyString())
                            .executes { ctx -> schedule(ctx, type, targets(ctx), StringArgumentType.getString(ctx, "duration"), true, StringArgumentType.getString(ctx, "reason")) },
                    ),
            )
            .then(
                BrigadierCommand.requiredArgumentBuilder<String>("reason", StringArgumentType.greedyString())
                    .executes { ctx -> schedule(ctx, type, targets(ctx), StringArgumentType.getString(ctx, "duration"), false, StringArgumentType.getString(ctx, "reason")) },
            )

    private fun atTimeBranch(type: PlanType, serverArgument: String?) =
        BrigadierCommand.requiredArgumentBuilder<String>("time", StringArgumentType.word())
            .executes { ctx -> atSchedule(ctx, type, serverArgument, StringArgumentType.getString(ctx, "time"), false, "") }
            .then(
                BrigadierCommand.literalArgumentBuilder("--silent")
                    .executes { ctx -> atSchedule(ctx, type, serverArgument, StringArgumentType.getString(ctx, "time"), true, "") }
                    .then(BrigadierCommand.requiredArgumentBuilder<String>("reason", StringArgumentType.greedyString())
                        .executes { ctx -> atSchedule(ctx, type, serverArgument, StringArgumentType.getString(ctx, "time"), true, StringArgumentType.getString(ctx, "reason")) }),
            )
            .then(BrigadierCommand.requiredArgumentBuilder<String>("reason", StringArgumentType.greedyString())
                .executes { ctx -> atSchedule(ctx, type, serverArgument, StringArgumentType.getString(ctx, "time"), false, StringArgumentType.getString(ctx, "reason")) })

    private fun schedule(ctx: CommandContext<CommandSource>, type: PlanType, targets: Set<ServerId>, raw: String, silent: Boolean, reason: String): Int {
        if (!ctx.source.hasPermission(PERMISSION)) return 0
        val duration = try { RestartTimes.parseDuration(raw) } catch (error: IllegalArgumentException) { send(ctx, "<red>${error.message}"); return 0 }
        val now = Instant.now()
        return create(ctx, type, targets, now.plus(duration), now, silent, reason)
    }

    private fun atSchedule(ctx: CommandContext<CommandSource>, type: PlanType, serverArgument: String?, raw: String, silent: Boolean, reason: String): Int {
        if (!ctx.source.hasPermission(PERMISSION)) return 0
        val cfg = config?.invoke() ?: return 0
        val execution = try { RestartTimes.nextClock(raw, ZoneId.of(cfg.networkRestart.timezone), Instant.now()) } catch (error: IllegalArgumentException) { send(ctx, "<red>${error.message}"); return 0 }
        val warning = maxOf(Instant.now(), execution.minusSeconds(cfg.networkRestart.announcementPointsSeconds.maxOrNull() ?: 7200))
        val targets = serverArgument?.let { setOf(ServerId(StringArgumentType.getString(ctx, it))) }.orEmpty()
        return create(ctx, type, targets, execution, warning, silent, reason)
    }

    private fun create(ctx: CommandContext<CommandSource>, type: PlanType, targets: Set<ServerId>, execution: Instant, warning: Instant, silent: Boolean, reason: String): Int {
        val service = network ?: return 0
        return try {
            val resolved = if (type == PlanType.NETWORK) config?.invoke()?.networkRestart?.members?.toSet().orEmpty() else targets
            val plan = service.createManual(type, resolved, execution, warning, reason, ctx.source.toString(), silent)
            send(ctx, "<green>Scheduled <white>${type.name.lowercase()}<green> restart <white>#${plan.id.toString().take(8)}<green>.")
            1
        } catch (error: IllegalArgumentException) {
            send(ctx, "<red>${error.message}")
            0
        }
    }

    private fun cancelNetwork(ctx: CommandContext<CommandSource>, type: PlanType): Int {
        if (network?.cancel(type) != true) {
            send(ctx, "<red>No active ${type.name.lowercase()} restart plan was found.")
            return 0
        }
        send(ctx, "<green>Cancelled active ${type.name.lowercase()} restart plan.")
        return 1
    }

    private fun resolveTarget(ctx: CommandContext<CommandSource>, explicit: String?): ServerId? {
        if (explicit != null) return ServerId(explicit)
        val source = ctx.source
        if (source is Player) {
            val current = source.currentServer.orElse(null)
            if (current != null) return ServerId(current.serverInfo.name)
        }
        send(ctx, "&cConsole must specify a target server: /$LITERAL <minutes> <server>")
        return null
    }

    private fun configuredServerArgument(name: String) =
        BrigadierCommand.requiredArgumentBuilder<String>(name, StringArgumentType.word())
            .suggests { _, builder ->
                config?.invoke()?.networkRestart?.serverIds?.keys
                    ?.map(ServerId::value)
                    ?.filter { it.startsWith(builder.remaining, ignoreCase = true) }
                    ?.forEach(builder::suggest)
                builder.buildFuture()
            }

    private fun renderResult(ctx: CommandContext<CommandSource>, result: SchedCommandResult) {
        when (result) {
            is SchedCommandResult.Armed ->
                send(ctx, "&aArmed restart for &f${result.server.value}&a — countdown ${result.durationSeconds}s.")
            is SchedCommandResult.Cancelled ->
                send(ctx, "&aCancelled restart for &f${result.server.value}&a.")
            is SchedCommandResult.Rejected -> {
                // SECURITY (REQ-090, finding #9): the reason string may
                // include text derived from user input or exception
                // messages. Rendering it through MiniMessage would
                // interpret embedded <click>/<run_command> tags.
                // Render as plain text + the prefix label.
                ctx.source.sendMessage(
                    Component.text("Rejected: ", NamedTextColor.RED)
                        .append(Component.text(result.reason, NamedTextColor.WHITE)),
                )
            }
            is SchedCommandResult.Status -> renderStatus(ctx, result)
        }
    }

    private fun renderStatus(ctx: CommandContext<CommandSource>, result: SchedCommandResult) {
        val status = result as? SchedCommandResult.Status ?: return
        if (status.states.isEmpty()) {
            send(ctx, "&7No coordinators armed.")
            return
        }
        ctx.source.sendMessage(
            Component.text("Coordinator states:", NamedTextColor.GRAY),
        )
        status.states.forEach { (server, state) ->
            ctx.source.sendMessage(
                Component.text(" • ", NamedTextColor.DARK_GRAY)
                    .append(Component.text(server.value, NamedTextColor.WHITE))
                    .append(Component.text(" → ", NamedTextColor.DARK_GRAY))
                    .append(Component.text(state.name, NamedTextColor.AQUA)),
            )
        }
    }

    private fun send(ctx: CommandContext<CommandSource>, legacy: String) {
        // Legacy &-codes → MiniMessage-compatible Adventure components.
        val component = legacy
            .replace("&a", "<green>")
            .replace("&c", "<red>")
            .replace("&f", "<white>")
            .replace("&7", "<gray>")
        ctx.source.sendRichMessage(component)
    }
}
