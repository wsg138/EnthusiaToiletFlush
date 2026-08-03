package com.badgersmc.queuerestart.velocity.infrastructure.command

import com.badgersmc.queuerestart.velocity.application.schedule.AdminCommandResult
import com.badgersmc.queuerestart.velocity.application.schedule.QRestartAdminCommandHandler
import com.mojang.brigadier.arguments.StringArgumentType
import com.mojang.brigadier.context.CommandContext
import com.mojang.brigadier.tree.LiteralCommandNode
import com.velocitypowered.api.command.BrigadierCommand
import com.velocitypowered.api.command.CommandSource
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor

/**
 * Brigadier shim for `/qrestart`. Permission-gated on
 * `queuerestart.command.admin`.
 *
 * Tree:
 * ```
 *  /qrestart reload
 *  /qrestart trigger <name>
 *  /qrestart resolve <planIdPrefix>
 * ```
 */
class QRestartAdminCommand(
    private val handler: QRestartAdminCommandHandler,
) {
    companion object {
        const val PERMISSION = "queuerestart.command.admin"
        const val LITERAL = "qrestart"
    }

    fun build(): BrigadierCommand {
        val root = BrigadierCommand.literalArgumentBuilder(LITERAL)
            .requires { it.hasPermission(PERMISSION) }
            .executes { ctx ->
                send(ctx, "<red>Usage: /$LITERAL reload | trigger <scheduleName> | resolve <planIdPrefix>")
                0
            }
            .then(
                BrigadierCommand.literalArgumentBuilder("reload")
                    .executes { ctx ->
                        renderResult(ctx, handler.reload())
                        1
                    },
            )
            .then(
                BrigadierCommand.literalArgumentBuilder("trigger")
                    .then(
                        BrigadierCommand.requiredArgumentBuilder<String>(
                            "name",
                            StringArgumentType.word(),
                        )
                            .executes { ctx ->
                                val name = StringArgumentType.getString(ctx, "name")
                                renderResult(ctx, handler.trigger(name))
                                1
                            },
                    )
                    .executes { ctx ->
                        send(ctx, "<red>Usage: /$LITERAL trigger <scheduleName>")
                        0
                    },
            )
            .then(
                BrigadierCommand.literalArgumentBuilder("resolve")
                    .then(
                        BrigadierCommand.requiredArgumentBuilder<String>(
                            "plan",
                            StringArgumentType.word(),
                        ).executes { ctx ->
                            renderResult(ctx, handler.resolve(StringArgumentType.getString(ctx, "plan")))
                            1
                        },
                    )
                    .executes { ctx ->
                        send(ctx, "<red>Usage: /$LITERAL resolve <planIdPrefix>")
                        0
                    },
            )

        val node: LiteralCommandNode<CommandSource> = root.build()
        return BrigadierCommand(node)
    }

    private fun renderResult(ctx: CommandContext<CommandSource>, result: AdminCommandResult) {
        when (result) {
            is AdminCommandResult.Reloaded ->
                send(ctx, "<green>Config + cron reloaded. In-flight countdowns preserved.")
            is AdminCommandResult.Triggered ->
                ctx.source.sendMessage(
                    Component.text("Triggered schedule ", NamedTextColor.GREEN)
                        .append(Component.text(result.schedule, NamedTextColor.WHITE))
                        .append(Component.text(".", NamedTextColor.GREEN)),
                )
            is AdminCommandResult.Resolved ->
                ctx.source.sendMessage(
                    Component.text("Resolved NEEDS_REVIEW plan ", NamedTextColor.YELLOW)
                        .append(Component.text(result.plan, NamedTextColor.WHITE))
                        .append(
                            Component.text(
                                ". Verify the network manually before reopening access.",
                                NamedTextColor.YELLOW,
                            ),
                        ),
                )
            is AdminCommandResult.Rejected ->
                ctx.source.sendMessage(
                    Component.text("Rejected: ", NamedTextColor.RED)
                        .append(Component.text(result.reason, NamedTextColor.WHITE)),
                )
        }
    }

    private fun send(ctx: CommandContext<CommandSource>, mini: String) {
        ctx.source.sendRichMessage(mini)
    }
}
