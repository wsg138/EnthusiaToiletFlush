package com.badgersmc.queuerestart.velocity.infrastructure.command

import com.badgersmc.queuerestart.velocity.application.schedule.AdminCommandResult
import com.badgersmc.queuerestart.velocity.application.schedule.QRestartAdminCommandHandler
import com.mojang.brigadier.arguments.StringArgumentType
import com.mojang.brigadier.context.CommandContext
import com.mojang.brigadier.tree.LiteralCommandNode
import com.velocitypowered.api.command.BrigadierCommand
import com.velocitypowered.api.command.CommandSource

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
                send(ctx, "<green>Triggered schedule <white>${result.schedule}<green>.")
            is AdminCommandResult.Resolved ->
                send(ctx, "<yellow>Resolved NEEDS_REVIEW plan <white>${result.plan}</white>. Verify the network manually before reopening access.</yellow>")
            is AdminCommandResult.Rejected ->
                send(ctx, "<red>Rejected: ${result.reason}")
        }
    }

    private fun send(ctx: CommandContext<CommandSource>, mini: String) {
        ctx.source.sendRichMessage(mini)
    }
}
