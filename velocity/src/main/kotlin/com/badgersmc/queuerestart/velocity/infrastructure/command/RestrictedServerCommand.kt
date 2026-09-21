package com.badgersmc.queuerestart.velocity.infrastructure.command

import com.badgersmc.queuerestart.velocity.application.ports.QueueRestartConfig
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.infrastructure.velocity.BackendRestrictionPolicy
import com.velocitypowered.api.command.SimpleCommand
import com.velocitypowered.api.proxy.Player
import com.velocitypowered.api.proxy.ProxyServer
import com.velocitypowered.api.proxy.server.RegisteredServer
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.event.ClickEvent
import net.kyori.adventure.text.format.NamedTextColor

class RestrictedServerCommand(
    private val proxy: ProxyServer,
    private val config: () -> QueueRestartConfig,
) : SimpleCommand {
    override fun execute(invocation: SimpleCommand.Invocation) {
        val player = invocation.source() as? Player
        if (player == null) {
            invocation.source().sendMessage(Component.text("This command is player-only.", NamedTextColor.RED))
            return
        }

        val args = invocation.arguments()
        if (args.isEmpty()) {
            showServers(player)
            return
        }

        val target = visibleServers(player)
            .firstOrNull { it.serverInfo.name.equals(args[0], ignoreCase = true) }
        if (target == null) {
            player.sendMessage(Component.text("Unknown server.", NamedTextColor.RED))
            return
        }
        player.createConnectionRequest(target).fireAndForget()
    }

    override fun suggest(invocation: SimpleCommand.Invocation): List<String> {
        val player = invocation.source() as? Player ?: return emptyList()
        val prefix = invocation.arguments().firstOrNull().orEmpty()
        return visibleServers(player)
            .map { it.serverInfo.name }
            .filter { it.startsWith(prefix, ignoreCase = true) }
            .sortedWith(String.CASE_INSENSITIVE_ORDER)
    }

    override fun hasPermission(invocation: SimpleCommand.Invocation): Boolean =
        invocation.source().hasPermission("velocity.command.server")

    private fun visibleServers(player: Player): List<RegisteredServer> =
        proxy.allServers.filter { server ->
            val required = BackendRestrictionPolicy.requiredPermission(
                config(),
                ServerId(server.serverInfo.name),
            )
            required == null || player.hasPermission(required)
        }

    private fun showServers(player: Player) {
        val current = player.currentServer.map { it.serverInfo.name }.orElse("unknown")
        player.sendMessage(
            Component.text("Current server: ", NamedTextColor.GRAY)
                .append(Component.text(current, NamedTextColor.YELLOW)),
        )
        val visible = visibleServers(player).sortedBy { it.serverInfo.name.lowercase() }
        var line = Component.text("Servers: ", NamedTextColor.GRAY)
        visible.forEachIndexed { index, server ->
            if (index > 0) line = line.append(Component.text(", ", NamedTextColor.DARK_GRAY))
            val name = server.serverInfo.name
            line = line.append(
                Component.text(name, NamedTextColor.AQUA)
                    .clickEvent(ClickEvent.runCommand("/server $name")),
            )
        }
        player.sendMessage(line)
    }
}
