package com.badgersmc.queuerestart.velocity.infrastructure.velocity

import com.badgersmc.queuerestart.velocity.application.ports.RestartNotice
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.format.TextDecoration

/** Builds neutral-root restart notices so separator decoration cannot leak. */
class RestartNoticeRenderer {
    fun notice(notice: RestartNotice): Component {
        if (notice.urgent) {
            return Component.empty()
                .append(prefix(notice))
                .append(Component.text(notice.detail, NamedTextColor.RED, TextDecoration.BOLD))
        }

        var output = Component.empty()
            .append(separator())
            .append(Component.newline())
            .append(prefix(notice))
            .append(Component.text(notice.heading, NamedTextColor.GOLD, TextDecoration.BOLD))
            .append(Component.newline())
            .append(prefix(notice))
            .append(Component.text(notice.detail, NamedTextColor.GRAY))
            .append(Component.newline())
            .append(prefix(notice))
            .append(Component.text(notice.warning, NamedTextColor.GRAY))
        if (notice.reason.isNotBlank()) {
            output = output
                .append(Component.newline())
                .append(prefix(notice))
                .append(Component.text("Reason: ${notice.reason}", NamedTextColor.YELLOW))
        }
        return output.append(Component.newline()).append(separator())
    }

    fun disconnect(notice: RestartNotice): Component {
        var output = Component.empty()
            .append(Component.text(notice.heading, NamedTextColor.RED))
            .append(Component.newline())
            .append(Component.text(notice.detail, NamedTextColor.GRAY))
            .append(Component.newline())
            .append(Component.text(notice.warning, NamedTextColor.GRAY))
        if (notice.reason.isNotBlank()) {
            output = output
                .append(Component.newline())
                .append(Component.text("Reason: ${notice.reason}", NamedTextColor.YELLOW))
        }
        return output
    }

    private fun separator(): Component = Component.text(
        "--------------------------------------------------",
        NamedTextColor.DARK_GRAY,
        TextDecoration.STRIKETHROUGH,
    )

    private fun prefix(notice: RestartNotice): Component = Component.text(
        if (notice.type == "SERVER") "[Server] " else "[Network] ",
        NamedTextColor.GOLD,
    )
}
