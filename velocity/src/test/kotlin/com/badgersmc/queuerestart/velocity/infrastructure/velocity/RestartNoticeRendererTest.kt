package com.badgersmc.queuerestart.velocity.infrastructure.velocity

import com.badgersmc.queuerestart.velocity.application.ports.RestartNotice
import net.kyori.adventure.text.Component
import net.kyori.adventure.text.TextComponent
import net.kyori.adventure.text.format.TextDecoration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RestartNoticeRendererTest {
    private val renderer = RestartNoticeRenderer()

    @Test
    fun `normal notice has neutral root and separator-only strikethrough`() {
        val component = renderer.notice(
            RestartNotice(
                type = "NETWORK",
                heading = "FULL NETWORK RESTART",
                detail = "The network restarts in 1 minute.",
                warning = "Players will disconnect.",
                reason = "Maintenance",
            ),
        )

        assertThat(component.decoration(TextDecoration.STRIKETHROUGH))
            .isNotEqualTo(TextDecoration.State.TRUE)
        val textNodes = flatten(component).filterIsInstance<TextComponent>()
        val separators = textNodes.filter { it.content().startsWith("---") }
        assertThat(separators).hasSize(2)
        assertThat(separators).allMatch {
            it.decoration(TextDecoration.STRIKETHROUGH) == TextDecoration.State.TRUE
        }
        assertThat(textNodes.filterNot(separators::contains)).allMatch {
            it.decoration(TextDecoration.STRIKETHROUGH) != TextDecoration.State.TRUE
        }
        assertThat(textNodes.first { it.content().contains("Reason:") }
            .decoration(TextDecoration.STRIKETHROUGH)).isNotEqualTo(TextDecoration.State.TRUE)
    }

    @Test
    fun `urgent network text is not struck through`() {
        val component = renderer.notice(
            RestartNotice("NETWORK", "NETWORK RESTART", "Restarting in 5 seconds...", "", urgent = true),
        )

        assertThat(flatten(component).filterIsInstance<TextComponent>())
            .allMatch { it.decoration(TextDecoration.STRIKETHROUGH) != TextDecoration.State.TRUE }
    }

    @Test
    fun `disconnect screen remains neutral`() {
        val component = renderer.disconnect(
            RestartNotice("NETWORK", "Restarting", "Reconnect shortly.", "", "Maintenance", true),
        )

        assertThat(flatten(component).filterIsInstance<TextComponent>())
            .allMatch { it.decoration(TextDecoration.STRIKETHROUGH) != TextDecoration.State.TRUE }
    }

    private fun flatten(root: Component): List<Component> = buildList {
        fun visit(component: Component) {
            add(component)
            component.children().forEach(::visit)
        }
        visit(root)
    }
}
