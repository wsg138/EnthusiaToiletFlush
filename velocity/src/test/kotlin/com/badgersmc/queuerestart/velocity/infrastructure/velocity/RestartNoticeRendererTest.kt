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
        val textNodes = effectiveTextNodes(component)
        val separators = textNodes.filter { it.component.content().startsWith("---") }
        assertThat(separators).hasSize(2)
        assertThat(separators).allMatch(EffectiveTextNode::struckThrough)
        assertThat(textNodes.filterNot(separators::contains)).allMatch { !it.struckThrough }
        assertThat(textNodes.first { it.component.content().contains("Reason:") }.struckThrough).isFalse()
    }

    @Test
    fun `urgent network text and reason are not struck through`() {
        val component = renderer.notice(
            RestartNotice(
                "NETWORK",
                "NETWORK RESTART",
                "Restarting in 5 seconds...",
                "",
                "Maintenance",
                true,
            ),
        )

        val textNodes = effectiveTextNodes(component)
        assertThat(textNodes).allMatch { !it.struckThrough }
        assertThat(textNodes.map { it.component.content() }.joinToString(""))
            .contains("Restarting in 5 seconds...")
            .contains("Maintenance")
    }

    @Test
    fun `disconnect screen remains neutral`() {
        val component = renderer.disconnect(
            RestartNotice("NETWORK", "Restarting", "Reconnect shortly.", "", "Maintenance", true),
        )

        assertThat(effectiveTextNodes(component)).allMatch { !it.struckThrough }
    }

    private fun effectiveTextNodes(root: Component): List<EffectiveTextNode> = buildList {
        fun visit(component: Component, inherited: TextDecoration.State) {
            val own = component.decoration(TextDecoration.STRIKETHROUGH)
            val effective = if (own == TextDecoration.State.NOT_SET) inherited else own
            if (component is TextComponent) {
                add(EffectiveTextNode(component, effective == TextDecoration.State.TRUE))
            }
            component.children().forEach { visit(it, effective) }
        }
        visit(root, TextDecoration.State.FALSE)
    }

    private data class EffectiveTextNode(
        val component: TextComponent,
        val struckThrough: Boolean,
    )
}
