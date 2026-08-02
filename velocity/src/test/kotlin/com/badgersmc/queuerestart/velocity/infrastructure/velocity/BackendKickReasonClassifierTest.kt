package com.badgersmc.queuerestart.velocity.infrastructure.velocity

import net.kyori.adventure.text.Component
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class BackendKickReasonClassifierTest {
    @Test
    fun `recognizes the vanilla not whitelisted translation key`() {
        assertThat(
            BackendKickReasonClassifier.isWhitelist(
                Component.translatable("multiplayer.disconnect.not_whitelisted"),
            ),
        ).isTrue()
    }

    @Test
    fun `recognizes common plain text whitelist messages`() {
        assertThat(
            BackendKickReasonClassifier.isWhitelist(
                Component.text("You are not white-listed on this server!"),
            ),
        ).isTrue()
    }

    @Test
    fun `does not replace unrelated backend kick reasons`() {
        assertThat(
            BackendKickReasonClassifier.isWhitelist(
                Component.text("Flying is not enabled on this server"),
            ),
        ).isFalse()
    }
}
