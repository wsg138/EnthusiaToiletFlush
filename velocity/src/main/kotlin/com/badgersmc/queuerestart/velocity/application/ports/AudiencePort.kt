package com.badgersmc.queuerestart.velocity.application.ports

import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Outbound port — broadcasts to every player connected to a target
 * backend. Implemented by `infrastructure/audience/AdventureAudienceAdapter`.
 *
 * The port speaks raw MiniMessage strings + a placeholder map; rendering
 * to Adventure `Component` happens inside the adapter.
 */
interface AudiencePort {
    /**
     * Broadcast a chat message to all players currently on [target].
     * [placeholders] entries are substituted as `<key>` → `value` before
     * MiniMessage parsing.
     */
    fun broadcast(target: ServerId, miniMessage: String, placeholders: Map<String, String> = emptyMap())

    /** Disconnect one player with a rendered, configurable reason. */
    fun disconnect(playerId: PlayerId, miniMessage: String, placeholders: Map<String, String> = emptyMap())

    /**
     * Disconnect one player and complete after the adapter has observed the
     * player leave the proxy. The default preserves the legacy synchronous
     * contract for tests and non-Velocity adapters.
     */
    fun disconnectAndAwait(
        playerId: PlayerId,
        miniMessage: String,
        placeholders: Map<String, String> = emptyMap(),
    ): CompletionStage<Boolean> {
        disconnect(playerId, miniMessage, placeholders)
        return CompletableFuture.completedFuture(true)
    }

    /** Play [cue] for every player currently on [target]. */
    fun playSound(target: ServerId, cue: SoundCue)
}
