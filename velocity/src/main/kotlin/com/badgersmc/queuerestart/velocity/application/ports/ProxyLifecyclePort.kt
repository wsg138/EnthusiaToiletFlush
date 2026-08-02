package com.badgersmc.queuerestart.velocity.application.ports

/**
 * Outbound port for proxy-wide restart side effects.
 *
 * Kept separate from [ProxyPort] because backend roster, transfer, and ping
 * operations are unrelated to the lifecycle of the Velocity process itself.
 */
interface ProxyLifecyclePort {
    /** Broadcast a MiniMessage-formatted warning to every connected player. */
    fun broadcast(miniMessage: String, placeholders: Map<String, String> = emptyMap())

    /** Play [cue] for every player connected to the proxy. */
    fun playSound(cue: SoundCue)

    /** Cleanly stop Velocity, disconnecting players with [reason]. */
    fun shutdown(reason: String)
}
