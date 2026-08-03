package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.application.ports.SoundCue

/** Shared sound-key policy for backend, proxy, and full-network countdowns. */
object CountdownSoundPolicy {
    fun resolve(sounds: Map<String, SoundCue>, secondsRemaining: Long): SoundCue? {
        val key = when (secondsRemaining) {
            0L -> "t0"
            in 1L..10L -> "tick"
            30L -> "30s"
            60L -> "1m"
            120L -> "2m"
            300L -> "5m"
            600L -> "10m"
            1200L -> "20m"
            else -> return null
        }
        return sounds[key]
    }
}
