package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.application.ports.ProxyLifecyclePort
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.domain.coordinator.RestartState
import com.badgersmc.queuerestart.velocity.domain.countdown.CountdownSchedule
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.time.Duration
import java.time.Instant

/** Command-facing contract for the proxy restart path. */
interface ProxyRestartController {
    val state: RestartState
    fun arm(durationSeconds: Int): Boolean
    fun cancel(): Boolean
}

/**
 * Drives a proxy-wide countdown and clean Velocity shutdown.
 *
 * Backend restarts are intentionally handled by [RestartOrchestrator] and a
 * Paper companion. The proxy is not a registered backend and cannot receive
 * that plugin message, so it needs its own lifecycle path.
 */
class ProxyRestartService(
    private val lifecycle: ProxyLifecyclePort,
    private val marksSupplier: () -> List<Int>,
    private val warningMessageSupplier: () -> String,
    private val hubSupplier: () -> ServerId,
    private val soundResolver: (Int) -> SoundCue?,
    private val onMark: (Int, Boolean) -> Unit = { _, _ -> },
) : ProxyRestartController {

    override var state: RestartState = RestartState.IDLE
        private set

    private var durationSeconds: Int = 0
    private var startedAt: Instant? = null
    private var schedule: CountdownSchedule? = null
    private var lastFiredSecond: Int? = null

    override fun arm(durationSeconds: Int): Boolean {
        require(durationSeconds > 0) { "durationSeconds must be > 0" }
        if (state != RestartState.IDLE) return false

        this.durationSeconds = durationSeconds
        startedAt = null
        schedule = CountdownSchedule(marksSupplier())
        lastFiredSecond = null
        state = RestartState.ARMED
        return true
    }

    override fun cancel(): Boolean {
        if (state != RestartState.ARMED && state != RestartState.COUNTDOWN) return false

        lifecycle.broadcast(CANCEL_MESSAGE)
        reset()
        return true
    }

    /** Advance the proxy countdown. Called by the plugin's existing 1 Hz task. */
    fun tick(now: Instant) {
        when (state) {
            RestartState.ARMED -> {
                startedAt = now
                state = RestartState.COUNTDOWN
                fireMark(durationSeconds)
            }

            RestartState.COUNTDOWN -> {
                val started = startedAt ?: return
                val elapsed = Duration.between(started, now).seconds.coerceAtLeast(0).toInt()
                val remaining = (durationSeconds - elapsed).coerceAtLeast(0)
                fireMark(remaining)
                if (remaining == 0) {
                    state = RestartState.RESTART_SENT
                    lifecycle.shutdown(SHUTDOWN_REASON)
                }
            }

            else -> Unit
        }
    }

    private fun fireMark(secondsRemaining: Int) {
        val mark = schedule?.fireAt(secondsRemaining) ?: return
        if (lastFiredSecond == mark.secondsRemaining) return
        lastFiredSecond = mark.secondsRemaining

        val template = if (mark.isT0) T0_MESSAGE else warningMessageSupplier()
        lifecycle.broadcast(
            template,
            mapOf(
                "server" to TARGET.value,
                "time" to formatTime(mark.secondsRemaining),
                "hub" to hubSupplier().value,
            ),
        )
        soundResolver(mark.secondsRemaining)?.let(lifecycle::playSound)
        onMark(mark.secondsRemaining, mark.isT0)
    }

    private fun reset() {
        state = RestartState.IDLE
        durationSeconds = 0
        startedAt = null
        schedule = null
        lastFiredSecond = null
    }

    private fun formatTime(seconds: Int): String = when {
        seconds == 0 -> "0s"
        seconds % 60 == 0 -> "${seconds / 60}m"
        else -> "${seconds}s"
    }

    companion object {
        val TARGET: ServerId = ServerId("proxy")
        const val T0_MESSAGE: String = "<red>Proxy restarting now. Reconnect shortly.</red>"
        const val CANCEL_MESSAGE: String = "<green>Proxy restart cancelled.</green>"
        const val SHUTDOWN_REASON: String = "Scheduled proxy restart. Reconnect shortly."
    }
}
