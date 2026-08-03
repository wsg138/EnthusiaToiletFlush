package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.application.ports.AudiencePort
import com.badgersmc.queuerestart.velocity.application.ports.SoundCue
import com.badgersmc.queuerestart.velocity.domain.countdown.CountdownSchedule
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.plan.RestartTimes
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

data class CountdownPresentation(
    val messageTemplate: String,
    val t0Template: String,
    val soundResolver: (Int) -> SoundCue?,
)

/**
 * REQ-003, REQ-004.
 *
 * Tracks the previous observed remaining time and consumes every crossed mark.
 * When a delayed tick crosses multiple marks, only the newest relevant mark is
 * presented, preventing stale-message bursts. Presentation is resolved at fire
 * time so a successful reload affects active and future countdowns.
 */
class CountdownBroadcaster(
    private val audience: AudiencePort,
    messageTemplate: String = "",
    t0Template: String = "",
    soundResolver: (Int) -> SoundCue? = { null },
    private val presentationSupplier: (() -> CountdownPresentation)? = null,
    /** Operator-visible log line per fired mark. Default no-op for tests. */
    private val onMark: (ServerId, Int, Boolean) -> Unit = { _, _, _ -> },
) {
    private val fallbackPresentation = CountdownPresentation(messageTemplate, t0Template, soundResolver)

    private data class Active(
        val schedule: CountdownSchedule,
        val hub: ServerId,
        var previousRemaining: Int?,
        val consumed: MutableSet<Int> = ConcurrentHashMap.newKeySet(),
        val silent: Boolean = false,
    )

    private val active = ConcurrentHashMap<ServerId, Active>()

    fun register(
        target: ServerId,
        schedule: CountdownSchedule,
        hub: ServerId,
        silent: Boolean = false,
        startingSeconds: Int? = null,
    ) {
        val previous = startingSeconds?.let { if (it == Int.MAX_VALUE) it else it + 1 }
        active[target] = Active(schedule, hub, previous, silent = silent)
    }

    fun cancel(target: ServerId) {
        active.remove(target)
    }

    fun tick(target: ServerId, secondsRemaining: Int) {
        require(secondsRemaining >= 0) { "secondsRemaining must be ≥ 0" }
        val countdown = active[target] ?: return
        synchronized(countdown) {
            val previous = countdown.previousRemaining
            val crossed = when {
                previous == null -> countdown.schedule.fireAt(secondsRemaining)?.let(::listOf).orEmpty()
                secondsRemaining > previous -> emptyList()
                else -> countdown.schedule.crossedMarks(previous, secondsRemaining)
            }.filter { countdown.consumed.add(it.secondsRemaining) }

            if (previous == null || secondsRemaining < previous) {
                countdown.previousRemaining = secondsRemaining
            }

            // configuredMarks are descending; the smallest crossed value is
            // the newest warning closest to the current point in time.
            val mark = crossed.minByOrNull { it.secondsRemaining } ?: return
            if (countdown.silent) return

            val presentation = presentationSupplier?.invoke() ?: fallbackPresentation
            val template = if (mark.isT0) presentation.t0Template else presentation.messageTemplate
            val placeholders = mapOf(
                "server" to target.value,
                "time" to formatTime(mark.secondsRemaining),
                "hub" to countdown.hub.value,
            )
            audience.broadcast(target, template, placeholders)
            presentation.soundResolver(mark.secondsRemaining)?.let { audience.playSound(target, it) }
            onMark(target, mark.secondsRemaining, mark.isT0)
        }
    }

    private fun formatTime(seconds: Int): String = RestartTimes.format(Duration.ofSeconds(seconds.toLong()))
}
