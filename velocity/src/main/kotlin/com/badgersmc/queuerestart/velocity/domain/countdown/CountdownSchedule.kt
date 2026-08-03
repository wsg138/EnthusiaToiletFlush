package com.badgersmc.queuerestart.velocity.domain.countdown

/**
 * A point in the countdown at which the proxy broadcasts + plays a sound.
 * `secondsRemaining == 0` is T-0 (the moment the restart fires).
 */
data class MarkSecond(val secondsRemaining: Int) {
    init {
        require(secondsRemaining >= 0) { "secondsRemaining must be ≥ 0, got $secondsRemaining" }
    }

    val isT0: Boolean get() = secondsRemaining == 0
}

/**
 * REQ-003, REQ-004.
 *
 * T-0 is always a mark. [crossedMarks] makes countdown delivery resilient to
 * scheduler delay by identifying every configured mark crossed between two
 * observations; callers can consume all of them while presenting only the
 * newest relevant mark.
 */
class CountdownSchedule(rawMarks: Collection<Int>) {

    /** All marks (configured + implicit T-0) in descending order. */
    val configuredMarks: List<MarkSecond>

    private val markSet: Set<Int>

    init {
        require(rawMarks.none { it < 0 }) { "marks must be ≥ 0: $rawMarks" }
        val deduped = (rawMarks.toSet() + 0).sortedDescending()
        configuredMarks = deduped.map { MarkSecond(it) }
        markSet = deduped.toSet()
    }

    /** Returns a [MarkSecond] when [secondsRemaining] is an exact mark. */
    fun fireAt(secondsRemaining: Int): MarkSecond? =
        if (secondsRemaining >= 0 && secondsRemaining in markSet) MarkSecond(secondsRemaining) else null

    /**
     * Returns marks in `[currentRemaining, previousRemaining)` in descending
     * order. An upward clock jump produces no marks and never re-opens an
     * already consumed portion of the countdown.
     */
    fun crossedMarks(previousRemaining: Int, currentRemaining: Int): List<MarkSecond> {
        require(previousRemaining >= 0) { "previousRemaining must be ≥ 0" }
        require(currentRemaining >= 0) { "currentRemaining must be ≥ 0" }
        if (currentRemaining > previousRemaining) return emptyList()
        return configuredMarks.filter {
            it.secondsRemaining < previousRemaining && it.secondsRemaining >= currentRemaining
        }
    }
}
