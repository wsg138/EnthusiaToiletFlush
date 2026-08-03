package com.badgersmc.queuerestart.velocity.domain.coordinator

import com.badgersmc.queuerestart.velocity.domain.cohort.Cohort
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.util.UUID

/** State machine positions per implementation.md §5. */
enum class RestartState {
    IDLE,
    ARMED,
    COUNTDOWN,
    DRAINING,
    RESTART_SENT,
    SERVER_DOWN,
    REJOIN_RELEASE,
}

/**
 * One per target server. Pure-domain state machine — drives no side effects
 * itself. The application layer feeds it events (`arm`, `beginCountdown`,
 * `beginDrain`, `restartSent`, `serverDown`, `serverUp`, `releaseComplete`,
 * `cancel`) sourced from clock ticks, plugin messages, and operator input.
 *
 * REQ-001, REQ-002, REQ-005, REQ-061. Illegal transitions throw
 * [IllegalStateException]; cancel is legal only in [RestartState.ARMED] and
 * [RestartState.COUNTDOWN]; double-arm is rejected.
 */
class RestartCoordinator(val serverId: ServerId) {

    @Volatile
    var state: RestartState = RestartState.IDLE
        private set

    var cohort: Cohort? = null
        private set

    var durationSeconds: Int = 0
        private set

    @Volatile
    var restartBaselineBootId: UUID? = null
        private set

    @Synchronized
    fun arm(cohort: Cohort, durationSeconds: Int) {
        require(durationSeconds >= 0) { "durationSeconds must be ≥ 0" }
        check(state == RestartState.IDLE) {
            "Cannot arm — coordinator for ${serverId.value} is in $state (REQ-061)"
        }
        this.cohort = cohort
        this.durationSeconds = durationSeconds
        state = RestartState.ARMED
    }

    fun beginCountdown() = transition(from = RestartState.ARMED, to = RestartState.COUNTDOWN)

    fun beginDrain() = transition(from = RestartState.COUNTDOWN, to = RestartState.DRAINING)

    @Synchronized
    fun restartSent(baselineBootId: UUID) {
        check(state == RestartState.DRAINING) {
            "Illegal transition: expected ${RestartState.DRAINING}, was $state (target ${RestartState.RESTART_SENT})"
        }
        restartBaselineBootId = baselineBootId
        state = RestartState.RESTART_SENT
    }

    fun serverDown() = transition(from = RestartState.RESTART_SENT, to = RestartState.SERVER_DOWN)

    fun serverUp() = transition(from = RestartState.SERVER_DOWN, to = RestartState.REJOIN_RELEASE)

    @Synchronized
    fun releaseComplete() {
        check(state == RestartState.REJOIN_RELEASE) {
            "Illegal transition: expected ${RestartState.REJOIN_RELEASE}, was $state (target ${RestartState.IDLE})"
        }
        cohort = null
        durationSeconds = 0
        restartBaselineBootId = null
        state = RestartState.IDLE
    }

    /** REQ-005. Legal only in ARMED, COUNTDOWN. */
    @Synchronized
    fun cancel() {
        check(state == RestartState.ARMED || state == RestartState.COUNTDOWN) {
            "Cannot cancel from $state — only ARMED or COUNTDOWN may be cancelled"
        }
        cohort = null
        durationSeconds = 0
        restartBaselineBootId = null
        state = RestartState.IDLE
    }

    /** Operator-only recovery after manually reconciling an uncertain restart. */
    @Synchronized
    fun forceResetAfterReview() {
        cohort = null
        durationSeconds = 0
        restartBaselineBootId = null
        state = RestartState.IDLE
    }

    @Synchronized
    private fun transition(from: RestartState, to: RestartState) {
        check(state == from) { "Illegal transition: expected $from, was $state (target $to)" }
        state = to
    }
}
