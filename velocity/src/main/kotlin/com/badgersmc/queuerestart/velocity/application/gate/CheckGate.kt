package com.badgersmc.queuerestart.velocity.application.gate

import com.badgersmc.queuerestart.common.protocol.CheckOutcome
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import java.util.concurrent.ConcurrentHashMap

/** Result of a gate decision for one player. */
enum class GateOutcome {
    /** Player may proceed into the rejoin queue. */
    RELEASED,

    /** Player removed from the cohort and must not be re-queued. */
    DROPPED,

    /** Still awaiting a verdict. */
    PENDING,

    /** No record of this player in the gate. */
    UNKNOWN,
}

/**
 * REQ-040, REQ-041, REQ-042, REQ-043.
 *
 * Per-player gate that holds cohort members on the hub until CheckHacks
 * reports a verdict (or a timeout fires). Bypass-perm holders skip entirely.
 *
 * Time is supplied by the caller — the application layer pulls it from
 * [com.badgersmc.queuerestart.velocity.application.ports.ClockPort] when
 * registering players and ticking expirations.
 */
class CheckGate(
    private val timeoutSeconds: () -> Int,
    private val releaseOnTimeout: () -> Boolean,
) {
    constructor(timeoutSeconds: Int, releaseOnTimeout: Boolean) : this({ timeoutSeconds }, { releaseOnTimeout })

    private data class PendingEntry(val deadlineSeconds: Long)

    // Touched from the proxy tick (timeouts), the messaging callback
    // (CheckHacksResult), and the rejoin path (register at server-up).
    private val pending = ConcurrentHashMap<PlayerId, PendingEntry>()

    /** Add [playerId] to the gate. Bypass holders are released immediately. */
    fun register(playerId: PlayerId, hasBypass: Boolean, nowSeconds: Long): GateOutcome {
        if (hasBypass) {
            pending.remove(playerId)
            return GateOutcome.RELEASED
        }
        pending[playerId] = PendingEntry(deadlineSeconds = nowSeconds + timeoutSeconds())
        return GateOutcome.PENDING
    }

    /** Apply a CheckHacks verdict. Unknown players return [GateOutcome.UNKNOWN]. */
    fun onResult(playerId: PlayerId, outcome: CheckOutcome): GateOutcome {
        if (pending.remove(playerId) == null) return GateOutcome.UNKNOWN
        return when (outcome) {
            CheckOutcome.CLEAN, CheckOutcome.PROTECTED -> GateOutcome.RELEASED
            CheckOutcome.DETECTED -> GateOutcome.DROPPED
            CheckOutcome.TIMEOUT -> if (releaseOnTimeout()) GateOutcome.RELEASED else GateOutcome.DROPPED
        }
    }

    /**
     * Advance the gate clock. Returns every player whose deadline has
     * elapsed and the outcome to apply per [releaseOnTimeout].
     */
    fun tick(nowSeconds: Long): List<Pair<PlayerId, GateOutcome>> {
        val expired = pending.entries.filter { it.value.deadlineSeconds <= nowSeconds }
        if (expired.isEmpty()) return emptyList()
        val outcome = if (releaseOnTimeout()) GateOutcome.RELEASED else GateOutcome.DROPPED
        for ((pid, _) in expired) pending.remove(pid)
        return expired.map { it.key to outcome }
    }

    fun isPending(playerId: PlayerId): Boolean = pending.containsKey(playerId)
}
