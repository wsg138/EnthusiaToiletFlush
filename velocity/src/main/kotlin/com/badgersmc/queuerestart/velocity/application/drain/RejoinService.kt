package com.badgersmc.queuerestart.velocity.application.drain

import com.badgersmc.queuerestart.common.protocol.CheckOutcome
import com.badgersmc.queuerestart.velocity.application.gate.CheckGate
import com.badgersmc.queuerestart.velocity.application.gate.GateOutcome
import com.badgersmc.queuerestart.velocity.application.ports.ProxyPort
import com.badgersmc.queuerestart.velocity.application.ports.QueuePort
import com.badgersmc.queuerestart.velocity.domain.cohort.Cohort
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.rank.RankLadder
import java.util.concurrent.ConcurrentHashMap

/**
 * REQ-030, REQ-032, REQ-033, REQ-034, REQ-040, REQ-041, REQ-042, REQ-043.
 *
 * Given a cohort snapshotted at arm-time and a target server that has
 * just come back online, holds every still-connected member at the
 * [CheckGate] until CheckHacks clears them (or [bypass.checkhacks]
 * holders skip the gate). Only `RELEASED` players are enqueued;
 * `DROPPED` (CheckHacks DETECTED) verdicts silently strand the player
 * on the hub.
 *
 * Two interaction points the orchestrator wires:
 *  - [onCheckHacksResult] — invoked when a `CheckHacksResultMessage` arrives.
 *  - [tick] — once per second from the proxy tick loop; drives gate
 *    timeouts.
 */
class RejoinService(
    private val proxy: ProxyPort,
    private val queue: QueuePort,
    private val rankLadder: () -> RankLadder,
    private val gate: CheckGate,
) {
    constructor(proxy: ProxyPort, queue: QueuePort, rankLadder: RankLadder, gate: CheckGate) :
        this(proxy, queue, { rankLadder }, gate)

    private data class Pending(val target: ServerId, val weight: Int)

    /** Players awaiting a gate verdict, keyed by player. */
    private val pending = ConcurrentHashMap<PlayerId, Pending>()

    fun enqueueRejoin(target: ServerId, cohort: Cohort, nowSeconds: Long) {
        for (member in cohort.members) {
            val pid = member.playerId
            if (!proxy.isOnline(pid)) continue
            val perms = proxy.permissionsOf(pid)
            val weight = rankLadder().resolve(perms)
            val hasBypass = "queuerestart.bypass.checkhacks" in perms
            val outcome = gate.register(pid, hasBypass, nowSeconds)
            when (outcome) {
                GateOutcome.RELEASED -> queue.enqueue(target, pid, weight)
                GateOutcome.PENDING -> pending[pid] = Pending(target, weight)
                GateOutcome.DROPPED, GateOutcome.UNKNOWN -> Unit
            }
        }
    }

    /**
     * REQ-040, REQ-041, REQ-090 (finding B): apply a CheckHacks verdict
     * only if the player is currently on the source backend that sent
     * it. CheckHacks runs on whichever backend the player is on (typically
     * the hub during the rejoin wait), so the source server attesting to
     * a verdict must actually host the player. Drops cross-backend
     * spoofs from a compromised backend that doesn't host the player.
     */
    fun onCheckHacksResult(source: ServerId, playerId: PlayerId, outcome: CheckOutcome) {
        if (!proxy.playersOn(source).contains(playerId)) return
        val applied = gate.onResult(playerId, outcome)
        finalize(playerId, applied)
    }

    /** REQ-042. Drive gate timeouts; enqueue or drop expired players. */
    fun tick(nowSeconds: Long) {
        for ((pid, outcome) in gate.tick(nowSeconds)) {
            finalize(pid, outcome)
        }
    }

    private fun finalize(playerId: PlayerId, outcome: GateOutcome) {
        when (outcome) {
            GateOutcome.RELEASED -> {
                val p = pending.remove(playerId) ?: return
                if (!proxy.isOnline(playerId)) return
                queue.enqueue(p.target, playerId, p.weight)
            }
            GateOutcome.DROPPED -> {
                pending.remove(playerId)
            }
            GateOutcome.PENDING, GateOutcome.UNKNOWN -> Unit
        }
    }
}
