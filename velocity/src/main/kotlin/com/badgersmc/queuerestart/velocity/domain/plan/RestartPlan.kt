package com.badgersmc.queuerestart.velocity.domain.plan

import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

enum class PlanType { SERVER, PROXY, NETWORK }

enum class PlanState {
    SCHEDULED,
    COUNTING_DOWN,
    PREFLIGHT,
    TRANSFERRING,
    DISPATCHING,
    COMPLETED,
    CANCELLED,
    FAILED,
    MISSED,
    NEEDS_REVIEW,
}

data class RestartPlan(
    val id: UUID = UUID.randomUUID(),
    val type: PlanType,
    val targets: Set<ServerId>,
    val createdAt: Instant,
    val executionAt: Instant,
    val warningAt: Instant,
    val reason: String = "",
    val creator: String,
    val automaticKey: String? = null,
    val silent: Boolean = false,
    @Volatile var state: PlanState = PlanState.SCHEDULED,
    val announcedSeconds: MutableSet<Long> = ConcurrentHashMap.newKeySet(),
    val targetResults: MutableMap<String, String> = ConcurrentHashMap(),
    val dispatchedActionKeys: MutableSet<String> = ConcurrentHashMap.newKeySet(),
    val acceptedActionKeys: MutableSet<String> = ConcurrentHashMap.newKeySet(),
    @Volatile var actionStarted: Boolean = false,
    @Volatile var maintenanceEnabled: Boolean = false,
    @Volatile var dryRun: Boolean = false,
    /** True only after the ephemeral backend coordinator accepted this plan. */
    @Volatile var backendArmAccepted: Boolean = false,
    /** Authenticated backend JVM identities captured immediately before destructive action. */
    val baselineBootIds: MutableMap<ServerId, UUID> = ConcurrentHashMap(),
    /** Velocity process identity captured before a proxy-including power action. */
    @Volatile var proxyBaselineBootId: UUID? = null,
    /** Deadline for observing the expected authenticated boot transitions. */
    @Volatile var executionDeadlineAt: Instant? = null,
    /** Previous observed countdown value used to detect crossed warning marks. */
    @Volatile var lastObservedRemainingSeconds: Long? = null,
    /** Wall-clock time at which this plan's restart action completed. */
    @Volatile var completedAt: Instant? = null,
    var failure: String = "",
) {
    fun active(): Boolean = state in setOf(
        PlanState.SCHEDULED,
        PlanState.COUNTING_DOWN,
        PlanState.PREFLIGHT,
        PlanState.TRANSFERRING,
        PlanState.DISPATCHING,
    )

    fun cancellable(): Boolean = state in setOf(PlanState.SCHEDULED, PlanState.COUNTING_DOWN)

    /** Unresolved destructive state blocks every overlapping future restart. */
    fun blocksScheduling(): Boolean = active() || state == PlanState.NEEDS_REVIEW
}
