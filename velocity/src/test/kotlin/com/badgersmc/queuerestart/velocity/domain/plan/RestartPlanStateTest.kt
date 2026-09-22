package com.badgersmc.queuerestart.velocity.domain.plan

import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals

class RestartPlanStateTest {
    @Test
    fun `every plan state has explicit active cancellable and scheduling semantics`() {
        val active = setOf(
            PlanState.SCHEDULED,
            PlanState.COUNTING_DOWN,
            PlanState.PREFLIGHT,
            PlanState.TRANSFERRING,
            PlanState.DISPATCHING,
        )
        val cancellable = setOf(PlanState.SCHEDULED, PlanState.COUNTING_DOWN)
        val blocksScheduling = active + PlanState.NEEDS_REVIEW

        assertEquals(PlanState.entries.toSet(), (active + (PlanState.entries.toSet() - active)))

        PlanState.entries.forEach { state ->
            val plan = plan(state)
            assertEquals(state in active, plan.active(), "active policy for $state")
            assertEquals(state in cancellable, plan.cancellable(), "cancellable policy for $state")
            assertEquals(state in blocksScheduling, plan.blocksScheduling(), "schedule-block policy for $state")
        }
    }

    @Test
    fun `needs-review is not active or cancellable but still fences overlapping restarts`() {
        val plan = plan(PlanState.NEEDS_REVIEW)

        assertEquals(false, plan.active())
        assertEquals(false, plan.cancellable())
        assertEquals(true, plan.blocksScheduling())
    }

    private fun plan(state: PlanState) = RestartPlan(
        type = PlanType.SERVER,
        targets = setOf(ServerId("survival")),
        createdAt = Instant.parse("2026-01-01T00:00:00Z"),
        executionAt = Instant.parse("2026-01-01T00:05:00Z"),
        warningAt = Instant.parse("2026-01-01T00:04:00Z"),
        creator = "test",
        state = state,
    )
}
