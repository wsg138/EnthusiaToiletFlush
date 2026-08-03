package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.domain.cohort.Cohort
import com.badgersmc.queuerestart.velocity.domain.cohort.CohortMember
import com.badgersmc.queuerestart.velocity.domain.coordinator.RestartState
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * REQ-001, REQ-005, REQ-052, REQ-060, REQ-061, REQ-062.
 *
 * Pure-application command logic — no Brigadier here. The actual
 * Brigadier-bound `SchedRestartCommand` (`infrastructure/command/`) is a
 * thin shim that parses args and forwards to this handler.
 */
class SchedRestartCommandHandlerTest {

    private val survival = ServerId("survival")
    private val creative = ServerId("creative")
    private val hub = ServerId("lobby")

    private fun cohort(name: String) = Cohort(setOf(
        CohortMember(PlayerId(UUID.nameUUIDFromBytes(name.toByteArray()))),
    ))

    private fun handler(
        companionOn: Set<ServerId> = setOf(survival, creative),
        cohorts: Map<ServerId, Cohort> = mapOf(survival to cohort("p"), creative to cohort("p")),
        hubServer: () -> ServerId = { hub },
    ) = SchedRestartCommandHandler(
        registry = CoordinatorRegistry(),
        hubServer = hubServer,
        companionPresent = { it in companionOn },
        cohortFor = { cohorts[it] ?: Cohort(emptySet()) },
    )

    @Test
    fun `arm valid target returns Armed`() {
        val result = handler().arm(survival, durationMinutes = 5)
        assertThat(result).isEqualTo(SchedCommandResult.Armed(survival, 300))
    }

    @Test
    fun `arm seconds preserves an exact companion delay`() {
        val result = handler().armSeconds(survival, durationSeconds = 61)
        assertThat(result).isEqualTo(SchedCommandResult.Armed(survival, 61))
    }

    @Test
    fun `arm hub target is rejected (REQ-060)`() {
        val result = handler().arm(hub, durationMinutes = 5)
        assertThat(result).isInstanceOf(SchedCommandResult.Rejected::class.java)
        assertThat((result as SchedCommandResult.Rejected).reason)
            .containsIgnoringCase("hub")
    }

    @Test
    fun `reloaded hub supplier protects the new hub`() {
        var currentHub = hub
        val handler = handler(hubServer = { currentHub })
        currentHub = survival

        val result = handler.arm(survival, durationMinutes = 5)

        assertThat(result).isInstanceOf(SchedCommandResult.Rejected::class.java)
        assertThat((result as SchedCommandResult.Rejected).reason)
            .containsIgnoringCase("hub")
    }

    @Test
    fun `arm while already armed is rejected (REQ-061)`() {
        val h = handler()
        h.arm(survival, 5)
        val again = h.arm(survival, 10)
        assertThat(again).isInstanceOf(SchedCommandResult.Rejected::class.java)
        assertThat((again as SchedCommandResult.Rejected).reason)
            .containsIgnoringCase("already")
    }

    @Test
    fun `arm without companion is rejected (REQ-062)`() {
        val result = handler(companionOn = emptySet()).arm(survival, 5)
        assertThat(result).isInstanceOf(SchedCommandResult.Rejected::class.java)
        assertThat((result as SchedCommandResult.Rejected).reason)
            .containsIgnoringCase("companion")
    }

    @Test
    fun `cancel from ARMED returns Cancelled (REQ-005)`() {
        val h = handler()
        h.arm(survival, 5)
        val result = h.cancel(survival)
        assertThat(result).isEqualTo(SchedCommandResult.Cancelled(survival))
    }

    @Test
    fun `cancel from idle is rejected`() {
        val result = handler().cancel(survival)
        assertThat(result).isInstanceOf(SchedCommandResult.Rejected::class.java)
    }

    @Test
    fun `status reports every coordinator state (REQ-052)`() {
        val h = handler()
        h.arm(survival, 5)
        // creative untouched — still IDLE (or absent, depending on registry).
        val result = h.status()
        assertThat(result).isInstanceOf(SchedCommandResult.Status::class.java)
        val states = (result as SchedCommandResult.Status).states
        assertThat(states[survival]).isEqualTo(RestartState.ARMED)
    }

    @Test
    fun `arm rejects non-positive duration`() {
        val result = handler().arm(survival, durationMinutes = 0)
        assertThat(result).isInstanceOf(SchedCommandResult.Rejected::class.java)
    }
}
