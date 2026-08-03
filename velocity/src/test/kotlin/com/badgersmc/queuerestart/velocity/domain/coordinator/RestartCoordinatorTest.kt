package com.badgersmc.queuerestart.velocity.domain.coordinator

import com.badgersmc.queuerestart.velocity.domain.cohort.Cohort
import com.badgersmc.queuerestart.velocity.domain.cohort.CohortMember
import com.badgersmc.queuerestart.velocity.domain.id.PlayerId
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * REQ-001, REQ-002, REQ-005, REQ-061, implementation.md §5.
 *
 * Verifies the state machine:
 *
 *  IDLE → ARMED → COUNTDOWN → DRAINING → RESTART_SENT
 *       → SERVER_DOWN → REJOIN_RELEASE → IDLE
 *
 * Cancel is legal only in ARMED, COUNTDOWN. Arming while not IDLE is rejected.
 */
class RestartCoordinatorTest {

    private val serverId = ServerId("survival")
    private val cohort = Cohort(setOf(CohortMember(PlayerId(UUID.randomUUID()))))

    private fun fresh() = RestartCoordinator(serverId)

    @Test
    fun `starts idle`() {
        assertThat(fresh().state).isEqualTo(RestartState.IDLE)
    }

    @Test
    fun `arm transitions IDLE to ARMED and snapshots cohort`() {
        val c = fresh()
        c.arm(cohort, durationSeconds = 60)
        assertThat(c.state).isEqualTo(RestartState.ARMED)
        assertThat(c.cohort).isEqualTo(cohort)
        assertThat(c.durationSeconds).isEqualTo(60)
    }

    @Test
    fun `double arm is rejected (REQ-061)`() {
        val c = fresh().also { it.arm(cohort, 60) }
        assertThatThrownBy { c.arm(cohort, 60) }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `full happy path drives back to IDLE`() {
        val c = fresh()
        c.arm(cohort, 60)
        c.beginCountdown()
        assertThat(c.state).isEqualTo(RestartState.COUNTDOWN)
        c.beginDrain()
        assertThat(c.state).isEqualTo(RestartState.DRAINING)
        c.restartSent(UUID.randomUUID())
        assertThat(c.state).isEqualTo(RestartState.RESTART_SENT)
        c.serverDown()
        assertThat(c.state).isEqualTo(RestartState.SERVER_DOWN)
        c.serverUp()
        assertThat(c.state).isEqualTo(RestartState.REJOIN_RELEASE)
        c.releaseComplete()
        assertThat(c.state).isEqualTo(RestartState.IDLE)
    }

    @Test
    fun `cancel from ARMED returns to IDLE`() {
        val c = fresh().also { it.arm(cohort, 60) }
        c.cancel()
        assertThat(c.state).isEqualTo(RestartState.IDLE)
    }

    @Test
    fun `cancel from COUNTDOWN returns to IDLE`() {
        val c = fresh().also {
            it.arm(cohort, 60)
            it.beginCountdown()
        }
        c.cancel()
        assertThat(c.state).isEqualTo(RestartState.IDLE)
    }

    @Test
    fun `cancel from IDLE is rejected`() {
        assertThatThrownBy { fresh().cancel() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `cancel from DRAINING is rejected`() {
        val c = fresh().also {
            it.arm(cohort, 60)
            it.beginCountdown()
            it.beginDrain()
        }
        assertThatThrownBy { c.cancel() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `cancel after restart sent is rejected`() {
        val c = fresh().also {
            it.arm(cohort, 60)
            it.beginCountdown()
            it.beginDrain()
            it.restartSent(UUID.randomUUID())
        }
        assertThatThrownBy { c.cancel() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `beginCountdown from IDLE is rejected`() {
        assertThatThrownBy { fresh().beginCountdown() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `beginDrain from ARMED is rejected`() {
        val c = fresh().also { it.arm(cohort, 60) }
        assertThatThrownBy { c.beginDrain() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `restartSent from COUNTDOWN is rejected`() {
        val c = fresh().also {
            it.arm(cohort, 60)
            it.beginCountdown()
        }
        assertThatThrownBy { c.restartSent(UUID.randomUUID()) }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `serverUp from RESTART_SENT is rejected (must serverDown first)`() {
        val c = fresh().also {
            it.arm(cohort, 60)
            it.beginCountdown()
            it.beginDrain()
            it.restartSent(UUID.randomUUID())
        }
        assertThatThrownBy { c.serverUp() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `releaseComplete only legal in REJOIN_RELEASE`() {
        val c = fresh().also { it.arm(cohort, 60) }
        assertThatThrownBy { c.releaseComplete() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `cancel transition clears cohort snapshot`() {
        val c = fresh().also { it.arm(cohort, 60) }
        c.cancel()
        assertThat(c.cohort).isNull()
    }

    @Test
    fun `arm again after a complete cycle is allowed`() {
        val c = fresh()
        c.arm(cohort, 60)
        c.beginCountdown()
        c.beginDrain()
        c.restartSent(UUID.randomUUID())
        c.serverDown()
        c.serverUp()
        c.releaseComplete()
        // back to IDLE — should be reusable
        c.arm(cohort, 90)
        assertThat(c.state).isEqualTo(RestartState.ARMED)
        assertThat(c.durationSeconds).isEqualTo(90)
    }
}
