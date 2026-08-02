package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.application.ports.ProxyRestartScheduleConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.LocalTime
import java.time.ZoneId

class ProxyScheduleDefinitionsTest {

    @Test
    fun `schedule fires warn minutes before configured shutdown time`() {
        val zone = ZoneId.of("America/New_York")
        val def = proxyScheduleDefinitions(
            ProxyRestartScheduleConfig(
                restartTimes = listOf(LocalTime.of(4, 0)),
                zone = zone,
                warnMinutes = 20,
            ),
        ).single()

        assertThat(def.name).isEqualTo("proxy-04:00")
        assertThat(def.target).isEqualTo(ProxyRestartService.TARGET)
        assertThat(def.cronExpression).isEqualTo("40 3 * * *")
        assertThat(def.warnMinutes).isEqualTo(20)
        assertThat(def.zone).isEqualTo(zone)
    }

    @Test
    fun `countdown start wraps across midnight`() {
        val defs = proxyScheduleDefinitions(
            ProxyRestartScheduleConfig(
                restartTimes = listOf(LocalTime.of(0, 10)),
                zone = ZoneId.of("UTC"),
                warnMinutes = 20,
            ),
        )

        assertThat(defs.single().cronExpression).isEqualTo("50 23 * * *")
    }

    @Test
    fun `empty restart time list disables automatic proxy schedules`() {
        val defs = proxyScheduleDefinitions(
            ProxyRestartScheduleConfig(
                restartTimes = emptyList(),
                zone = ZoneId.of("UTC"),
                warnMinutes = 20,
            ),
        )

        assertThat(defs).isEmpty()
    }
}
