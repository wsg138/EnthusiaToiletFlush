package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.common.schedule.BackendSchedule
import com.badgersmc.queuerestart.velocity.application.ports.SchedulerPort
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.LocalTime
import java.time.ZoneId

class BackendScheduleSyncAdditionalDefinitionsTest {

    private class RecordingScheduler : SchedulerPort {
        val scheduled = mutableListOf<ScheduleDefinition>()
        var cancelCalls = 0

        override fun schedule(def: ScheduleDefinition, onFire: (ScheduleDefinition) -> Unit) {
            scheduled += def
        }

        override fun cancelAll() {
            cancelCalls++
            scheduled.clear()
        }
    }

    @Test
    fun `proxy definitions remain registered when backend cache changes`() {
        val scheduler = RecordingScheduler()
        val service = ScheduleService(scheduler) {}
        val cache = BackendScheduleCache()
        val proxyDefinition = ScheduleDefinition(
            name = "proxy-04:00",
            target = ProxyRestartService.TARGET,
            cronExpression = "40 3 * * *",
            warnMinutes = 20,
            zone = ZoneId.of("UTC"),
        )
        val sync = BackendScheduleSync(
            cache = cache,
            scheduleService = service,
            additionalDefinitions = { listOf(proxyDefinition) },
        )

        sync.start()
        assertThat(service.all().map { it.name }).containsExactly("proxy-04:00")

        cache.put(
            ServerId("survival"),
            BackendSchedule(
                times = listOf(LocalTime.of(5, 0)),
                zone = ZoneId.of("UTC"),
                warnMinutes = 15,
            ),
        )

        assertThat(service.all().map { it.name })
            .containsExactlyInAnyOrder("survival-05:00", "proxy-04:00")
        assertThat(scheduler.scheduled.map { it.name })
            .containsExactlyInAnyOrder("survival-05:00", "proxy-04:00")
    }

    @Test
    fun `refresh rebuilds proxy-owned schedules from current supplier`() {
        val scheduler = RecordingScheduler()
        val service = ScheduleService(scheduler) {}
        val cache = BackendScheduleCache()
        var proxyDefinitions = listOf(
            ScheduleDefinition(
                name = "proxy-04:00",
                target = ProxyRestartService.TARGET,
                cronExpression = "40 3 * * *",
                warnMinutes = 20,
                zone = ZoneId.of("UTC"),
            ),
        )
        val sync = BackendScheduleSync(
            cache = cache,
            scheduleService = service,
            additionalDefinitions = { proxyDefinitions },
        )
        sync.start()

        proxyDefinitions = listOf(
            ScheduleDefinition(
                name = "proxy-06:00",
                target = ProxyRestartService.TARGET,
                cronExpression = "45 5 * * *",
                warnMinutes = 15,
                zone = ZoneId.of("UTC"),
            ),
        )
        sync.refresh()

        assertThat(service.all().map { it.name }).containsExactly("proxy-06:00")
        assertThat(scheduler.cancelCalls).isGreaterThanOrEqualTo(2)
    }
}
