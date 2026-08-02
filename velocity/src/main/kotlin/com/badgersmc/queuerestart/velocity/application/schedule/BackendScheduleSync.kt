package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.common.schedule.BackendSchedule
import com.badgersmc.queuerestart.velocity.domain.id.ServerId

/**
 * Subscribes to a [BackendScheduleCache] and reloads [ScheduleService] each
 * time the cache changes. Translates each `(server, time)` pair into a
 * [ScheduleDefinition] with cron expression `m h * * *` evaluated in the
 * backend-advertised zone — so the proxy's countdown UX fires `warnMinutes`
 * before each backend's locally-scheduled `Bukkit.shutdown()`.
 *
 * [additionalDefinitions] supplies proxy-owned schedules that do not come
 * from Paper companions, notably the Velocity process's own restart times.
 *
 * Schedule names are deterministic (`<server>-<HH:mm>`) so re-translations
 * are stable across announce cycles.
 */
class BackendScheduleSync(
    private val cache: BackendScheduleCache,
    private val scheduleService: ScheduleService,
    private val additionalDefinitions: () -> List<ScheduleDefinition> = { emptyList() },
) {

    fun start() {
        cache.subscribe { snapshot -> applyToService(snapshot) }
    }

    /** Rebuild cron registrations after proxy-side config reload. */
    fun refresh() {
        applyToService(cache.snapshot())
    }

    private fun applyToService(snapshot: Map<ServerId, BackendSchedule>) {
        scheduleService.reload(toDefinitions(snapshot) + additionalDefinitions())
    }

    private fun toDefinitions(snapshot: Map<ServerId, BackendSchedule>): List<ScheduleDefinition> {
        val defs = mutableListOf<ScheduleDefinition>()
        for ((server, schedule) in snapshot) {
            val warn = schedule.warnMinutes.coerceAtLeast(1)
            for (time in schedule.times) {
                // Cron fires `warn` minutes BEFORE the companion's local
                // shutdown so the proxy-side countdown lands on T-0 at the
                // same wall time `Bukkit.shutdown()` runs.
                val fireAt = time.minusMinutes(warn.toLong())
                defs += ScheduleDefinition(
                    name = "${server.value}-%02d:%02d".format(time.hour, time.minute),
                    target = server,
                    cronExpression = "${fireAt.minute} ${fireAt.hour} * * *",
                    warnMinutes = warn,
                    zone = schedule.zone,
                )
            }
        }
        return defs
    }
}
