package com.badgersmc.queuerestart.velocity.application.schedule

import com.badgersmc.queuerestart.velocity.application.ports.ProxyRestartScheduleConfig

/**
 * Translate configured proxy shutdown times into cron entries that fire at
 * countdown start. Subtracting [ProxyRestartScheduleConfig.warnMinutes]
 * keeps T-0 aligned with the configured restart time, including midnight
 * wrap-around.
 */
fun proxyScheduleDefinitions(cfg: ProxyRestartScheduleConfig): List<ScheduleDefinition> =
    cfg.restartTimes.map { restartAt ->
        val countdownAt = restartAt.minusMinutes(cfg.warnMinutes.toLong())
        ScheduleDefinition(
            name = "proxy-%02d:%02d".format(restartAt.hour, restartAt.minute),
            target = ProxyRestartService.TARGET,
            cronExpression = "${countdownAt.minute} ${countdownAt.hour} * * *",
            warnMinutes = cfg.warnMinutes,
            zone = cfg.zone,
        )
    }
