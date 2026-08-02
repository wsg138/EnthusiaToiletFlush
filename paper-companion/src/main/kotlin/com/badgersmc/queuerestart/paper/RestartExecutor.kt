package com.badgersmc.queuerestart.paper

import com.badgersmc.queuerestart.common.protocol.RestartMode

/**
 * Bukkit-free abstraction over server lifecycle ops. The Paper-bound impl
 * (`BukkitServerControl`) calls `Bukkit.shutdown()`,
 * `Bukkit.dispatchCommand(consoleSender, …)`, and `System.exit(code)`.
 */
interface ServerControl {
    fun shutdown()
    fun dispatchConsoleCommand(command: String)
    fun exitProcess(code: Int)
}

/** Handle to a scheduled action. `cancel` is idempotent. */
interface ScheduledHandle {
    fun cancel()
}

/**
 * Schedules an action `delaySeconds` from now on the server's main thread.
 * The Paper-bound impl wraps `Bukkit.getScheduler().runTaskLater(…)`.
 * The default in-process implementation runs the action synchronously,
 * which keeps tests ergonomic and makes a `delaySeconds == 0` message
 * behave identically to the legacy "fire now" path.
 */
fun interface RestartScheduler {
    fun runAfterSeconds(delaySeconds: Int, action: () -> Unit): ScheduledHandle

    companion object {
        val IMMEDIATE: RestartScheduler = RestartScheduler { _, action ->
            action()
            // Action already ran synchronously; nothing to cancel.
            object : ScheduledHandle { override fun cancel() = Unit }
        }
    }
}

/**
 * REQ-021. Executes a `RestartNow` plugin message by dispatching the right
 * action against [ServerControl] after deferring by `delaySeconds`.
 *
 * Managed restarts normally arrive with `delaySeconds == 0` after Velocity
 * reaches T-0 and finishes moving or disconnecting players. Non-zero delays
 * remain supported for protocol compatibility and direct tests.
 *
 * [abort] cancels the pending shutdown, used when the proxy sends a
 * `RestartCancelMessage` (operator ran `/schedrestart cancel`). Without
 * this, the companion would still fire `Bukkit.shutdown()` after the
 * original delay despite the proxy-side cancel.
 */
class RestartExecutor(
    private val control: ServerControl,
    private val scheduler: RestartScheduler = RestartScheduler.IMMEDIATE,
) {

    @Volatile
    private var pending: ScheduledHandle? = null

    fun execute(mode: RestartMode, argument: String, delaySeconds: Int) {
        require(delaySeconds >= 0) { "delaySeconds must be ≥ 0; got $delaySeconds" }
        // Replace any prior pending shutdown so a re-arm at a different
        // delay doesn't leave two shutdown tasks racing.
        pending?.cancel()
        pending = scheduler.runAfterSeconds(delaySeconds) {
            pending = null
            when (mode) {
                RestartMode.SHUTDOWN -> control.shutdown()
                RestartMode.COMMAND -> control.dispatchConsoleCommand(argument)
                RestartMode.EXIT_CODE -> {
                    val code = argument.toIntOrNull()
                        ?: throw IllegalArgumentException(
                            "EXIT_CODE mode requires a numeric argument; got '$argument'",
                        )
                    control.exitProcess(code)
                }
            }
        }
    }

    /** Cancel the pending shutdown if one is scheduled. Idempotent. */
    fun abort(): Boolean {
        val handle = pending ?: return false
        handle.cancel()
        pending = null
        return true
    }
}
