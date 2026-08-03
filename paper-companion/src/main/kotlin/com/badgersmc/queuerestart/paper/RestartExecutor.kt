package com.badgersmc.queuerestart.paper

import com.badgersmc.queuerestart.common.protocol.RestartMode
import java.util.UUID

/**
 * Bukkit-free abstraction over server lifecycle ops. The Paper-bound impl
 * (`BukkitServerControl`) calls only `Bukkit.shutdown()`.
 */
interface ServerControl {
    fun shutdown()
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
    private val processedDeliveries: ProcessedDeliveryStore = ProcessedDeliveryStore(),
) {

    @Volatile
    private var pending: ScheduledHandle? = null

    @Synchronized
    fun execute(deliveryId: UUID, mode: RestartMode, argument: String, delaySeconds: Int): Boolean {
        require(delaySeconds >= 0) { "delaySeconds must be ≥ 0; got $delaySeconds" }
        require(mode == RestartMode.SHUTDOWN) { "only SHUTDOWN restart delivery is supported" }
        require(argument.isEmpty()) { "SHUTDOWN restart delivery must not contain an argument" }
        if (!processedDeliveries.markIfNew(deliveryId)) return false
        try {
            // Replace any prior pending shutdown so a re-arm at a different
            // delay doesn't leave two shutdown tasks racing.
            pending?.cancel()
            var completedSynchronously = false
            val handle = scheduler.runAfterSeconds(delaySeconds) {
                synchronized(this) {
                    completedSynchronously = true
                    pending = null
                }
                control.shutdown()
            }
            // A test/in-process scheduler may execute the callback before
            // runAfterSeconds returns. Do not resurrect that completed task as
            // a cancellable pending shutdown after the callback cleared it.
            pending = if (completedSynchronously) null else handle
        } catch (error: Throwable) {
            processedDeliveries.remove(deliveryId)
            throw error
        }
        return true
    }

    /**
     * Durably consumes a cancellation delivery before aborting the current task.
     * This makes captured signed cancellation frames idempotent across plugin/JVM restarts.
     */
    @Synchronized
    fun abort(deliveryId: UUID): Boolean {
        if (!processedDeliveries.markIfNew(deliveryId)) return false
        val handle = pending
        handle?.cancel()
        pending = null
        return handle != null
    }
}
