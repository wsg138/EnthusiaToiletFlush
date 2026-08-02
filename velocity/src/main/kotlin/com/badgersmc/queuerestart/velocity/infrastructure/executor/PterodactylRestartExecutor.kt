package com.badgersmc.queuerestart.velocity.infrastructure.executor

import com.badgersmc.queuerestart.velocity.application.ports.ExternalRestartExecutor
import com.badgersmc.queuerestart.velocity.application.ports.NetworkRestartConfig
import com.badgersmc.queuerestart.velocity.application.ports.PowerActionResult
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage

class PterodactylRestartExecutor(
    private val config: NetworkRestartConfig,
    private val client: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(config.connectTimeoutSeconds))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build(),
) : ExternalRestartExecutor {
    override val name: String = "PTERODACTYL"

    override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
        send(panelServerId, power = false, attempt = 0)

    override fun restart(@Suppress("UNUSED_PARAMETER") actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> {
        return send(panelServerId, power = true, attempt = 0)
    }

    private fun send(id: String, power: Boolean, attempt: Int): CompletionStage<PowerActionResult> {
        require(id.matches(Regex("[A-Za-z0-9_-]{4,64}"))) { "invalid configured panel identifier" }
        val base = URI.create(config.panelUrl)
        val uri = base.resolve("/api/client/servers/$id${if (power) "/power" else ""}")
        require(uri.host.equals(base.host, ignoreCase = true)) { "authenticated request host changed" }
        val builder = HttpRequest.newBuilder(uri)
            .timeout(Duration.ofSeconds(config.requestTimeoutSeconds))
            .header("Authorization", "Bearer ${config.apiKey}")
            .header("Accept", "Application/vnd.pterodactyl.v1+json")
        if (power) {
            builder.header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"signal\":\"restart\"}"))
        } else {
            builder.GET()
        }
        return client.sendAsync(builder.build(), HttpResponse.BodyHandlers.discarding())
            .handle { response, error ->
                if (error != null) {
                    val cause = unwrap(error)
                    // A timeout after a power request may mean Pterodactyl
                    // accepted it but its response was lost. Never retry a
                    // restart action automatically; only preflight GETs are
                    // safe to retry.
                    if (!power && attempt < config.maximumRetries && (cause is IOException || cause is HttpTimeoutException)) {
                        return@handle send(id, power, attempt + 1)
                    }
                    return@handle CompletableFuture.completedFuture(
                        PowerActionResult(false, "Pterodactyl connection failed: ${cause.javaClass.simpleName}"),
                    )
                }
                val status = response.statusCode()
                val accepted = status in 200..299
                CompletableFuture.completedFuture(
                    PowerActionResult(accepted, if (accepted) "HTTP $status accepted" else "Pterodactyl returned HTTP $status"),
                )
            }.thenCompose { it }
    }

    private fun unwrap(error: Throwable): Throwable =
        if (error is CompletionException && error.cause != null) error.cause!! else error
}

class DryRunRestartExecutor : ExternalRestartExecutor {
    override val name: String = "DRY_RUN"
    override val performsPowerActions: Boolean = false
    override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
        CompletableFuture.completedFuture(PowerActionResult(true, "dry-run preflight"))
    override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
        CompletableFuture.completedFuture(PowerActionResult(true, "dry-run restart accepted"))
}

/**
 * Rebuilds the external client only after a successful config reload changes
 * executor settings. This keeps `/qrestart reload` meaningful without
 * retaining an API key in logs or plugin state outside the active client.
 */
class ConfiguredRestartExecutor(
    private val configSupplier: () -> NetworkRestartConfig,
) : ExternalRestartExecutor {
    private var currentConfig = configSupplier()
    private var current: ExternalRestartExecutor = create(currentConfig)

    override val name: String get() = delegate().name
    override val performsPowerActions: Boolean get() = delegate().performsPowerActions

    override fun preflight(panelServerId: String): CompletionStage<PowerActionResult> =
        delegate().preflight(panelServerId)

    override fun restart(actionKey: String, panelServerId: String): CompletionStage<PowerActionResult> =
        delegate().restart(actionKey, panelServerId)

    @Synchronized private fun delegate(): ExternalRestartExecutor {
        val updated = configSupplier()
        if (updated != currentConfig) {
            currentConfig = updated
            current = create(updated)
        }
        return current
    }

    private fun create(config: NetworkRestartConfig): ExternalRestartExecutor =
        if (config.enabled && config.executorType == "PTERODACTYL") PterodactylRestartExecutor(config) else DryRunRestartExecutor()
}
