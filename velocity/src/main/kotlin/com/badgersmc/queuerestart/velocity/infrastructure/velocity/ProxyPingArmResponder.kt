package com.badgersmc.queuerestart.velocity.infrastructure.velocity

import com.badgersmc.queuerestart.common.schedule.ArmEncoding
import com.badgersmc.queuerestart.common.schedule.AuthenticatedPollProtocol
import com.badgersmc.queuerestart.common.schedule.AuthenticatedPollSignal
import com.badgersmc.queuerestart.common.schedule.CompanionCapabilities
import com.badgersmc.queuerestart.common.schedule.PollSignalKind
import com.badgersmc.queuerestart.velocity.application.arm.PendingArmStore
import com.badgersmc.queuerestart.velocity.application.companion.CompanionRegistry
import com.badgersmc.queuerestart.velocity.application.ports.ClockPort
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.velocitypowered.api.event.Subscribe
import com.velocitypowered.api.event.proxy.ProxyPingEvent
import com.velocitypowered.api.proxy.server.ServerPing
import org.slf4j.Logger
import java.util.concurrent.atomic.AtomicLong

/** Authenticated heartbeat, ACK, and retryable poll-back delivery endpoint. */
class ProxyPingArmResponder(
    private val store: PendingArmStore,
    private val clock: ClockPort,
    private val logger: Logger,
    private val protocol: AuthenticatedPollProtocol,
    private val companions: CompanionRegistry,
    private val allowedServers: () -> Set<ServerId>,
) {
    private val nextInvalidWarningAtMillis = AtomicLong(0)

    @Subscribe
    fun onProxyPing(event: ProxyPingEvent) {
        val rawHost = event.connection.rawVirtualHost.orElse(null) ?: return
        if (!AuthenticatedPollProtocol.looksLikeRequest(rawHost)) return

        val request = protocol.decodeRequest(rawHost)
        if (request == null) {
            warnInvalid("signature, timestamp, or replay validation failed")
            hideSample(event)
            return
        }

        val now = clock.now()
        val serverId = ServerId(request.serverId)
        if (serverId !in allowedServers()) {
            warnInvalid("authenticated poll named unconfigured backend '${serverId.value}'")
            hideSample(event)
            return
        }

        companions.record(serverId, request.bootId, request.capabilities, now)
        val compatible = request.capabilities and CompanionCapabilities.REQUIRED == CompanionCapabilities.REQUIRED
        if (!compatible) {
            warnInvalid("backend '${serverId.value}' lacks required control capabilities")
            hideSample(event)
            return
        }

        request.acknowledgement?.let { deliveryId ->
            if (store.acknowledge(serverId, deliveryId, now)) {
                logger.info("queue-restart: companion {} acknowledged delivery {}", serverId.value, deliveryId)
            }
        }

        val rawPending = store.peekDelivery(serverId, now)
        val pending = store.peekDeliveryForBoot(serverId, request.bootId, now)
        if (rawPending != null && pending == null && rawPending.expectedBootId != request.bootId) {
            logger.info(
                "queue-restart: discarded stale delivery {} for {} after boot identity changed",
                rawPending.id,
                serverId.value,
            )
        }
        val sample = if (pending == null) {
            emptyList()
        } else {
            val signal = when (val delivery = pending.delivery) {
                PendingArmStore.Delivery.Cancel -> AuthenticatedPollSignal(
                    pending.id,
                    PollSignalKind.CANCEL,
                    "",
                )
                is PendingArmStore.Delivery.Arm -> AuthenticatedPollSignal(
                    pending.id,
                    PollSignalKind.ARM,
                    ArmEncoding.encode(delivery.value),
                )
            }
            listOf(
                ServerPing.SamplePlayer(
                    protocol.encodeSignal(serverId.value, request.nonce, signal),
                    ArmEncoding.MARKER_UUID,
                ),
            )
        }

        event.ping = event.ping.asBuilder().clearSamplePlayers().samplePlayers(sample).build()
    }

    private fun hideSample(event: ProxyPingEvent) {
        event.ping = event.ping.asBuilder().clearSamplePlayers().build()
    }

    private fun warnInvalid(reason: String) {
        val now = System.currentTimeMillis()
        val allowedAt = nextInvalidWarningAtMillis.get()
        if (now < allowedAt || !nextInvalidWarningAtMillis.compareAndSet(allowedAt, now + INVALID_WARNING_INTERVAL_MILLIS)) {
            return
        }
        logger.warn("queue-restart: rejected invalid companion poll: {}", reason)
    }

    companion object {
        private const val INVALID_WARNING_INTERVAL_MILLIS = 30_000L
    }
}
