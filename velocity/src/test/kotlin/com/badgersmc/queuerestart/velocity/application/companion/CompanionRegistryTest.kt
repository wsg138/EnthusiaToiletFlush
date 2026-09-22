package com.badgersmc.queuerestart.velocity.application.companion

import com.badgersmc.queuerestart.common.schedule.CompanionCapabilities
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import java.time.Duration
import java.time.Instant
import java.util.UUID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CompanionRegistryTest {
    private val server = ServerId("survival")
    private val boot = UUID.fromString("11111111-2222-3333-4444-555555555555")
    private val now = Instant.parse("2026-01-01T00:10:00Z")
    private val timeout = Duration.ofSeconds(30)

    @Test
    fun `missing or incomplete heartbeat is never compatible`() {
        val registry = CompanionRegistry()
        assertNull(registry.compatibleHeartbeat(server, now, timeout))
        assertFalse(registry.isCompatible(server, now, timeout))

        registry.record(
            server,
            boot,
            CompanionCapabilities.RESTART_SHUTDOWN or CompanionCapabilities.BOOT_IDENTITY,
            now,
        )

        assertNull(registry.compatibleHeartbeat(server, now, timeout))
        assertFalse(registry.isCompatible(server, now, timeout))
    }

    @Test
    fun `required capabilities at the freshness boundary are accepted`() {
        val registry = CompanionRegistry()
        val seen = now.minus(timeout)
        registry.record(server, boot, CompanionCapabilities.REQUIRED, seen)

        val heartbeat = registry.compatibleHeartbeat(server, now, timeout)
        assertEquals(boot, heartbeat?.bootId)
        assertEquals(CompanionCapabilities.REQUIRED, heartbeat?.capabilities)
        assertEquals(seen, heartbeat?.lastSeen)
        assertTrue(registry.isCompatible(server, now, timeout))
    }

    @Test
    fun `heartbeat older than timeout is rejected`() {
        val registry = CompanionRegistry()
        registry.record(server, boot, CompanionCapabilities.REQUIRED, now.minusSeconds(31))

        assertNull(registry.compatibleHeartbeat(server, now, timeout))
        assertFalse(registry.isCompatible(server, now, timeout))
    }

    @Test
    fun `extra future capability bits do not invalidate the required contract`() {
        val registry = CompanionRegistry()
        val extraCapability = 1 shl 10
        registry.record(server, boot, CompanionCapabilities.REQUIRED or extraCapability, now)

        assertTrue(registry.isCompatible(server, now, timeout))
    }

    @Test
    fun `new authenticated heartbeat replaces the previous boot identity and freshness`() {
        val registry = CompanionRegistry()
        val newBoot = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        registry.record(server, boot, CompanionCapabilities.REQUIRED, now.minusSeconds(20))
        registry.record(server, newBoot, CompanionCapabilities.REQUIRED, now)

        val heartbeat = registry.heartbeat(server)
        assertEquals(newBoot, heartbeat?.bootId)
        assertEquals(now, heartbeat?.lastSeen)
    }
}
