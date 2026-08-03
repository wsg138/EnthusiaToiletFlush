package com.badgersmc.queuerestart.velocity.infrastructure.persistence

import com.badgersmc.queuerestart.velocity.application.ports.RestartPlanStore
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.plan.PlanState
import com.badgersmc.queuerestart.velocity.domain.plan.PlanType
import com.badgersmc.queuerestart.velocity.domain.plan.RestartPlan
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class AtomicRestartPlanStore(private val path: Path, private val warning: (String) -> Unit) : RestartPlanStore {
    override fun load(): List<RestartPlan> {
        if (Files.notExists(path)) return emptyList()
        return try {
            Files.readAllLines(path, StandardCharsets.UTF_8).filter(String::isNotBlank).map(::decode)
        } catch (error: Exception) {
            val backup = path.resolveSibling("${path.fileName}.corrupt-${System.currentTimeMillis()}")
            Files.move(path, backup, StandardCopyOption.REPLACE_EXISTING)
            warning("restart state was corrupt and moved to ${backup.fileName}: ${error.message}")
            emptyList()
        }
    }

    @Synchronized override fun save(plans: Collection<RestartPlan>) {
        Files.createDirectories(path.parent)
        val temporary = path.resolveSibling("${path.fileName}.tmp")
        Files.writeString(temporary, plans.joinToString("\n", postfix = if (plans.isEmpty()) "" else "\n", transform = ::encode), StandardCharsets.UTF_8)
        try {
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING)
        }
    }

    private fun encode(plan: RestartPlan): String = listOf(
        plan.id, plan.type, plan.state, plan.createdAt, plan.executionAt, plan.warningAt,
        b64(plan.creator), b64(plan.reason), b64(plan.automaticKey.orEmpty()), plan.silent,
        plan.actionStarted, plan.maintenanceEnabled, b64(plan.failure),
        b64(plan.targets.joinToString(",") { it.value }),
        b64(plan.announcedSeconds.joinToString(",")),
        b64(plan.targetResults.entries.joinToString(",") { "${it.key}=${it.value}" }),
        b64(plan.dispatchedActionKeys.joinToString(",")),
        plan.completedAt?.toString().orEmpty(),
        plan.dryRun,
    ).joinToString("|")

    private fun decode(line: String): RestartPlan {
        val p = line.split('|')
        require(p.size in 16..19) { "invalid restart state record" }
        return RestartPlan(
            id = UUID.fromString(p[0]), type = PlanType.valueOf(p[1]),
            targets = text(p[13]).split(',').filter(String::isNotBlank).map(::ServerId).toSet(),
            createdAt = Instant.parse(p[3]), executionAt = Instant.parse(p[4]), warningAt = Instant.parse(p[5]),
            reason = text(p[7]), creator = text(p[6]), automaticKey = text(p[8]).ifBlank { null },
            silent = p[9].toBoolean(), state = PlanState.valueOf(p[2]),
            announcedSeconds = ConcurrentHashMap.newKeySet<Long>().also { it += text(p[14]).split(',').filter(String::isNotBlank).map(String::toLong) },
            targetResults = ConcurrentHashMap<String, String>().also { results ->
                text(p[15]).split(',').filter { '=' in it }.forEach { results[it.substringBefore('=')] = it.substringAfter('=') }
            },
            dispatchedActionKeys = ConcurrentHashMap.newKeySet<String>().also { keys ->
                if (p.size >= 17) keys += text(p[16]).split(',').filter(String::isNotBlank)
            },
            actionStarted = p[10].toBoolean(), maintenanceEnabled = p[11].toBoolean(),
            completedAt = p.getOrNull(17)?.takeIf(String::isNotBlank)?.let(Instant::parse),
            dryRun = p.getOrNull(18)?.toBoolean() ?: false,
            failure = text(p[12]),
        )
    }

    private fun b64(value: String): String = Base64.getUrlEncoder().withoutPadding().encodeToString(value.toByteArray(StandardCharsets.UTF_8))
    private fun text(value: String): String = if (value.isBlank()) "" else String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8)
}
