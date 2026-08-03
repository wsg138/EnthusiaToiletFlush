package com.badgersmc.queuerestart.velocity.infrastructure.persistence

import com.badgersmc.queuerestart.velocity.application.ports.RestartPlanStore
import com.badgersmc.queuerestart.velocity.domain.id.ServerId
import com.badgersmc.queuerestart.velocity.domain.plan.PlanState
import com.badgersmc.queuerestart.velocity.domain.plan.PlanType
import com.badgersmc.queuerestart.velocity.domain.plan.RestartPlan
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.util.Base64
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class AtomicRestartPlanStore(
    private val path: Path,
    private val warning: (String) -> Unit,
) : RestartPlanStore {
    override fun load(): List<RestartPlan> {
        if (Files.notExists(path)) return emptyList()
        return try {
            require(Files.size(path) <= MAX_STATE_BYTES) { "restart state file exceeds safety limit" }
            val decoded = Files.readAllLines(path, StandardCharsets.UTF_8)
                .filter(String::isNotBlank)
                .also { require(it.size <= MAX_PLAN_COUNT) { "restart state contains too many plans" } }
                .also { lines -> require(lines.all { it.length <= MAX_RECORD_CHARS }) { "restart state record exceeds safety limit" } }
                .map(::decode)
            require(decoded.map(RestartPlan::id).distinct().size == decoded.size) {
                "restart state contains duplicate plan ids"
            }
            decoded
        } catch (error: Exception) {
            val backup = path.resolveSibling("${path.fileName}.corrupt-${System.currentTimeMillis()}")
            val copied = runCatching {
                Files.copy(path, backup, StandardCopyOption.REPLACE_EXISTING)
                true
            }.getOrDefault(false)
            warning(
                if (copied) {
                    "restart state was corrupt; a diagnostic copy was written to ${backup.fileName}: ${error.message}"
                } else {
                    "restart state was corrupt and could not be copied for diagnostics: ${error.message}"
                },
            )
            throw IllegalStateException(
                "restart state is corrupt; refusing to start because destructive actions may require reconciliation. " +
                    "The original state file was intentionally left in place so a second start cannot bypass this failure.",
                error,
            )
        }
    }

    @Synchronized
    override fun save(plans: Collection<RestartPlan>) {
        val parent = path.parent
        parent?.let(Files::createDirectories)
        val temporary = path.resolveSibling("${path.fileName}.tmp")
        val content = plans.sortedBy { it.id.toString() }
            .joinToString("\n", postfix = if (plans.isEmpty()) "" else "\n", transform = ::encode)
        FileChannel.open(
            temporary,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE,
        ).use { channel ->
            val bytes = ByteBuffer.wrap(content.toByteArray(StandardCharsets.UTF_8))
            while (bytes.hasRemaining()) channel.write(bytes)
            channel.force(true)
        }
        try {
            Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING)
        }
        if (parent != null) {
            FileChannel.open(parent, StandardOpenOption.READ).use { channel ->
                channel.force(true)
            }
        }
    }

    private fun encode(plan: RestartPlan): String = listOf(
        plan.id,
        plan.type,
        plan.state,
        plan.createdAt,
        plan.executionAt,
        plan.warningAt,
        b64(plan.creator),
        b64(plan.reason),
        b64(plan.automaticKey.orEmpty()),
        plan.silent,
        plan.actionStarted,
        plan.maintenanceEnabled,
        b64(plan.failure),
        b64(plan.targets.sortedBy(ServerId::value).joinToString(",") { it.value }),
        b64(plan.announcedSeconds.sorted().joinToString(",")),
        b64(encodeStringMap(plan.targetResults)),
        b64(plan.dispatchedActionKeys.sorted().joinToString(",")),
        plan.completedAt?.toString().orEmpty(),
        plan.dryRun,
        plan.backendArmAccepted,
        plan.lastObservedRemainingSeconds?.toString().orEmpty(),
        b64(plan.acceptedActionKeys.sorted().joinToString(",")),
        b64(encodeBootMap(plan.baselineBootIds)),
        plan.proxyBaselineBootId?.toString().orEmpty(),
        plan.executionDeadlineAt?.toString().orEmpty(),
    ).joinToString("|")

    private fun decode(line: String): RestartPlan {
        val p = line.split('|')
        require(p.size >= LEGACY_FIELD_COUNT) { "invalid restart state record" }
        return RestartPlan(
            id = UUID.fromString(p[0]),
            type = PlanType.valueOf(p[1]),
            targets = text(p[13]).split(',').filter(String::isNotBlank).map(::ServerId).toSet(),
            createdAt = Instant.parse(p[3]),
            executionAt = Instant.parse(p[4]),
            warningAt = Instant.parse(p[5]),
            reason = text(p[7]),
            creator = text(p[6]),
            automaticKey = text(p[8]).ifBlank { null },
            silent = p[9].toBooleanStrict(),
            state = PlanState.valueOf(p[2]),
            announcedSeconds = ConcurrentHashMap.newKeySet<Long>().also { marks ->
                marks += text(p[14]).split(',').filter(String::isNotBlank).map(String::toLong)
            },
            targetResults = ConcurrentHashMap<String, String>().also { results ->
                results += decodeStringMap(text(p[15]))
            },
            dispatchedActionKeys = ConcurrentHashMap.newKeySet<String>().also { keys ->
                p.getOrNull(16)?.let(::text)?.split(',')?.filter(String::isNotBlank)?.let(keys::addAll)
            },
            acceptedActionKeys = ConcurrentHashMap.newKeySet<String>().also { keys ->
                p.getOrNull(21)?.let(::text)?.split(',')?.filter(String::isNotBlank)?.let(keys::addAll)
            },
            actionStarted = p[10].toBooleanStrict(),
            maintenanceEnabled = p[11].toBooleanStrict(),
            completedAt = p.getOrNull(17)?.takeIf(String::isNotBlank)?.let(Instant::parse),
            dryRun = p.getOrNull(18)?.toBooleanStrictOrNull() ?: false,
            backendArmAccepted = p.getOrNull(19)?.toBooleanStrictOrNull() ?: false,
            lastObservedRemainingSeconds = p.getOrNull(20)?.takeIf(String::isNotBlank)?.toLong(),
            baselineBootIds = ConcurrentHashMap<ServerId, UUID>().also { map ->
                p.getOrNull(22)?.let(::text)?.let(::decodeBootMap)?.let(map::putAll)
            },
            proxyBaselineBootId = p.getOrNull(23)?.takeIf(String::isNotBlank)?.let(UUID::fromString),
            executionDeadlineAt = p.getOrNull(24)?.takeIf(String::isNotBlank)?.let(Instant::parse),
            failure = text(p[12]),
        )
    }

    private fun encodeStringMap(values: Map<String, String>): String =
        "v2:" + values.entries.sortedBy(Map.Entry<String, String>::key).joinToString(",") {
            "${b64(it.key)}=${b64(it.value)}"
        }

    private fun decodeStringMap(value: String): Map<String, String> {
        if (value.startsWith("v2:")) {
            val entries = value.removePrefix("v2:")
                .split(',')
                .filter(String::isNotBlank)
                .map { encoded ->
                    require(encoded.count { it == '=' } == 1) { "invalid encoded result entry" }
                    text(encoded.substringBefore('=')) to text(encoded.substringAfter('='))
                }
            require(entries.map(Pair<String, String>::first).distinct().size == entries.size) {
                "duplicate result key"
            }
            return entries.toMap()
        }
        // Backward compatibility with the original comma/equals representation.
        val entries = value.split(',').filter(String::isNotBlank).map {
            require('=' in it) { "invalid legacy result entry" }
            it.substringBefore('=') to it.substringAfter('=')
        }
        require(entries.map(Pair<String, String>::first).distinct().size == entries.size) { "duplicate result key" }
        return entries.toMap()
    }

    private fun encodeBootMap(values: Map<ServerId, UUID>): String =
        "v2:" + values.entries.sortedBy { it.key.value }.joinToString(",") {
            "${b64(it.key.value)}=${b64(it.value.toString())}"
        }

    private fun decodeBootMap(value: String): Map<ServerId, UUID> {
        val encoded = value.startsWith("v2:")
        val entries = value.removePrefix("v2:")
            .split(',')
            .filter(String::isNotBlank)
            .map { entry ->
                require(entry.count { it == '=' } == 1) { "invalid boot identity entry" }
                if (encoded) {
                    ServerId(text(entry.substringBefore('='))) to UUID.fromString(text(entry.substringAfter('=')))
                } else {
                    ServerId(entry.substringBefore('=')) to UUID.fromString(entry.substringAfter('='))
                }
            }
        require(entries.map(Pair<ServerId, UUID>::first).distinct().size == entries.size) {
            "duplicate backend boot identity"
        }
        return entries.toMap()
    }

    private fun b64(value: String): String = Base64.getUrlEncoder().withoutPadding()
        .encodeToString(value.toByteArray(StandardCharsets.UTF_8))

    private fun text(value: String): String = if (value.isBlank()) {
        ""
    } else {
        String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8)
    }

    companion object {
        private const val LEGACY_FIELD_COUNT = 16
        private const val MAX_PLAN_COUNT = 10_000
        private const val MAX_RECORD_CHARS = 256_000
        private const val MAX_STATE_BYTES = 32L * 1024L * 1024L
    }
}
