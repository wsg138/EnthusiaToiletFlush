package com.badgersmc.queuerestart.paper

import java.nio.charset.StandardCharsets
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.LinkedHashSet
import java.util.UUID

/** Persistent bounded idempotency ledger shared by plugin-message and SLP delivery. */
class ProcessedDeliveryStore(
    private val path: Path? = null,
    private val maximumEntries: Int = 512,
) {
    private val processed = LinkedHashSet<UUID>()

    init {
        require(maximumEntries > 0) { "maximumEntries must be positive" }
        load()
    }

    @Synchronized
    fun markIfNew(deliveryId: UUID): Boolean {
        if (!processed.add(deliveryId)) return false
        trim()
        try {
            save()
        } catch (error: Throwable) {
            processed.remove(deliveryId)
            throw error
        }
        return true
    }

    @Synchronized
    fun remove(deliveryId: UUID) {
        if (processed.remove(deliveryId)) save()
    }

    @Synchronized
    fun contains(deliveryId: UUID): Boolean = deliveryId in processed

    private fun load() {
        val file = path ?: return
        if (Files.notExists(file)) return
        try {
            Files.readAllLines(file, StandardCharsets.UTF_8)
                .filter(String::isNotBlank)
                .forEach { line ->
                    processed += UUID.fromString(line.trim())
                }
            trim()
        } catch (error: Throwable) {
            throw IllegalStateException(
                "processed delivery state is corrupt; refusing to enable to prevent replayed restarts: $file",
                error,
            )
        }
    }

    private fun trim() {
        while (processed.size > maximumEntries) {
            val iterator = processed.iterator()
            iterator.next()
            iterator.remove()
        }
    }

    private fun save() {
        val file = path ?: return
        file.parent?.let(Files::createDirectories)
        val temporary = file.resolveSibling("${file.fileName}.tmp")
        Files.writeString(
            temporary,
            processed.joinToString("\n", postfix = if (processed.isEmpty()) "" else "\n"),
            StandardCharsets.UTF_8,
        )
        try {
            Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
        } catch (_: AtomicMoveNotSupportedException) {
            Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING)
        }
    }
}
