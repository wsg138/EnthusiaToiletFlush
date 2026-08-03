package com.badgersmc.queuerestart.paper

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

class ProcessedDeliveryStoreTest {
    @TempDir lateinit var temp: Path

    @Test
    fun `delivery ids remain consumed after reopening`() {
        val path = temp.resolve("processed.state")
        val id = UUID.randomUUID()
        assertThat(ProcessedDeliveryStore(path).markIfNew(id)).isTrue()
        assertThat(ProcessedDeliveryStore(path).markIfNew(id)).isFalse()
    }

    @Test
    fun `corrupt ledger fails closed and remains in place`() {
        val path = temp.resolve("processed.state")
        Files.writeString(path, "not-a-uuid\n")
        assertThatThrownBy { ProcessedDeliveryStore(path) }
            .isInstanceOf(IllegalStateException::class.java)
        assertThat(Files.readString(path)).isEqualTo("not-a-uuid\n")
    }

    @Test
    fun `bounded ledger evicts oldest entries deterministically`() {
        val path = temp.resolve("processed.state")
        val first = UUID.randomUUID()
        val second = UUID.randomUUID()
        val third = UUID.randomUUID()
        val store = ProcessedDeliveryStore(path, maximumEntries = 2)

        assertThat(store.markIfNew(first)).isTrue()
        assertThat(store.markIfNew(second)).isTrue()
        assertThat(store.markIfNew(third)).isTrue()

        val reopened = ProcessedDeliveryStore(path, maximumEntries = 2)
        assertThat(reopened.contains(first)).isFalse()
        assertThat(reopened.contains(second)).isTrue()
        assertThat(reopened.contains(third)).isTrue()
    }
}
