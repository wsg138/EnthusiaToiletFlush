package com.badgersmc.queuerestart.velocity

import java.nio.file.Files
import java.nio.file.Path
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** Keeps the major restart/control feature families attached to concrete regression suites. */
class FullFeatureCoverageContractTest {
    @Test
    fun `major production surfaces retain regression evidence`() {
        val root = repositoryRoot()
        coverage().forEach { (feature, evidence) ->
            evidence.forEach { relative ->
                assertTrue(
                    Files.isRegularFile(root.resolve(relative)),
                    "$feature lost required regression evidence: $relative",
                )
            }
        }
    }

    private fun coverage(): Map<String, List<String>> = linkedMapOf(
        "wire codec and arm encoding" to listOf(
            "common/src/test/java/com/badgersmc/queuerestart/common/protocol/CodecTest.kt",
            "common/src/test/java/com/badgersmc/queuerestart/common/schedule/ArmEncodingTest.kt",
            "common/src/test/java/com/badgersmc/queuerestart/common/schedule/ScheduleEncodingTest.kt",
        ),
        "authenticated poll and control security" to listOf(
            "common/src/test/java/com/badgersmc/queuerestart/common/schedule/AuthenticatedPollProtocolTest.kt",
            "common/src/test/java/com/badgersmc/queuerestart/common/security/ControlSecurityTest.kt",
        ),
        "Paper companion delivery and restart execution" to listOf(
            "paper-companion/src/test/java/com/badgersmc/queuerestart/paper/ProcessedDeliveryStoreTest.kt",
            "paper-companion/src/test/java/com/badgersmc/queuerestart/paper/RestartExecutorTest.kt",
            "paper-companion/src/test/java/com/badgersmc/queuerestart/paper/NextOccurrenceTest.kt",
        ),
        "companion heartbeat identity, freshness and capabilities" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/companion/CompanionRegistryTest.kt",
        ),
        "restart plan state safety" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/domain/plan/RestartPlanStateTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/domain/plan/RestartTimesTest.kt",
        ),
        "backend drain, hub routing and rejoin" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/drain/DrainPlannerTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/drain/HubResolverTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/drain/RejoinServiceTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/drain/PingPollerTest.kt",
        ),
        "gate/preflight behavior" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/gate/CheckGateTest.kt",
        ),
        "network restart handoff, failures and recovery" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/network/NetworkRestartServiceTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/network/NetworkRestartHandoffSafetyTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/network/NetworkRestartRecoveryRegressionTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/network/NetworkRestartTransferFailureTest.kt",
        ),
        "countdown and restart orchestration" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/schedule/CountdownBroadcasterTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/schedule/RestartOrchestratorTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/schedule/RestartDrainSettlementTest.kt",
        ),
        "schedule/admin command handling" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/schedule/ScheduleServiceTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/schedule/SchedRestartCommandHandlerTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/application/schedule/QRestartAdminCommandHandlerTest.kt",
        ),
        "coordinator/countdown/rank domain rules" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/domain/coordinator/RestartCoordinatorTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/domain/countdown/CountdownScheduleTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/domain/rank/RankLadderTest.kt",
        ),
        "configuration, persistence and scheduler adapters" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/config/ConfigurateConfigAdapterTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/persistence/AtomicRestartPlanStoreTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/schedule/CronUtilsSchedulerTest.kt",
        ),
        "Pterodactyl execution and plugin messaging" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/executor/PterodactylRestartExecutorTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/messaging/PluginMessageAdapterTest.kt",
        ),
        "proxy/queue/rendering adapters" to listOf(
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/velocity/ProxyAdapterTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/velocity/QueueAdapterTest.kt",
            "velocity/src/test/kotlin/com/badgersmc/queuerestart/velocity/infrastructure/velocity/RestartNoticeRendererTest.kt",
        ),
        "architecture boundaries" to listOf(
            "velocity/src/test/kotlin/architecture/LayerRulesTest.kt",
        ),
    )

    private fun repositoryRoot(): Path {
        val current = Path.of("").toAbsolutePath().normalize()
        if (Files.isRegularFile(current.resolve("settings.gradle.kts"))) return current
        val parent = current.parent
        if (parent != null && Files.isRegularFile(parent.resolve("settings.gradle.kts"))) return parent
        error("Could not locate EnthusiaToiletFlush repository root from $current")
    }
}
