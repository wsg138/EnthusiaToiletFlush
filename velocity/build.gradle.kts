plugins {
    `java-library`
    kotlin("jvm")
    id("com.gradleup.shadow") version "8.3.5"
}

description = "Velocity proxy plugin: countdown, drain, restart trigger, rejoin queue."

dependencies {
    api(project(":common"))

    // CTD's velocity-api is a superset of upstream — bundles the
    // com.velocitypowered.* surface plus CTD additions (queue manager,
    // cluster services). Compiling against CTD only avoids the upstream
    // jar shadowing CTD's ProxyServer additions.
    compileOnly("com.velocityctd:velocity-api:3.5.0-SNAPSHOT")
    // velocity-plugin.json is hand-authored at src/main/resources/ — Kotlin
    // doesn't run Java annotation processors without KAPT, and the metadata
    // is static enough that one JSON file beats a KAPT round-trip.

    implementation("com.cronutils:cron-utils:9.2.1")
    implementation("org.spongepowered:configurate-yaml:4.1.2")

    testImplementation(project(":common"))
    testImplementation("com.lemonappdev:konsist:0.17.3")
}

kotlin {
    jvmToolchain(21)
}

tasks.processResources {
    val pluginVersion = project.version.toString()
    inputs.property("pluginVersion", pluginVersion)
    filesMatching("velocity-plugin.json") {
        expand("version" to pluginVersion)
    }
}

val verifyPluginMetadata = tasks.register("verifyPluginMetadata") {
    dependsOn(tasks.processResources)
    doLast {
        val metadata = layout.buildDirectory.file("resources/main/velocity-plugin.json").get().asFile.readText()
        val expected = "\"version\": \"${project.version}\""
        check(expected in metadata) {
            "velocity-plugin.json version mismatch: expected $expected"
        }
        check("\${version}" !in metadata) {
            "velocity-plugin.json still contains an unresolved version placeholder"
        }
    }
}

tasks.shadowJar {
    archiveClassifier.set("")
    relocate("com.cronutils", "com.badgersmc.queuerestart.shaded.cronutils")
    relocate("org.spongepowered.configurate", "com.badgersmc.queuerestart.shaded.configurate")
}

tasks.build { dependsOn(tasks.shadowJar) }

/**
 * T-060: Konsist layer-rule check. Runs as part of `:test` (and therefore
 * `:check`) — this task gives CI a clearly-named gate for layer violations
 * separately from the rest of the unit suite.
 */
val konsistCheck = tasks.register<Test>("konsistCheck") {
    group = "verification"
    description = "Runs Konsist architecture / layer-rule tests."
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    filter { includeTestsMatching("architecture.LayerRulesTest") }
}

tasks.named("check") {
    dependsOn(konsistCheck, verifyPluginMetadata)
}
