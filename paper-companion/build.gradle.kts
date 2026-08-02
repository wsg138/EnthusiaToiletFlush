plugins {
    `java-library`
    id("com.gradleup.shadow") version "8.3.5"
    kotlin("jvm")
}

description = "Paper backend companion: executes restart + bridges CheckHacks events."

dependencies {
    api(project(":common"))

    compileOnly("io.papermc.paper:paper-api:1.21.4-R0.1-SNAPSHOT")

    // CheckHacks is private; the bridge subscribes via reflection so we
    // do not need its types on the compile classpath. Drop a
    // CheckHacks-fork jar into ~/.m2/repository and uncomment if you want
    // type-safe binding:
    // compileOnly("me.branduzzo:CheckHacks:1.2.0")
    implementation(kotlin("stdlib-jdk8"))
}

tasks.processResources {
    val pluginVersion = project.version.toString()
    inputs.property("pluginVersion", pluginVersion)
    filesMatching("plugin.yml") {
        expand("version" to pluginVersion)
    }
}

val verifyPluginMetadata = tasks.register("verifyPluginMetadata") {
    dependsOn(tasks.processResources)
    doLast {
        val metadata = layout.buildDirectory.file("resources/main/plugin.yml").get().asFile.readText()
        val expected = "version: ${project.version}"
        check(expected in metadata) {
            "plugin.yml version mismatch: expected $expected"
        }
        check("\${version}" !in metadata) {
            "plugin.yml still contains an unresolved version placeholder"
        }
    }
}

tasks.shadowJar {
    archiveClassifier.set("")
}

tasks.build { dependsOn(tasks.shadowJar) }
tasks.named("check") { dependsOn(verifyPluginMetadata) }

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(21)
}
