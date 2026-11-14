plugins {
    base
    id("org.springframework.boot") version "4.1.2" apply false
    kotlin("jvm") version "2.4.21" apply false
    id("com.diffplug.spotless") version "8.0.0" apply false
}
allprojects {
    group = "dev.flowforge"
    version = "1.0.0"
    repositories { mavenCentral() }
}
subprojects {
    apply(plugin = "java-library")
    apply(plugin = "jacoco")
    apply(plugin = "com.diffplug.spotless")
    extensions.configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java { googleJavaFormat("1.28.0"); removeUnusedImports() }
        kotlin { ktfmt("0.59").kotlinlangStyle() }
    }
    extensions.configure<JavaPluginExtension> { toolchain.languageVersion.set(JavaLanguageVersion.of(21)) }
    dependencies {
        "api"(platform("org.springframework.boot:spring-boot-dependencies:4.1.2"))
        "testImplementation"("org.junit.jupiter:junit-jupiter")
        "testImplementation"("org.assertj:assertj-core")
        "testImplementation"("org.mockito:mockito-core")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging { events("passed", "skipped", "failed") }
    }
    tasks.named<Test>("test") { useJUnitPlatform { excludeTags("integration") } }
    tasks.register<Test>("integrationTest") {
        description = "Runs Docker-backed integration tests; fails if Docker is unavailable."
        testClassesDirs = project.extensions.getByType<SourceSetContainer>()["test"].output.classesDirs
        classpath = project.extensions.getByType<SourceSetContainer>()["test"].runtimeClasspath
        systemProperty("flowforge.root", rootProject.projectDir.absolutePath)
        useJUnitPlatform { includeTags("integration") }
        shouldRunAfter(tasks.named("test"))
    }
    tasks.withType<JavaCompile>().configureEach { options.compilerArgs.addAll(listOf("-Xlint:unchecked", "-parameters")) }
}
