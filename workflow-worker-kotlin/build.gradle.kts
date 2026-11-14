plugins {
    id("org.springframework.boot")
    kotlin("jvm")
}
kotlin { jvmToolchain(21) }
dependencies {
    implementation(project(":worker-runtime"))
    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))
}
