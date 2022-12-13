import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
    java
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("org.apache.kafka:kafka-clients:2.6.3")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.9.6")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.9.6")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.9.6")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

application {
    mainClass.set("MainKt")
}