import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    id("org.springframework.boot") version "4.1.0" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

allprojects {
    group = "io.github.viniciusdsandrade.outbox"
    version = "0.0.1-SNAPSHOT"
    repositories { mavenCentral() }
}

configure(
    listOf(
        project(":services:orders-service"),
        project(":services:payments-service"),
        project(":services:billing-service"),
    ),
) {
    apply(plugin = "java")
    apply(plugin = "org.springframework.boot")
    apply(plugin = "io.spring.dependency-management")

    extensions.configure<JavaPluginExtension> {
        toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    }

    dependencies {
        "implementation"("org.springframework.boot:spring-boot-starter-actuator")
        "implementation"("org.springframework.boot:spring-boot-starter-data-jpa")
        "implementation"("org.springframework.boot:spring-boot-starter-flyway")
        "implementation"("org.springframework.boot:spring-boot-starter-kafka")
        "implementation"("org.springframework.boot:spring-boot-starter-webmvc")
        "implementation"("org.flywaydb:flyway-database-postgresql")
        "runtimeOnly"("org.postgresql:postgresql")

        "testImplementation"("org.springframework.boot:spring-boot-starter-actuator-test")
        "testImplementation"("org.springframework.boot:spring-boot-starter-data-jpa-test")
        "testImplementation"("org.springframework.boot:spring-boot-starter-flyway-test")
        "testImplementation"("org.springframework.boot:spring-boot-starter-kafka-test")
        "testImplementation"("org.springframework.boot:spring-boot-starter-webmvc-test")
        "testImplementation"("org.springframework.boot:spring-boot-testcontainers")
        "testImplementation"("org.testcontainers:testcontainers-junit-jupiter")
        "testImplementation"("org.testcontainers:testcontainers-postgresql")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        jvmArgs("-Xshare:off")
    }
    tasks.withType<BootJar>().configureEach {
        archiveFileName.set("${project.name}.jar")
    }
}
