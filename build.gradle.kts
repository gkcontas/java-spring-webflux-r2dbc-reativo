plugins {
    java
    id("org.springframework.boot") version "3.5.3"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.gkcontas"
version = "1.0.0"
description = "The same endpoints on WebFlux/R2DBC and on MVC/JDBC, measured side by side"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Both web stacks are on the classpath on purpose: the comparison has to be
    // between two stacks inside one application, not between two applications.
    // Which one starts is decided by spring.main.web-application-type, set per
    // profile. With both present Spring Boot would default to servlet, so the
    // reactive side names it explicitly instead of relying on that default.
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-web")

    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    // JDBC is not only here for the blocking side of the comparison: Flyway has
    // no R2DBC implementation, so a reactive application that versions its
    // schema keeps a blocking driver for migrations whether it wants one or not.
    implementation("org.springframework.boot:spring-boot-starter-jdbc")

    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")

    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")
    implementation("io.micrometer:micrometer-registry-prometheus")

    // Bridges reactor's Context to ThreadLocal, which is what makes MDC survive
    // a thread hop. Without it the logs of a reactive request carry no id.
    implementation("io.micrometer:context-propagation")

    runtimeOnly("org.postgresql:postgresql")
    runtimeOnly("org.postgresql:r2dbc-postgresql")

    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:r2dbc")
    testImplementation("org.awaitility:awaitility")

    // Instruments the JDK to throw when a blocking call runs on a thread that
    // must not block. Test scope only: it is a diagnostic, not a runtime
    // dependency, and it needs a JVM flag to install on Java 13+.
    testImplementation("io.projectreactor.tools:blockhound:1.0.13.RELEASE")
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("-Xlint:deprecation")
}

tasks.withType<Test> {
    useJUnitPlatform()
    // BlockHound rewrites JDK methods to add a check, and since Java 13 that
    // needs the redefinition to be allowed explicitly. Without the flag the
    // agent fails to install and the test that proves detection is a false pass.
    jvmArgs("-XX:+AllowRedefinitionToAddDeleteMethods")
}

// BlockHound instruments the JVM, not a test class, and it cannot be
// uninstalled. Sharing a JVM with the integration suite poisons it: the trap
// endpoints block on purpose, and r2dbc-postgresql blocks on the event loop
// while authenticating, so every reactive test that opens a connection fails
// with a BlockingOperationError instead of its own assertion.
//
// So the BlockHound tests get a JVM of their own. This is the trade-off the
// tool comes with, and the reason it belongs in CI as a separate job rather
// than mixed into the suite.
val blockHoundTest = tasks.register<Test>("blockHoundTest") {
    description = "Runs the BlockHound tests in an isolated JVM"
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    jvmArgs("-XX:+AllowRedefinitionToAddDeleteMethods")
    filter { includeTestsMatching("*BlockHound*") }
}

tasks.named<Test>("test") {
    filter { excludeTestsMatching("*BlockHound*") }
}

tasks.named("check") {
    dependsOn(blockHoundTest)
}
