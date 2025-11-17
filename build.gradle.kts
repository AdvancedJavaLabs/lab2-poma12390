plugins {
    java
}

allprojects {
    group = "itmo.maga.javaparallel.lab2"
    version = "1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(17))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    dependencies {
        // Логирование
        implementation("org.slf4j:slf4j-api:2.0.13")
        runtimeOnly("ch.qos.logback:logback-classic:1.5.6")

        // JSON-сериализация (для сообщений в очередях)
        implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
        implementation("com.fasterxml.jackson.module:jackson-module-parameter-names:2.17.2")
        implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.2")

        // Тесты
        testImplementation("org.junit.jupiter:junit-jupiter:5.11.0")
    }
}
