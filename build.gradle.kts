import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.api.plugins.JavaPluginExtension

plugins {
    // no root plugins here
    id("com.google.protobuf") version "0.9.4" apply false
}

allprojects {
    repositories { mavenCentral() }
}

subprojects {
    // apply Java plugin to all subprojects
    pluginManager.apply("java")

    // configure the Java extension without the 'java { }' accessor
    extensions.configure<JavaPluginExtension> {
        toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
    }

    // use string-based configuration names since accessors aren’t generated here
    dependencies {
        add("testImplementation", platform("org.junit:junit-bom:5.11.3"))
        add("testImplementation", "org.junit.jupiter:junit-jupiter")
        add("testRuntimeOnly", "org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }
}
