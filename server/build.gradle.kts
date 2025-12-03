import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    application
    id("com.google.protobuf") version "0.9.4"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.25.3"
    }

    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.65.1"
        }
    }

    generateProtoTasks {
        all().configureEach {
            plugins {
                create("grpc")
            }
        }
    }
}


application {
    mainClass.set("io.dynlite.server.Main")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":storage"))

    implementation("io.undertow:undertow-core:2.3.20.Final")

    // Jackson
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    implementation("com.fasterxml.jackson.core:jackson-core:2.18.2")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.18.2")
    implementation("javax.annotation:javax.annotation-api:1.3.2")

    // gRPC
    implementation("io.grpc:grpc-netty-shaded:1.65.1")
    implementation("io.grpc:grpc-protobuf:1.65.1")
    implementation("io.grpc:grpc-stub:1.65.1")
    implementation("com.google.protobuf:protobuf-java:3.25.3")

    testImplementation("io.grpc:grpc-inprocess:1.65.1")
}


tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "io.dynlite.server.Main"
    }
}

// Fat jar: server-all.jar
tasks.named<ShadowJar>("shadowJar") {
    archiveBaseName.set("server")
    archiveClassifier.set("all")
    mergeServiceFiles()
}
