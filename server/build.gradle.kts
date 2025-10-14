plugins { application }
dependencies {
    implementation(project(":core"))
    implementation(project(":storage"))
    implementation("io.undertow:undertow-core:2.3.20.Final")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.20.0")
    implementation("com.fasterxml.jackson.core:jackson-annotations:3.0-rc5")
    implementation("com.fasterxml.jackson.core:jackson-core:2.20.0")
}
application { mainClass.set("io.dynlite.server.Main") }
