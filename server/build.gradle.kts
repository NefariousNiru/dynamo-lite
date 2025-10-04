plugins { application }
dependencies {
    implementation(project(":core"))
    implementation(project(":storage"))
}
application { mainClass.set("io.dynlite.server.Main") }
