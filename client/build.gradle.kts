plugins { application }
dependencies { implementation(project(":core")) }
application { mainClass.set("io.dynlite.client.Cli") }
