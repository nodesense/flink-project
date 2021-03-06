ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "Flink Project"

version := "0.1-SNAPSHOT"

organization := "org.example"

ThisBuild / scalaVersion := "2.12.7"

val flinkVersion = "1.11.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

// https://mvnrepository.com/artifact/org.apache.flink/flink-clients
libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.11.1"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

// assembly / mainClass := Some("org.example.Job")

assembly / mainClass := Some("org.example.FlinkCumulative")

mainClass in (Compile, packageBin) := Some("org.example.FlinkCumulative")


// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
