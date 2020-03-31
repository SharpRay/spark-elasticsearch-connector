val sparkVersion = "2.4.5"
val jodaVersion = "2.10.5"
val jacksonVersion = "2.6.5"
val jacksonModuleScalaVersion = "2.10.3"
val apacheHttpVersion = "4.5.11"

val myDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "joda-time" % "joda-time" % jodaVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonModuleScalaVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % jacksonVersion,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonVersion,
  "com.fasterxml.jackson.jaxrs" % "jackson-jaxrs-smile-provider" % jacksonVersion,
  "org.apache.httpcomponents" % "httpclient" % apacheHttpVersion
)

lazy val commonSettings = Seq(
  organization := "org.rzlabs",
  version := "0.1.0-SNAPSHOT",
  
  scalaVersion := "2.12.11"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "spark-elasticsearch-connector",
    libraryDependencies ++= myDependencies
  )

assemblyMergeStrategy in assembly := {
  case PathList("module-info.class", xs @ _*) => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
