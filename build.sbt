name := "data-engineering-project"
version := "0.1.0"
scalaVersion := "2.13.12"

// Comprehensive dependency overrides to ensure Scala version consistency
dependencyOverrides ++= Seq(
  "org.scala-lang" % "scala-library" % "2.13.12",
  "org.scala-lang" % "scala-reflect" % "2.13.12",
  "org.scala-lang" % "scala-compiler" % "2.13.12"
)

// Add assembly plugin - Spark/Kafka dependencies marked as "provided"
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.5.0" % "provided", 
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0" % "provided",
  "org.apache.kafka" % "kafka-clients" % "3.5.1" % "provided",
  "com.typesafe" % "config" % "1.4.2",
  "org.json4s" %% "json4s-jackson" % "4.0.7",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  // Delta Lake with compatible version for Spark 3.5.0  
  "io.delta" %% "delta-core" % "2.4.0" % "provided"
) 

resolvers ++= Seq(
  Resolver.mavenCentral,
  "Maven Central" at "https://repo1.maven.org/maven2/"
)

// Configure for Delta Lake
fork := true
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)