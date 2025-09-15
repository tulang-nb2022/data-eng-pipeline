name := "data-engineering-project"
version := "0.1.0"
scalaVersion := "2.13.12"

// Comprehensive dependency overrides to ensure Scala version consistency
dependencyOverrides ++= Seq(
  "org.scala-lang" % "scala-library" % "2.13.12",
  "org.scala-lang" % "scala-reflect" % "2.13.12",
  "org.scala-lang" % "scala-compiler" % "2.13.12"
)

// Add assembly plugin
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-streaming" % "3.5.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.0",
  "org.apache.kafka" % "kafka-clients" % "3.6.0",
  "com.typesafe" % "config" % "1.4.2",
  "org.json4s" %% "json4s-jackson" % "4.0.7",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "io.delta" %% "delta-core" % "2.4.0",
  "io.delta" %% "delta-storage" % "2.4.0"
) 

resolvers ++= Seq(
  "Delta Lake Repository" at "https://repo1.maven.org/maven2/",
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/"
)