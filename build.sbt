name := "data-engineering-project"
version := "0.1.0"
scalaVersion := "2.12.15"
mainClass in assembly := Some("transform.DataTransformerApp")

// Add assembly plugin
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case PathList("reference.conf")           => MergeStrategy.concat
  case PathList("application.conf")         => MergeStrategy.concat
  case PathList("yarn-default.xml")         => MergeStrategy.discard
  case x if x.endsWith("yarn-default.xml")  => MergeStrategy.discard
  case x                                    => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.spark" %% "spark-streaming" % "3.2.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.0",
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "io.delta" %% "delta-core" % "1.0.0",
  "com.typesafe" % "config" % "1.4.1",
  "org.json4s" %% "json4s-jackson" % "3.7.0-M11",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  // AWS S3 support
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.12.261",
  "com.amazonaws" % "aws-java-sdk-core" % "1.12.261"
)

// Enable Delta Lake SQL commands
// sparkDependencies += "io.delta" %% "delta-core" % "1.1.0" 