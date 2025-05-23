name := "cursor-data-eng"
version := "0.1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.spark" %% "spark-streaming" % "3.2.0",
  "io.delta" %% "delta-core" % "1.1.0",
  "com.amazonaws" % "aws-java-sdk-athena" % "1.12.261",
  "org.json4s" %% "json4s-jackson" % "3.7.0-M11",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0"
)

// Enable Delta Lake SQL commands
sparkDependencies += "io.delta" %% "delta-core" % "1.1.0" 