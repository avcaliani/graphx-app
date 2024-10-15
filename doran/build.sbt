name := "doran"
organization := "br.avcaliani"
version := "1.0"

scalaVersion := "2.12.20"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.3" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.3" % "provided",
  "org.apache.spark" %% "spark-graphx" % "3.4.0"
)

// über JARs
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
