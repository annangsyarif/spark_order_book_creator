val scalaVersion = "3.3.1"
val sparkVersion = "3.5.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "OrderBookCreator",
    organization := "org.anang.assessment",
    version := "0.1.0"
  )
  .settings(
    mainClass in assembly := Some(s"${organization.value}.${name.value}")
  )

val sparkDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.10" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "3.5.0_1.4.7" % Test,
  "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "3.2.1",
  "com.typesafe" % "config" % "1.4.3",
  "org.scalameta" %% "munit" % "0.7.29" % Test,
  "org.scala-lang" % "scala-library" % scalaVersion
)

libraryDependencies ++= sparkDependencies

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)