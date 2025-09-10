ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "CropYieldPipeline",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.knowm.xchart" %  "xchart"     % "3.8.6",
      "com.typesafe" % "config" % "1.4.3",
      "org.postgresql" % "postgresql" % "42.7.3"
    )
  )
