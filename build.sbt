name := "hometssk3"

version := "0.1"

scalaVersion := "2.12.13"


val sparkVersion = "3.1.0"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"
val scalaTestVersion = "3.2.1"
val flinkVersion = "1.12.1"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.postgresql" % "postgresql" % "42.2.5",
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)

enablePlugins(FlywayPlugin)

flywayUrl := "jdbc:postgresql://localhost:5432/docker"
flywayUser := "docker"
flywayPassword := "docker"
flywayLocations += "db/migration"