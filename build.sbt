name := "lsh4s"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.slf4s" %% "slf4s-api" % "1.7.12",
  "org.scalanlp" %% "breeze" % "0.12",
  "org.scalikejdbc" %% "scalikejdbc" % "2.3.5",
  "com.h2database" % "h2" % "1.4.191"
)
