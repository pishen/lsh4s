lazy val root = (project in file("."))
  .settings(
    name := "lsh4s",
    version := "0.6.0",
    scalaVersion := "2.11.8",
    crossScalaVersions := Seq("2.10.6", "2.11.8"),
    libraryDependencies ++= Seq(
      "org.slf4s" %% "slf4s-api" % "1.7.12",
      "org.mapdb" % "mapdb" % "2.0-beta13",
      "org.scalanlp" %% "breeze" % "0.11.2",
      "com.github.pathikrit" %% "better-files" % "2.14.0" % "test",
      "org.scalatest" %% "scalatest" % "2.2.6" % "test",
      "org.slf4j" % "slf4j-simple" % "1.7.14" % "test"
    ),
    organization := "net.pishen",
    licenses += ("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    homepage := Some(url("https://github.com/pishen/lsh4s")),
    pomExtra := (
      <scm>
        <url>https://github.com/pishen/lsh4s.git</url>
        <connection>scm:git:git@github.com:pishen/lsh4s.git</connection>
      </scm>
      <developers>
        <developer>
          <id>pishen</id>
          <name>Pishen Tsai</name>
        </developer>
      </developers>
    )
  )
