// in the name of ALLAH

name := "sbt-hadoop-template"

version := "0.1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  /* Hadoop's client dependency will be added automatilcaly by HadoopPlugin
   * depend on the version and as provided dependency (in runtime).
   */
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

scalacOptions ++= Seq("-deprecation","-feature")

val hadoopHome = "/home/reza/.i/tools/hadoop-2.6.3"

hadoopHomeDir in hadoop := Some(new File(hadoopHome))

// hadoopConfDir in hadoop := Some(baseDirectory.value / "hadoop-local" / "conf")

hadoopConfDir in hadoop := Some(baseDirectory.value / "hadoop-pesudo" / "conf")
