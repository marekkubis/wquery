name := "WQuery"

organization := "org.wquery"

version := "0.9"

scalaVersion := "2.10.3"

description :=
    """ WQuery is a domain-specific query language designed for WordNet-like lexical databases.
    The WQuery interpreter operates on platforms that provide Java Runtime Environment and works with wordnets stored in XML files.
    It may be used as a standalone application or as an API to a lexical database in Java-based systems."""

homepage := Some(url("http://www.wquery.org"))

startYear := Some(2007)

licenses += "WQuery License" -> url("file://LICENSE.txt")

publishMavenStyle := true

// dependiencies

libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-swing" % scalaVersion.value,
    "org.scalaz" %% "scalaz-core" % "6.0.4",
    "org.slf4j" % "slf4j-api" % "1.7.5",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.scala-stm" %% "scala-stm" % "0.7",
    "org.clapper" %% "argot" % "1.0.1" exclude("jline", "jline"),
    "org.scalatest" %% "scalatest" % "2.0" % "test",
    "org.testng" % "testng" % "6.1" % "test"
)

// assembly

val assemblyName = TaskKey[String]("assembly-name", "Creates the assembly name.")

assemblyName <<= (version) map { (v: String) => "wquery-" + v }

val assemblyFile = TaskKey[File]("assembly-file", "Creates the assembly file name.")

assemblyFile <<= (target, assemblyName) map { (t: File, n: String) => t / (n + ".zip") }

val templateFilesMappings = TaskKey[Seq[(File, String)]]("template-files-mappings", "Maps template file paths to the finals destinations.")

templateFilesMappings <<= (baseDirectory, assemblyName) map { (base, dirName) => 
    val templateDir = base / "src/main/assembly/template/"
    val templatePaths = (templateDir ** "*").get 
    templatePaths x Path.rebase(templateDir, dirName)
}

val assembly = TaskKey[File]("assembly", "Creates an assembly.")

assembly <<= (packageBin in Compile, update, templateFilesMappings, assemblyName, assemblyFile) map {
  (jar, updateReport, templateMappings, dirName, zipFile) =>
    val inputs = Seq(jar) x Path.flatRebase(dirName)
    val dependencies = 
        updateReport.select(Set("compile", "runtime")) x Path.flatRebase(dirName + "/lib")
    IO.zip(inputs ++ dependencies ++ templateMappings, zipFile)
    zipFile
}

