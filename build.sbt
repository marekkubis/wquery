import com.typesafe.sbt.SbtGhPages.ghpages
import com.typesafe.sbt.SbtGit.git
import com.typesafe.sbt.SbtSite.SiteKeys._
import com.typesafe.sbt.SbtSite.site
import de.johoop.jacoco4sbt.JacocoPlugin._
import sbtfilter.Plugin.FilterKeys._

//
// Project information
//
name := "WQuery"

organization := "org.wquery"

version := "0.8-SNAPSHOT"

scalaVersion := "2.10.4"

description :=
    """WQuery is a domain-specific query language designed for WordNet-like lexical databases.
    The WQuery interpreter operates on platforms that provide Java Runtime Environment and works with wordnets stored in XML files.
    It may be used as a standalone application or as an API to a lexical database in Java-based systems."""

homepage := Some(url("http://www.wquery.org"))

startYear := Some(2007)

licenses += "WQuery License" -> url("file://LICENSE.txt.txt")

publishMavenStyle := true

//
// Dependiencies
//
libraryDependencies ++= Seq(
    "org.scalaz" %% "scalaz-core" % "6.0.4",
    "org.slf4j" % "slf4j-api" % "1.7.5",
    "ch.qos.logback" % "logback-classic" % "1.0.13",
    "org.scala-stm" %% "scala-stm" % "0.7",
    "org.rogach" %% "scallop" % "0.9.5",
    "jline" % "jline" % "2.12",
    "com.twitter" %% "chill" % "0.4.0",
    "org.scalatest" %% "scalatest" % "2.0" % "test",
    "org.testng" % "testng" % "6.1" % "test"
)

//
// Resource filters
//
sbtfilter.Plugin.filterSettings

includeFilter in (Compile, filterResources) ~= { f => f || ("wconsole" | "wguiconsole") }

extraProps += "startYear" -> startYear.value.get.toString

extraProps += "currentYear" -> new java.text.SimpleDateFormat("yyyy").format(new java.util.Date())

//
// Assembly
//
val assemblyName = TaskKey[String]("assembly-name", "Creates the assembly name.")

assemblyName <<= version map { (v: String) => "wquery-" + v }

val assemblyFile = TaskKey[File]("assembly-file", "Creates the assembly file name.")

assemblyFile <<= (target, assemblyName) map { (t: File, n: String) => t / (n + ".zip") }

val templateFilesMappings = TaskKey[Seq[(File, String)]]("template-files-mappings", "Maps template file paths to the final destinations.")

templateFilesMappings <<= (baseDirectory, assemblyName) map { (base, dirName) => 
    val templateDir = base / "src/main/assembly/template/"
    val templatePaths = (templateDir ** "*").get 
    templatePaths x Path.rebase(templateDir, dirName)
}

val infoFilesMappings = TaskKey[Seq[(File, String)]]("info-files-mappings", "Maps README.md, ChangeLog, etc. file paths to the final destinations.")

infoFilesMappings <<= (baseDirectory, assemblyName) map { (base, dirName) => 
    val infoPaths = Seq(base / "README.md", base / "LICENSE.txt", base / "ChangeLog")
    infoPaths x Path.rebase(base, dirName)
}

val assembly = TaskKey[File]("assembly", "Creates an assembly.")

assembly <<= (packageBin in Compile, update, infoFilesMappings, templateFilesMappings, assemblyName, assemblyFile) map {
  (jar, updateReport, infoMappings, templateMappings, dirName, zipFile) =>
    val inputs = Seq(jar) x Path.flatRebase(dirName + "/lib")
    val dependencies = 
        updateReport.select(Set("compile", "runtime")) x Path.flatRebase(dirName + "/lib")
    IO.zip(inputs ++ dependencies ++ infoMappings ++ templateMappings, zipFile)
    zipFile
}

//
// Scalastyle
//
org.scalastyle.sbt.ScalastylePlugin.Settings

//
// Coverage
//
jacoco.settings

//
// Site
//
site.settings

includeFilter in makeSite ~= { f => f || ("CNAME" | "*.pdf") }

ghpages.settings

git.remoteRepo := "git@github.com:marekkubis/wquery.git"
