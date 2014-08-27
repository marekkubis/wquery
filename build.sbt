import com.typesafe.sbt.SbtGhPages.ghpages
import com.typesafe.sbt.SbtGit.git
import com.typesafe.sbt.SbtNativePackager._
import com.typesafe.sbt.SbtSite.SiteKeys._
import com.typesafe.sbt.SbtSite.site
import com.typesafe.sbt.packager.universal
import de.johoop.jacoco4sbt.JacocoPlugin._
import sbtfilter.Plugin.FilterKeys._
import sbtrelease._

//
// Project information
//
name := "WQuery"

organization := "org.wquery"

scalaVersion := "2.10.4"

description :=
    """WQuery is a domain-specific query language designed for WordNet-like lexical databases.
    The WQuery interpreter operates on platforms that provide Java Runtime Environment and works with wordnets stored in XML files.
    It may be used as a standalone application or as an API to a lexical database in Java-based systems."""

homepage := Some(url("http://www.wquery.org"))

startYear := Some(2007)

licenses += "WQuery License" -> url("file://LICENSE")

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
// Native Packager
//
packageArchetype.java_application

val infoFilesMappings = TaskKey[Seq[(File, String)]]("info-files-mappings", "Maps README.md, ChangeLog, etc. file paths to the final destinations.")

infoFilesMappings <<= baseDirectory map { base =>
    Seq(base / "README.md" -> "README",
      base / "LICENSE" -> "LICENSE",
      base / "ChangeLog" -> "ChangeLog")
}

mappings in Universal ++= infoFilesMappings.value

val assemblyFileName = TaskKey[File]("assembly-file", "Creates the assembly file name.")

assemblyFileName <<= (target, version, ReleaseKeys.releaseVersion) map {
  (t: File, v: String, r: String => String) =>
    t / ("wquery-" + r(v) + ".tar.gz")
}

val assembly = TaskKey[File]("assembly", "Creates an assembly.")

assembly <<= (universal.Keys.packageZipTarball in Universal, assemblyFileName) map {
  (packagedFile, assemblyFile) =>
    IO.move(packagedFile, assemblyFile)
    assemblyFile
}

//
// Sonatype Deployment
//
sonatypeSettings

pomExtra := {
  <url>http://www.wquery.org</url>
    <licenses>
      <license>
        <name>BSD-style</name>
        <url>http://raw.githubusercontent.com/marekkubis/wquery/master/LICENSE</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/marekkubis/wquery.git</connection>
      <url>github.com/marekkubis/wquery.git</url>
    </scm>
    <developers>
      <developer>
        <id>marekkubis</id>
        <name>Marek Kubis</name>
        <url>http://marekkubis.com</url>
      </developer>
    </developers>
}

//
// Release
//
releaseSettings

ReleaseKeys.releaseProcess <<= thisProjectRef apply { ref =>
  import sbtrelease.ReleaseStateTransformations._
  Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    releaseTask(assembly in ThisBuild in ref),
//    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    pushChanges
  )
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
