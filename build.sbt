name := "concept-analysis"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += ("releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")

resolvers += ("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")

resolvers += ("Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/")
  
libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
"org.apache.spark" % "spark-mllib_2.11" % "2.2.0" % "provided","org.scalanlp" %% "breeze" % "0.12")