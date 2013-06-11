name := "SQSSampleScala"

version := "1.0.0"

scalaVersion := "2.10.0"

scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")

libraryDependencies += "net.databinder.dispatch" %% "dispatch-core" % "0.10.1"

libraryDependencies += "javax.xml.bind" % "jaxb-api" % "2.0"

libraryDependencies += "com.sun.xml.bind" % "jaxb-impl" % "2.0.5"

libraryDependencies += "com.sun.xml.bind" % "jaxb-xjc" % "2.0"

libraryDependencies += "commons-codec" % "commons-codec" % "1.3"

libraryDependencies += "commons-logging" % "commons-logging" % "1.1"

libraryDependencies += "commons-logging" % "commons-logging-api" % "1.1"

libraryDependencies += "log4j" % "log4j" % "1.2.13"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.10" % "2.1.4"
 
//libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

//libraryDependencies += "junit" % "junit" % "4.10" % "test"


