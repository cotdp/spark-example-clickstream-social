import AssemblyKeys._

name := "spark-example-clickstream-social"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    //
	// Spark/Hadoop provided dependencies
	"org.apache.spark" %% "spark-assembly" % "1.1.0" % "provided",
	"org.apache.hadoop" % "hadoop-client" % "2.3.0" % "provided",
	"org.json4s" %% "json4s-jackson" % "3.2.10" % "provided",
	// Other dependencies
	"joda-time" % "joda-time" % "2.3"
)

assemblySettings

jarName in assembly := "spark-example-clickstream-social_2.10-1.0.jar"

assemblyOption in assembly ~= { _.copy(includeScala = false) }
