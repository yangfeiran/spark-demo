name := "SparkTwoExperiments"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"
val oracleVersion = "12.3"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/",
  "cdh.repo" at "https://repository.cloudera.com/artifactory/cloudera-repos"
)

libraryDependencies ++= Seq(
//    "org.spark-project.hive" % "hive-jdbc" % "1.2.1.spark2",
    "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.6.1",
  "mysql" % "mysql-connector-java" % "5.1.30",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.github.scopt" %% "scopt" % "3.3.0",
  "com.typesafe"%"config"%"1.2.1"
//  "spark.jobserver" % "job-server-api_2.10" % "2.1.0" % "provided"
)
    
