name := "mysql-vs-mongodb"

version := "1.0"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.reactivemongo" % "reactivemongo_2.11" % "0.12.0",
  "mysql" % "mysql-connector-java" % "6.0.5",
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "log4j" % "log4j" % "1.2.17",
  "org.scalikejdbc" %% "scalikejdbc" % "2.5.0"

)
