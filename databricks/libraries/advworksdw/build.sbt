
version := "1.0.0"
scalaVersion := "2.12.14"
name := "advworkslh"

organization := "org.advworkslh"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"

// https://mvnrepository.com/artifact/io.delta/delta-core
libraryDependencies += "io.delta" %% "delta-core" % "2.1.0"

// https://mvnrepository.com/artifact/org.scalatest/scalatest-funsuite
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.3.0-SNAP3" % "test"
libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.3.0-SNAP3" % "test"

// https://mvnrepository.com/artifact/com.microsoft.azure/spark-mssql-connector
libraryDependencies += "com.microsoft.azure" %% "spark-mssql-connector" % "1.2.0"
