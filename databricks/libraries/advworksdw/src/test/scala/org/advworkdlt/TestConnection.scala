package org.advworkdlt

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => f}
import org.advworkslh.services.{AdvWorksOLAP, AdvWorksOLTP, SparkCluster}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{ResultSet, Statement}

class TestConnection extends AnyFunSuite with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def spark: SparkSession = SparkCluster.getSparkSession

  override def beforeAll(): Unit = super.beforeAll()

  test("Test connection") {

    val query = "SELECT DB_NAME() AS DBNAME"
    val OLTPDbName = AdvWorksOLTP.getQuery(spark, query).head().getAs[String]("DBNAME")

    println(s"Successfully connected to database $OLTPDbName")

    val OLAPDbName = AdvWorksOLAP.getQuery(spark, query).head().getAs[String]("DBNAME")

    println(s"Successfully connected to database $OLAPDbName")

    val conn = AdvWorksOLAP.getJDBCConnection
    val stmt: Statement = conn.createStatement();
    val rs: ResultSet = stmt.executeQuery("SELECT DB_NAME() as dbname")
    rs.next()

    println(s"Successfully connected to database ${rs.getString("dbname")} using JDBC")

    val conn2 = AdvWorksOLTP.getJDBCConnection
    val stmt2: Statement = conn2.createStatement();
    val rs2: ResultSet = stmt.executeQuery("SELECT DB_NAME() as dbname")
    rs2.next()

    println(s"Successfully connected to database ${rs2.getString("dbname")} using JDBC")

  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

}
