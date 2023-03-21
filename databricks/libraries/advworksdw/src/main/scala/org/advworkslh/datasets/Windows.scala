package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, SQLServerDatabase}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import java.sql.Timestamp

object Windows extends BaseTable {

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Staging.Windows"
  override val idColumns: Array[String] = Array("Window")

  def registerWindow(spark: SparkSession, window: Int, fromTm: Timestamp, toTm: Timestamp): Unit = {

    val rows = List[Row](Row(window, fromTm, toTm)).asJava
    val schema: StructType =  AdvWorksOLAP.getQuery(spark, "SELECT TOP 1 * FROM Staging.Windows").schema
    val dfToInsert = spark.createDataFrame(rows, schema)

    AdvWorksOLAP.appendToTable(dfToInsert, "Staging.Windows")
  }

  def getWindowInfo(spark: SparkSession, window: Int): (Timestamp, Timestamp) = {

    val row = AdvWorksOLAP.getTable(spark, "Staging.Windows").filter(s"window = $window").head()
    val fromTm = row.getAs[Timestamp]("FromTm")
    val toTm   = row.getAs[Timestamp]("ToTm")
    (fromTm, toTm)
  }

  def getMostCurrentWindowInfo(spark: SparkSession): (Int, Timestamp, Timestamp) = {
    val maxWindowNb =
      AdvWorksOLAP.getTable(spark, "Staging.Windows").
        selectExpr("Max(Window) as MaxWindow").
        head().
        getAs[Int]("MaxWindow")

    val (fromTm, toTm) = Windows.getWindowInfo(spark, maxWindowNb)
    (maxWindowNb, fromTm, toTm)
  }

}
