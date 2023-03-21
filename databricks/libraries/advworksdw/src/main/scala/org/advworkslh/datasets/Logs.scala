package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, SQLServerDatabase}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

object Logs extends BaseTable {

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Staging.Logs"
  override val idColumns: Array[String] = Array("window", "table")

  def registerLog(spark: SparkSession, window: Int, table: String): Unit = {
    val rows = List[Row](Row(window, table)).asJava
    val schema: StructType = getSchema(spark)
    val dfToInsert = spark.createDataFrame(rows, schema)
    appendToTable(dfToInsert)
  }

}
