package org.advworkslh.datasets

import org.advworkslh.services.SQLServerDatabase
import org.apache.spark.sql.{DataFrame, SparkSession, functions => f}
import org.apache.spark.sql.types._

trait BaseTable {

  val database: SQLServerDatabase
  val table: String

  val idColumns: Array[String]

  def fullTableName: String = s"${database.name}.$table"

  def getDf(spark: SparkSession): DataFrame =
    database.getTable(spark, table)

  def getSchema(spark: SparkSession): StructType =
    getDf(spark).schema

  def appendToTable(df: DataFrame): Unit =
    database.appendToTable(df, table)

}
