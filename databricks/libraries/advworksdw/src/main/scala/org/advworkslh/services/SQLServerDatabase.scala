package org.advworkslh.services

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{Connection, DriverManager, Timestamp}

trait SQLServerDatabase {

  val name: String
  val connStringSecret: String

  def getJDBCConnection: Connection = {
    val spark: SparkSession = SparkCluster.getSparkSession
    val url = spark.conf.get(connStringSecret)
    val con: Connection = DriverManager.getConnection(url)
    con
  }

  def getTable(spark: SparkSession, table: String): DataFrame = {

    val url = spark.conf.get(connStringSecret)

    spark.read.
      format("jdbc").
      option("url", url).
      option("dbtable", table).
      load()
  }

  def appendToTable(df: DataFrame, table: String): Unit = {

    val spark = df.sparkSession
    val url = spark.conf.get(connStringSecret)

    df.
      write.
      mode("append").
      format("jdbc").
      option("url", url).
      option("dbtable", table).
      save()
  }

  def getQuery(spark: SparkSession, query: String): DataFrame = {

    val url = spark.conf.get(connStringSecret)

    spark.read.
      format("jdbc").
      option("url", url).
      option("query", query).
      load()
  }

  def getMaxLsnInfo(spark: SparkSession): (Array[Byte], Timestamp) = {
    val query =
      s"""
         |SELECT
         |sys.fn_cdc_get_max_lsn() as maxLsnNb,
         |sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_max_lsn()) as maxLsnTm
         |""".stripMargin

    val result = getQuery(spark, query).head()
    val maxLsnNb = result.getAs[Array[Byte]]("maxLsnNb")
    val maxLsnNTm = result.getAs[Timestamp]("maxLsnTm")

    (maxLsnNb, maxLsnNTm)
  }

  def getCurrentTime(spark: SparkSession): Timestamp = {
    val query =
      s"""
         |SELECT
         |GETDATE() as result
         |""".stripMargin

    val result = getQuery(spark, query).head()
    val currentTm = result.getAs[Timestamp]("result")

    currentTm
  }

  def getTimeLsnMapping(spark: SparkSession, tm: Timestamp, relOp: String): Array[Byte] = {

    val relOpOpts = Array(
      "largest less than",
      "largest less than or equal",
      "smallest greater than",
      "smallest greater than or equal"
    )

    assert(relOpOpts.contains(relOp))

    val query =
      s"""
        |SELECT
        |sys.fn_cdc_map_time_to_lsn('$relOp', '$tm') as result
        |""".stripMargin

    val row = getQuery(spark, query).head()
    val result = row.getAs[Array[Byte]]("result")
    result
  }

}
