package org.advworkslh.datasets

import org.advworkslh.services.SQLServerDatabase
import org.apache.spark.sql.{DataFrame, SparkSession, functions => f}
import org.advworkslh.utilities.Functions._
import org.apache.spark.sql.expressions.Window

import java.sql.Timestamp

trait CDCTable extends WindowedTable {

  val trackedDatabase: SQLServerDatabase
  val trackedTable: String
  val trackedColumns: Array[String]

  def getInitialDf(spark: SparkSession): DataFrame = {

    val selectList = trackedColumns.map("T." + _).mkString(", ")

    val query: String =
      s"""
         |select
         |    'I' as __operation,
         |    cast('2008-01-01' as datetime) as __tran_end_time,
         |    $selectList
         |from $trackedTable as T
         |""".stripMargin

    trackedDatabase.getQuery(spark, query)
  }

  def loadFirstWindow(spark: SparkSession): Unit = {

//    assert(
//      spark.table(fullTableName).count() == 0,
//      s"loadFirstWindow for $fullTableName: table is not empty"
//    )

    val df = getInitialDf(spark).
      withColumn("__window", f.lit(0))

    database.appendToTable(df, table)

    Logs.registerLog(spark, 0, fullTableName)
  }

  def getCDCEvents(spark: SparkSession, fromTm: Timestamp, toTm: Timestamp): DataFrame = {

    // (fromTm, toTm]

    assert(
      fromTm.toInstant.isBefore(toTm.toInstant),
      "fromTm is after toTm"
    )

    // check that the CDC capture process has proceed beyond toTm
    val (_, maxLsnTm) = trackedDatabase.getMaxLsnInfo(spark)

    // toTm <= maxLsnTm
    assert(
      !toTm.toInstant.isAfter(maxLsnTm.toInstant),
      s"CDC time range query for $table: The capture process has not progressed beyond the upper point of the interval"
    )

    val captureInstance = trackedTable.replace(".", "_")
    val selectList: String = trackedColumns.map("T." + _).mkString(", ")
    val rowFilter = trackedColumns.
      map(c => s"(sys.fn_cdc_has_column_changed('$captureInstance', '$c', __$$update_mask) = 1)").
      mkString(" or ")

    // get the lsn points for the query
    val fromLsn = trackedDatabase.getTimeLsnMapping(spark, fromTm, "smallest greater than")
    val toLsn = trackedDatabase.getTimeLsnMapping(spark, toTm, "largest less than or equal")

    val fromLsnLit = LsnNumberToString(fromLsn)
    val toLsnLit   = LsnNumberToString(toLsn)

    val query =
      s"""
         |SELECT
         |CASE __$$operation
         |    WHEN 1 THEN 'D'
         |    WHEN 2 THEN 'I'
         |    WHEN 4 THEN 'U'
         |END AS __operation,
         |TM.tran_end_time as __tran_end_time,
         |$selectList
         |FROM
         |cdc.fn_cdc_get_all_changes_$captureInstance(
         |  $fromLsnLit,
         |  $toLsnLit,
         |  'all'
         |) as T
         |LEFT JOIN  cdc.lsn_time_mapping AS TM
         |ON TM.start_lsn = T.__$$start_lsn
         |WHERE $rowFilter
         |""".stripMargin

    trackedDatabase.getQuery(spark, query)

  }

  def getWindowDf(spark: SparkSession, window: Int): DataFrame = {
    val (fromTm, toTm) = Windows.getWindowInfo(spark, window)
    getCDCEvents(spark, fromTm, toTm)
  }

  def getTemporalTable(spark: SparkSession): DataFrame = {

    val windowSpec = Window.
      partitionBy(idColumns.map(f.col):_*).
      orderBy(f.col("__tran_end_time"))

    database.getTable(spark, table).
      withColumn("valid_from", f.col("__tran_end_time")).
      withColumn(
        "valid_to",
        f.lead(f.col("__tran_end_time"), 1, Timestamp.valueOf("9999-12-31 00:00:00")).
          over(windowSpec)
      ).
      filter("__operation != 'D'").
      drop("__operation").
      drop("__tran_end_time").
      drop("__window")

  }

//  def getTemporalTable(spark: SparkSession, start: Timestamp, end: Timestamp): DataFrame = {
//
//    assert(start.toInstant.isBefore(end.toInstant))
//
//    getTemporalTable(spark).
//      filter(
//        intervalsIntersect(
//          f.col("valid_from"), f.col("valid_to"),
//          f.lit(start), f.lit(end)
//        )
//      ).
//      withColumn("valid_from", max(f.col("valid_from"), f.lit(start))).
//      withColumn("valid_to",   min(f.col("valid_to"),   f.lit(end)))
//
//  }

  def getTemporalTable(spark: SparkSession, start: Timestamp, end: Timestamp): DataFrame = {

    assert(start.toInstant.isBefore(end.toInstant))

    val idColumnsSeq = idColumns.mkString(", ")

    val query = s"""
      |SELECT
      |	*,
      |	IIF(__tran_end_time >= '$start', __tran_end_time, '$start') as valid_from,
      |	IIF(__next_tran_end_time <= '$end', __next_tran_end_time, '$end') as valid_to
      |FROM (
      |	SELECT *
      |	FROM (
      |		SELECT
      |		*,
      |		COALESCE(LEAD(__tran_end_time) OVER (PARTITION BY $idColumnsSeq ORDER BY __tran_end_time asc), '9999-12-31 00:00:00') AS __next_tran_end_time
      |		FROM $table
      |	) AS Q1
      |	WHERE __operation != 'D'
      |) AS Q2
      |WHERE staging.fn_IntervalsIntersect(__tran_end_time, __next_tran_end_time, '$start', '$end') = 1
      |""".stripMargin

    database.asInstanceOf[SQLServerDatabase].getQuery(spark, query).
      drop("__tran_end_time", "__next_tran_end_time", "__window", "__operation")

  }

  def getTemporalTable(spark: SparkSession, window: Int): DataFrame = {

    val (windowStart, windowEnd): (Timestamp, Timestamp) =
      Windows.getWindowInfo(spark, window)

    getTemporalTable(spark, windowStart, windowEnd)

  }

}
