package org.advworkslh.datasets

import org.apache.spark.sql.{DataFrame, SparkSession, functions => f}

trait WindowedTable extends BaseTable {

  val dependencies: Array[WindowedTable]

  def getLastWindowLoaded(spark: SparkSession): Int = {
    Logs.getDf(spark).
      filter(s"Table = '$fullTableName'").
      selectExpr("COALESCE(MAX(window), -1) as result").
      head().
      getAs[Int]("result")
  }

  def getWindowDf(spark: SparkSession, window: Int): DataFrame

  def postWindowLoad(window: Int): Unit = {}

  def loadWindow(spark: SparkSession, window: Int): Unit = {

    // check that the dataset has been loaded for (window - 1)
    val lastWindowLoaded = getLastWindowLoaded(spark)

    assert(
      lastWindowLoaded == window - 1,
      s"loadWindow for window $window of $fullTableName: window ${window-1} has not been loaded"
    )

    // check that the dependencies have been loaded for the current window
    for(dependency <- dependencies){
      assert(
        dependency.getLastWindowLoaded(spark) == window,
        s"loadWindow for window $window of $fullTableName: dependency ${dependency.fullTableName} has not been loaded"
      )
    }

    // check that the current window is empty
    assert(
      getDf(spark).filter(s"__window = $window").count() == 0,
      s"loadWindow for window $window of $fullTableName: target table already contains data for the window"
    )

    // get the window df
    val windowDf = getWindowDf(spark, window).
      withColumn("__window", f.lit(window))

    // load the window
    database.appendToTable(windowDf, table)

    // execute the post window load method
    postWindowLoad(window)

    println(s"Loaded window $window of table $fullTableName")

    // register the window load
    Logs.registerLog(spark, window, fullTableName)

  }


}
