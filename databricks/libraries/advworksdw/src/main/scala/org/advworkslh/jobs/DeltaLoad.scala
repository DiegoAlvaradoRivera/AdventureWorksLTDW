package org.advworkslh.jobs

import org.advworkslh.datasets._
import org.advworkslh.services.{AdvWorksOLTP, SparkCluster}
import org.apache.spark.sql.SparkSession

object DeltaLoad {

  def main(args: Array[String]): Unit = {

    println("Delta load started")

    val spark: SparkSession = SparkCluster.getSparkSession

    // retrieve the last loaded window information
    val (lastWindowNb, _, lastWindowEnd) = Windows.getMostCurrentWindowInfo(spark)

    // retrieve the current LSN number and time
    val (_, currLsnTm) = AdvWorksOLTP.getMaxLsnInfo(spark)

    //
    assert(
      lastWindowEnd.toInstant.isBefore(currLsnTm.toInstant),
      "Error: currLsnTm has not advanced since last delta load"
    )

    // create a new window with the
    val newWindowNb = lastWindowNb + 1
    Windows.registerWindow(spark, newWindowNb, lastWindowEnd, currLsnTm)

    println(s"Created window $newWindowNb")

    // load the windowed tables
    val windowedTables: Array[WindowedTable] = Array(
      CDCProduct, CDCProductModel, CDCProductCategory, CDCProductDescription, CDCProductModelProductDescription,
      CDCCustomer, CDCAddress, CDCCustomerAddress,
      DimProduct, DimCustomer,
      FactSalesOrders
    )

    windowedTables.foreach(_.loadWindow(spark, newWindowNb))

    println("Delta load finished")

  }

}
