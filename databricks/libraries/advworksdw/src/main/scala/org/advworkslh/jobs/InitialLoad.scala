package org.advworkslh.jobs

import org.advworkslh.datasets._
import org.advworkslh.services.{AdvWorksOLTP, SparkCluster}
import java.sql.Timestamp

object InitialLoad {

  def main(args: Array[String]): Unit = {

    println("Initial load started")

    val spark = SparkCluster.getSparkSession

    val w0Start: Timestamp = Timestamp.valueOf("2008-01-01 00:00:00")
    val w0End: Timestamp = AdvWorksOLTP.getCurrentTime(spark)

    Windows.registerWindow(spark, 0, w0Start, w0End)

    Array[CDCTable](
        CDCProduct, CDCProductModel, CDCProductCategory, CDCProductDescription, CDCProductModelProductDescription,
        CDCCustomer, CDCCustomerAddress, CDCAddress
      ).
      foreach(_.loadFirstWindow(spark))

    Array[WindowedTable](
        DimProduct,
        DimCustomer,
        FactSalesOrders
      ).
      foreach(_.loadWindow(spark, 0))

    println("Initial load finished")

  }

}
