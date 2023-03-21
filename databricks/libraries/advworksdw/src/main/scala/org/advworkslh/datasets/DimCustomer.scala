package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, SQLServerDatabase}
import org.advworkslh.utilities.Functions.joinTemporalTables
import org.apache.spark.sql.{DataFrame, SparkSession}

object DimCustomer extends WindowedTable {

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Sales.BaseDimCustomer"

  override val idColumns: Array[String] = Array("CustomerID")

  override val dependencies: Array[WindowedTable] = Array(
    CDCAddress, CDCCustomerAddress, CDCCustomer
  )

  override def getWindowDf(spark: SparkSession, window: Int): DataFrame = {

    val customerRVT =
      CDCCustomer.
        getTemporalTable(spark, window)

    val customerAddressRVT =
      CDCCustomerAddress.
        getTemporalTable(spark, window).
        filter("AddressType = 'Main Office'").
        drop("AddressType")

    val addressRVT =
      CDCAddress.getTemporalTable(spark, window).
        withColumnRenamed("AddressLine1" , "MainOfficeAddressLine1").
        withColumnRenamed("AddressLine2" , "MainOfficeAddressLine2").
        withColumnRenamed("City"         , "MainOfficeCity").
        withColumnRenamed("StateProvince", "MainOfficeStateProvince").
        withColumnRenamed("CountryRegion", "MainOfficeCountryRegion").
        withColumnRenamed("PostalCode"   , "MainOfficePostalCode")

    val withCustomerAddress =
      joinTemporalTables(
        customerRVT,
        customerAddressRVT,
        Array("CustomerID")
      )

    val withAddress =
      joinTemporalTables(
        withCustomerAddress,
        addressRVT,
        Array("AddressID")
      ).drop("AddressID")

    withAddress
  }

}
