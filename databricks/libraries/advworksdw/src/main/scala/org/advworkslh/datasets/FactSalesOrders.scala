package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, AdvWorksOLTP, SQLServerDatabase}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.{CallableStatement, Connection}

object FactSalesOrders extends WindowedTable {

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Sales.FactSalesOrders"
  override val idColumns: Array[String] = Array("SalesOrderID")

  override val dependencies: Array[WindowedTable] = Array()

  override def getWindowDf(spark: SparkSession, window: Int): DataFrame = {

    val (windowStart, windowEnd) = Windows.getWindowInfo(spark, window)

    val query =
      s"""
        |SELECT
        |
        |	-- PK
        |	SOD.SalesOrderDetailID,
        |
        |	-- FK
        |	SOH.SalesOrderID,
        |	SOH.CustomerID,
        |	SOD.ProductID,
        |	SOH.OrderDate,
        |	SOH.DueDate,
        |	SOH.ShipDate,
        |
        |	-- METRICS
        |	SOH.Status,
        |	SOH.OnlineOrderFlag,
        |	SOH.PurchaseOrderNumber,
        |	SOH.AccountNumber,
        |	SOH.ShipMethod,
        |	SOH.CreditCardApprovalCode,
        |	SOH.Comment,
        |	SOD.OrderQty,
        |	SOD.UnitPrice,
        |	SOD.UnitPriceDiscount,
        |	SOH.TaxAmt * (SOD.LineTotal / SOH.Subtotal) AS AllocatedTaxAmt, -- SOH.Subtotal is the sum of the line totals
        |	SOH.Freight * (SOD.LineTotal / SOH.Subtotal) AS AllocatedFreight,
        |
        |	-- SHIPPING INFORMATION
        |	A.AddressLine1 as ShippingAddressLine1,
        |	A.AddressLine2 as ShippingAddressLine2,
        |	A.City as ShippingCity,
        |	A.StateProvince as ShippingStateProvince,
        |	A.CountryRegion as ShippingCountryRegion,
        |	A.PostalCode as ShippingPostalCode
        |
        |FROM
        |SalesLT.SalesOrderDetail AS SOD
        |LEFT JOIN SalesLT.SalesOrderHeader AS SOH ON SOH.SalesOrderID = SOD.SalesOrderID
        |LEFT JOIN SalesLT.Address          AS A   ON SOH.ShipToAddressID = A.AddressID
        |WHERE SOH.Status = 5 AND SOH.DueDate BETWEEN '$windowStart' AND '$windowEnd'
        |""".stripMargin

    AdvWorksOLTP.getQuery(spark, query)
  }

  override def postWindowLoad(window: Int): Unit = {

    val conn: Connection = database.getJDBCConnection
    val cs: CallableStatement = conn.prepareCall("{call Staging.sp_FactSalesOrdersPostWindowLoad(?)}")
    cs.setInt(1, window)
    cs.execute()

  }

}
