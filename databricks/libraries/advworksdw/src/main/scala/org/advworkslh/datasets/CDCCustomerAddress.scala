package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, AdvWorksOLTP, SQLServerDatabase}

object CDCCustomerAddress extends CDCTable {

  override val trackedDatabase: SQLServerDatabase = AdvWorksOLTP
  override val trackedTable: String = "SalesLT.CustomerAddress"
  override val trackedColumns: Array[String] = Array("CustomerID", "AddressID", "AddressType")

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Staging.CDCCustomerAddress"

  override val idColumns: Array[String] =
    Array("CustomerID", "AddressID")

  override val dependencies: Array[WindowedTable] = Array()

}
