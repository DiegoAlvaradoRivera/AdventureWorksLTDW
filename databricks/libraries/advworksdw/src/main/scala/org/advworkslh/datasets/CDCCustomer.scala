package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, AdvWorksOLTP, SQLServerDatabase}

object CDCCustomer extends CDCTable {

  override val trackedDatabase: SQLServerDatabase = AdvWorksOLTP
  override val trackedTable: String = "SalesLT.Customer"
  override val trackedColumns: Array[String] = Array(
    "CustomerID", "NameStyle", "Title", "FirstName", "MiddleName", "LastName", "Suffix", "CompanyName",
    "SalesPerson", "EmailAddress", "Phone"
  )

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Staging.CDCCustomer"

  override val idColumns: Array[String] = Array("CustomerID")

  override val dependencies: Array[WindowedTable] = Array()

}
