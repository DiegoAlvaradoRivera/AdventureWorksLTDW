package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, AdvWorksOLTP, SQLServerDatabase}

object CDCAddress extends CDCTable {

  override val trackedDatabase: SQLServerDatabase = AdvWorksOLTP
  override val trackedTable: String = "SalesLT.Address"
  override val trackedColumns: Array[String] = Array(
    "AddressID", "AddressLine1", "AddressLine2", "City", "StateProvince", "CountryRegion", "PostalCode"
  )

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Staging.CDCAddress"

  override val idColumns: Array[String] = Array("AddressID")

  override val dependencies: Array[WindowedTable] = Array()


}