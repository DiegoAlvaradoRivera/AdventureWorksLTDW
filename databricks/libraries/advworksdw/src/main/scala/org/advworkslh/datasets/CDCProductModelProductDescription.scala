package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, AdvWorksOLTP, SQLServerDatabase}

object CDCProductModelProductDescription extends CDCTable {

  override val trackedDatabase: SQLServerDatabase = AdvWorksOLTP
  override val trackedTable: String = "SalesLT.ProductModelProductDescription"
  override val trackedColumns: Array[String] = Array(
    "ProductModelID", "ProductDescriptionID", "Culture"
  )
  override val idColumns: Array[String] = Array(
    "ProductModelID", "ProductDescriptionID", "Culture"
  )

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Staging.CDCProductModelProductDescription"

  override val dependencies: Array[WindowedTable] = Array()

}
