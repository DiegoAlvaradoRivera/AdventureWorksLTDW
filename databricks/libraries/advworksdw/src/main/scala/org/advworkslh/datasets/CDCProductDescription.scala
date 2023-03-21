package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, AdvWorksOLTP, SQLServerDatabase}

object CDCProductDescription extends CDCTable {

  override val trackedDatabase: SQLServerDatabase = AdvWorksOLTP
  override val trackedTable: String = "SalesLT.ProductDescription"
  override val trackedColumns: Array[String] = Array(
    "ProductDescriptionID", "Description"
  )
  override val idColumns: Array[String] = Array("ProductDescriptionID")

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Staging.CDCProductDescription"

  override val dependencies: Array[WindowedTable] = Array()

}
