package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, AdvWorksOLTP, SQLServerDatabase}

object CDCProductModel extends CDCTable {

  override val trackedDatabase: SQLServerDatabase = AdvWorksOLTP
  override val trackedTable: String = "SalesLT.ProductModel"
  override val trackedColumns: Array[String] = Array("ProductModelID", "Name")

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Staging.CDCProductModel"

  override val idColumns: Array[String] = Array("ProductModelID")

  override val dependencies: Array[WindowedTable] = Array()

}
