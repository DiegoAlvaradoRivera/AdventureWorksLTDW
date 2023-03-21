package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, AdvWorksOLTP, SQLServerDatabase}

object CDCProductCategory extends CDCTable {

  override val trackedDatabase: SQLServerDatabase = AdvWorksOLTP
  override val trackedTable: String = "SalesLT.ProductCategory"
  override val trackedColumns: Array[String] = Array(
    "ProductCategoryID", "ParentProductCategoryID", "Name"
  )

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Staging.CDCProductCategory"

  override val idColumns: Array[String] = Array("ProductCategoryID")

  override val dependencies: Array[WindowedTable] = Array()

}
