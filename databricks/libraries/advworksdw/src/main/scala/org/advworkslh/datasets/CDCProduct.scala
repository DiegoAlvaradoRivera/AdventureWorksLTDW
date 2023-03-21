package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, AdvWorksOLTP, SQLServerDatabase}

object CDCProduct extends CDCTable {

  override val trackedDatabase: SQLServerDatabase = AdvWorksOLTP
  override val trackedTable: String = "SalesLT.Product"
  override val trackedColumns: Array[String] = Array(
      "ProductID", "Name", "ProductNumber", "Color", "StandardCost", "ListPrice", "Size", "Weight",
      "SellStartDate", "SellEndDate", "DiscontinuedDate", "ProductCategoryID", "ProductModelID"
    )

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Staging.CDCProduct"

  override val idColumns: Array[String] = Array("ProductID")

  override val dependencies: Array[WindowedTable] = Array()


}
