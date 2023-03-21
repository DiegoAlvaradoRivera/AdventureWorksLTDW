package org.advworkslh.datasets

import org.advworkslh.services.{AdvWorksOLAP, SQLServerDatabase}
import org.advworkslh.utilities.Functions.joinTemporalTables
import org.apache.spark.sql.{DataFrame, SparkSession}

object DimProduct extends WindowedTable {

  override val database: SQLServerDatabase = AdvWorksOLAP
  override val table: String = "Sales.BaseDimProduct"
  override val idColumns: Array[String] = Array("ProductID")

  override val dependencies: Array[WindowedTable] = Array(
    CDCProduct, CDCProductCategory,CDCProductDescription, CDCProductModelProductDescription
  )

  override def getWindowDf(spark: SparkSession, window: Int): DataFrame = {

    val productRVT: DataFrame =
      CDCProduct.getTemporalTable(spark, window)

    val prodSubcRVT: DataFrame =
      CDCProductCategory.
        getTemporalTable(spark, window).
        filter("ParentProductCategoryId IS NOT NULL").
        withColumnRenamed("Name", "SubcategoryName")

    val prodCatRVT =
      CDCProductCategory.
        getTemporalTable(spark, window).
        filter("ParentProductCategoryId IS NULL").
        withColumnRenamed("Name", "CategoryName").
        drop("ParentProductCategoryId")

    val prodModelRVT: DataFrame =
      CDCProductModel.
        getTemporalTable(spark, window).
        withColumnRenamed("name", "ModelName")

    val prodModelDescrRVT =
      joinTemporalTables(
          CDCProductModelProductDescription.getTemporalTable(spark, window).filter("Culture LIKE 'en'"),
          CDCProductDescription.getTemporalTable(spark, window),
          Array("ProductDescriptionID")
        ).
        drop("Culture", "ProductDescriptionID").
        withColumnRenamed("Description", "EnglishProductDescription")

    val prodModelWithPrdDesc =
      joinTemporalTables(
          prodModelRVT,
          prodModelDescrRVT,
          Array("ProductModelID")
        )

    val withProductModel =
      joinTemporalTables(
        productRVT,
        prodModelWithPrdDesc, Array("ProductModelID")
      ).drop("ProductModelID")

    val withProdSubc =
      joinTemporalTables(
        withProductModel,
        prodSubcRVT,
        Array("ProductCategoryID")
      ).drop("ProductCategoryID")

    val withProdCat =
      joinTemporalTables(
        withProdSubc,
        prodCatRVT,
        withProdSubc("ParentProductCategoryID") === prodCatRVT("ProductCategoryID")
      ).
        drop(withProdSubc("ParentProductCategoryID")).
        drop(prodCatRVT("ProductCategoryID"))

    withProdCat
  }

}
