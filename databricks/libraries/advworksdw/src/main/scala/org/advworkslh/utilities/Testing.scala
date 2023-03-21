package org.advworkslh.utilities

import org.apache.spark.sql.{DataFrame, SparkSession, functions => f}
import org.apache.spark.sql.expressions.Window
import java.io.File
import scala.reflect.io.Directory
import sys.process._

object Testing {

  def restoreLocalTmpFolder(path: String): Unit = {

    assert(path.startsWith("D:/tmp"))
    // assert(path.endsWith("spark-warehouse") || path.endsWith("data-lake"))

    val dir = new Directory(new File(path))
    dir.deleteRecursively()
  }

  def checkTemporalTableIntegrity(df: DataFrame, idCols: Array[String]): Boolean = {

    val windowSpec =
      Window.
        partitionBy(idCols.head, idCols.tail:_*).
        orderBy("valid_from")

    val nextStart = f.lead("valid_from", 1, null).over(windowSpec)

    df.
      withColumn("next_valid_from", nextStart).
      // omit the last row
      filter(f.col("next_valid_from").isNotNull).
      filter("valid_to != next_valid_from").
      isEmpty
  }

}