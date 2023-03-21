package org.advworkslh.utilities

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, functions => f}

object Functions {

  def getArgValue(args: Array[String], key: String): String = {
    val keyIndex = args.indexOf(key)
    assert(keyIndex != -1, s"key $key is not in args")
    args(keyIndex+1)
  }

  def prettyPrintSchema(df: DataFrame): Unit = {

    val schema = df.schema

    val maxColNameLength = schema.map(_.name.length+2).max
    val maxColTypeLength = schema.map(_.dataType.toString.length).max


    def addTrailingSpaces(string: String, n: Int): String =
      if (string.length <= n)
        string + " " * (n - string.length)
      else
        string


    def structFieldToString(structField: StructField): String = {
      val colName = addTrailingSpaces('"' + structField.name + '"', maxColNameLength)
      val colType = addTrailingSpaces(structField.dataType.toString, maxColTypeLength)

      if (structField.nullable) s"StructField($colName, $colType)"
      else s"StructField($colName, $colType, nullable = false)"
    }

    val head = "StructType(Array(\n"
    val body = schema.fields.map(structFieldToString).mkString(",\n")
    val foot = "\n))"

    println(head + body + foot)
  }

  def LsnNumberToString(lsnNUmber: Array[Byte]): String =
    "0x" + lsnNUmber.map("%02X".format(_)).mkString

  def intervalsIntersect(
                        start1: Column, end1: Column,
                        start2: Column, end2: Column
                        ): Column =
    (end1 > start2) && (end2 > start1)

  def min(col1: Column, col2: Column): Column =
    f.when(col1 <= col2, col1).otherwise(col2)

  def max(col1: Column, col2: Column): Column =
    f.when(col1 <= col2, col2).otherwise(col1)

  def joinTemporalTables(left: DataFrame, right: DataFrame, condition: Column): DataFrame = {

    left.
      join(
        right,
        condition,
        joinType = "left"
      ).
      withColumn("unmatched", right("valid_from").isNull && right("valid_to").isNull).
      filter(
        intervalsIntersect(left("valid_from"), left("valid_to"), right("valid_from"), right("valid_to")) ||
          f.col("unmatched")
      ).
      withColumn(
        "new_valid_from",
        f.when(f.col("unmatched"), left("valid_from")).
          otherwise(max(left("valid_from"), right("valid_from")))
      ).
      withColumn(
        "new_valid_to",
        f.when(f.col("unmatched"), left("valid_to")).
          otherwise(min(left("valid_to"),   right("valid_to")))
      ).
      drop(left("valid_from")).
      drop(left("valid_to")).
      drop(right("valid_from")).
      drop(right("valid_to")).
      drop("unmatched").
      withColumnRenamed("new_valid_from", "valid_from").
      withColumnRenamed("new_valid_to", "valid_to")

  }

  def joinTemporalTables(left: DataFrame, right: DataFrame, condition: Array[String]): DataFrame = {
    left.
      join(
        right,
        condition,
        joinType = "left"
      ).
      withColumn("unmatched", right("valid_from").isNull && right("valid_to").isNull).
      filter(
        intervalsIntersect(left("valid_from"), left("valid_to"), right("valid_from"), right("valid_to")) ||
          f.col("unmatched")
      ).
      withColumn(
        "new_valid_from",
        f.when(f.col("unmatched"), left("valid_from")).
          otherwise(max(left("valid_from"), right("valid_from")))
      ).
      withColumn(
        "new_valid_to",
        f.when(f.col("unmatched"), left("valid_to")).
          otherwise(min(left("valid_to"),   right("valid_to")))
      ).
      drop(left("valid_from")).
      drop(left("valid_to")).
      drop(right("valid_from")).
      drop(right("valid_to")).
      drop("unmatched").
      withColumnRenamed("new_valid_from", "valid_from").
      withColumnRenamed("new_valid_to", "valid_to")

  }

}