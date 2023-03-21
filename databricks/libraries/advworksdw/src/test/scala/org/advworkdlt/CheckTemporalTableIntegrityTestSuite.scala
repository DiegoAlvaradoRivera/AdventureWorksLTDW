package org.advworkdlt

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => f}
import org.advworkslh.services.{AdvWorksOLTP, SparkCluster}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.advworkslh.utilities.Testing._
import scala.collection.JavaConverters._

import java.sql.Timestamp

class CheckTemporalTableIntegrityTestSuite extends AnyFunSuite with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def spark: SparkSession = SparkCluster.getSparkSession

  override def beforeAll(): Unit = super.beforeAll()

  val schema = StructType(Array(
    StructField("id"        , IntegerType  , nullable = false),
    StructField("valid_from", TimestampType, nullable = false),
    StructField("valid_to"  , TimestampType, nullable = false)
  ))

  test("One entity with contiguous intervals should return true") {

    val rows = List[Row](
      Row(1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-03 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-03 00:00:00"), Timestamp.valueOf("2020-01-04 00:00:00")),
    ).asJava

    val df = spark.createDataFrame(rows, schema)

    assert(checkTemporalTableIntegrity(df, Array("id")))
  }

  test("One entity with non contiguous intervals should return false") {

    val rows = List[Row](
      Row(1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-03 00:00:00"), Timestamp.valueOf("2020-01-04 00:00:00")),
    ).asJava

    val df = spark.createDataFrame(rows, schema)

    assert(!checkTemporalTableIntegrity(df, Array("id")))
  }

  test("One entity with duplicated intervals should return false") {

    val rows = List[Row](
      Row(1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-03 00:00:00")),
    ).asJava

    val df = spark.createDataFrame(rows, schema)

    assert(!checkTemporalTableIntegrity(df, Array("id")))
  }

  test("One entity with overlapping intervals should return false") {

    val rows = List[Row](
      Row(1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-03 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-04 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-04 00:00:00"), Timestamp.valueOf("2020-01-05 00:00:00")),
    ).asJava

    val df = spark.createDataFrame(rows, schema)

    assert(!checkTemporalTableIntegrity(df, Array("id")))
  }

  test("Multiple entities with contiguous intervals should return true") {

    val rows = List[Row](
      Row(1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-03 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-03 00:00:00"), Timestamp.valueOf("2020-01-04 00:00:00")),

      Row(2, Timestamp.valueOf("2020-01-05 00:00:00"), Timestamp.valueOf("2020-01-06 00:00:00")),
      Row(2, Timestamp.valueOf("2020-01-06 00:00:00"), Timestamp.valueOf("2020-01-07 00:00:00")),
    ).asJava

    val df = spark.createDataFrame(rows, schema)

    assert(checkTemporalTableIntegrity(df, Array("id")))
  }

  test("Multiple entities with non contiguous intervals should return false") {

    val rows = List[Row](
      Row(1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-03 00:00:00"), Timestamp.valueOf("2020-01-04 00:00:00")),

      Row(2, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(2, Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-03 00:00:00")),
    ).asJava

    val df = spark.createDataFrame(rows, schema)

    assert(!checkTemporalTableIntegrity(df, Array("id")))
  }

  test("Multiple entities with overlapping intervals should return false") {

    val rows = List[Row](
      Row(1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-03 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-04 00:00:00")),

      Row(2, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(2, Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-03 00:00:00")),
    ).asJava

    val df = spark.createDataFrame(rows, schema)

    assert(!checkTemporalTableIntegrity(df, Array("id")))
  }

  test("Multiple entities with duplicated intervals should return false") {

    val rows = List[Row](
      Row(1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(1, Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-03 00:00:00")),

      Row(2, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-01-02 00:00:00")),
      Row(2, Timestamp.valueOf("2020-01-02 00:00:00"), Timestamp.valueOf("2020-01-03 00:00:00")),
    ).asJava

    val df = spark.createDataFrame(rows, schema)

    assert(!checkTemporalTableIntegrity(df, Array("id")))
  }


  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

}
