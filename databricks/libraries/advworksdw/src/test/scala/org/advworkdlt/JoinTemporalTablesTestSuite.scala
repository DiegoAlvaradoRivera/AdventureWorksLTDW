package org.advworkdlt

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions => f}
import org.advworkslh.services.{AdvWorksOLTP, SparkCluster}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.advworkslh.utilities.Functions._
import org.advworkslh.utilities.Testing._
import java.sql.Timestamp
import scala.collection.JavaConverters._

class JoinTemporalTablesTestSuite extends AnyFunSuite with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def spark: SparkSession = SparkCluster.getSparkSession

  override def beforeAll(): Unit = super.beforeAll()

  val leftSchema: StructType = StructType(Array(
    StructField("pk"        , IntegerType  , nullable = false),
    StructField("fk"        , IntegerType  , nullable = true),
    StructField("left_value", IntegerType,   nullable = true),
    StructField("valid_from", TimestampType, nullable = true),
    StructField("valid_to"  , TimestampType, nullable = true),
  ))

  val rightSchema: StructType = StructType(Array(
    StructField("pk"         , IntegerType  , nullable = false),
    StructField("right_value", IntegerType,   nullable = true),
    StructField("valid_from" , TimestampType, nullable = true),
    StructField("valid_to"   , TimestampType, nullable = true),
  ))

  def dayOfJan2020(n: Int): Timestamp = {
    val nf = n.formatted("%02d")
    Timestamp.valueOf(s"2020-01-$nf 00:00:00")
  }

  test("Test 1") {

    val leftRows = List[Row](
      Row(1, 1, 101, dayOfJan2020(1), dayOfJan2020(3)),
      Row(1, 1, 102, dayOfJan2020(3), dayOfJan2020(5))
    ).asJava

    val rightRows = List[Row](
      Row(1, 11, dayOfJan2020(1), dayOfJan2020(3)),
      Row(1, 12, dayOfJan2020(3), dayOfJan2020(5))
    ).asJava

    val leftDf = spark.createDataFrame(leftRows, leftSchema)
    val rightDf = spark.createDataFrame(rightRows, rightSchema)

    assert(checkTemporalTableIntegrity(leftDf, Array("pk")))
    assert(checkTemporalTableIntegrity(rightDf, Array("pk")))

    val actualDf =
      joinTemporalTables(
        leftDf,
        rightDf,
        leftDf("fk") === rightDf("pk")
      ).
      drop(leftDf("fk")).
      drop(rightDf("pk"))

    checkTemporalTableIntegrity(actualDf, Array("pk"))
    assert(actualDf.count() == 2)

  }


  test("Test 2") {

    val leftRows = List[Row](
      Row(1, 1, 101, dayOfJan2020(1), dayOfJan2020(3))
    ).asJava

    val rightRows = List[Row](
      Row(1, 11, dayOfJan2020(1), dayOfJan2020(2)),
      Row(1, 12, dayOfJan2020(2), dayOfJan2020(3))
    ).asJava

    val leftDf = spark.createDataFrame(leftRows, leftSchema)
    val rightDf = spark.createDataFrame(rightRows, rightSchema)

    assert(checkTemporalTableIntegrity(leftDf, Array("pk")))
    assert(checkTemporalTableIntegrity(rightDf, Array("pk")))

    val actualDf =
      joinTemporalTables(
        leftDf,
        rightDf,
        leftDf("fk") === rightDf("pk")
      ).
        drop(leftDf("fk")).
        drop(rightDf("pk"))

    checkTemporalTableIntegrity(actualDf, Array("pk"))
    assert(actualDf.count() == 2)

  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

}
