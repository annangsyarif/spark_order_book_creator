package org.anang.assessment

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.scalatest.funsuite.AnyFunSuite

import scala.io.Source

class DataTransformationTest extends AnyFunSuite {
  test("DataTransformation dfToJson should be correct"){
    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("test")
      .config("spark.driver.bindAddress", "localhost")
      .getOrCreate()

    val df = spark
      .read
      .option("header", true)
      .csv(getClass.getResource("/json_test.csv").getPath)
      .withColumn("col_a", col("col_a").cast(IntegerType))
      .withColumn("col_b", col("col_b").cast(StringType))

    val jsonString = DataTransformation.dfToJson(df)

    assert(
      jsonString == """[{"col_a":1,"col_b":"yes"},{"col_a":2,"col_b":"no"}]"""
    )

    spark.stop()
  }

  test("DataTransformation processData should be correct") {
    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("test")
      .config("spark.driver.bindAddress", "localhost")
      .getOrCreate()

    // create input dataframe
    val inputDF = spark
      .read
      .option("header", true)
      .csv(getClass.getResource("/input_data.csv").getPath)

    // call data transformation
    val resultDF = DataTransformation.processData(inputDF)
    val jsonResultDF = DataTransformation.dfToJson(resultDF)

    // expected data
    val expectedDF = spark
      .read
      .option("header", true)
      .csv(getClass.getResource("/expected_data.csv").getPath)
      .withColumn("symbol", col("symbol").cast(StringType))
      .withColumn("side", col("side").cast(StringType))
      .withColumn("price", col("price").cast(DoubleType))
      .withColumn("amount", col("amount").cast(DoubleType))
      .withColumn("total", col("total").cast(DoubleType))
      .withColumn("cum_sum", col("cum_sum").cast(DoubleType))

    val jsonExpectedDF = DataTransformation.dfToJson(expectedDF)

    // comparing
    assert(jsonResultDF == jsonExpectedDF)

    spark.stop()
  }
}
