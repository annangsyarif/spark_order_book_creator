package org.anang.assessment

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, col, concat, desc, lit, row_number, sum}
import org.apache.spark.sql.types.{DoubleType, StringType}

object DataTransformation {

  // function to process the dataframe
  def processData(accumulatedData: DataFrame): DataFrame = {

    // create window to get row number
    val window_rn = Window.partitionBy("order_id").orderBy(desc("created_at"))

    // group df, get the newest data, and drop the id with status closed
    val df_grouped = accumulatedData
      .withColumn("row_number", row_number().over(window_rn))
      .filter("row_number = 1")
      .filter("lower(status) = 'open'")
      .groupBy("price", "order_side", "symbol")
      .agg(
        sum("size").alias("amount")
      )

    // add total column
    val df_grouped_w_total = df_grouped
      .withColumn("total", col("price") * col("amount"))

    // split df to buy & sell
    val window_cumsum_buy = Window.partitionBy("order_side")
      .orderBy(desc("price"))
      .rowsBetween(Window.unboundedPreceding, 0)

    val df_buy = df_grouped_w_total
      .filter("lower(order_side) = 'buy'")
      .withColumn("rn", row_number().over(Window.partitionBy("order_side").orderBy(desc("price"))))
      .withColumn("side", concat(col("order_side"), lit('_'), col("rn")))
      .withColumn("cum_sum", sum("total").over(window_cumsum_buy))
      .drop("order_side")
      .drop("rn")
      .orderBy(desc("price"))

    val window_cumsum_sell = Window.partitionBy("order_side")
      .orderBy(asc("price"))
      .rowsBetween(Window.unboundedPreceding, 0)

    val df_sell = df_grouped_w_total
      .filter("lower(order_side) = 'sell'")
      .withColumn("rn", row_number().over(Window.partitionBy("order_side").orderBy(asc("price"))))
      .withColumn("side", concat(col("order_side"), lit('_'), col("rn")))
      .withColumn("cum_sum", sum("total").over(window_cumsum_sell))
      .drop("order_side")
      .drop("rn")
      .orderBy(asc("price"))

    // union the df and cast to specific type
    df_buy.union(df_sell)
      .withColumn("symbol", col("symbol").cast(StringType))
      .withColumn("side", col("side").cast(StringType))
      .withColumn("price", col("price").cast(DoubleType))
      .withColumn("amount", col("amount").cast(DoubleType))
      .withColumn("total", col("total").cast(DoubleType))
      .withColumn("cum_sum", col("cum_sum").cast(DoubleType))
  }


  // function to convert dataframe to json
  def dfToJson(df: DataFrame): String = {
    df.toJSON.collect().mkString("[", ",", "]")
  }
}
