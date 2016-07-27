package dataScience

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/*

 *
 *
 *

 */

object arpu {
  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val sqlContext: SQLContext = new HiveContext(sc)

    val customSchema = StructType(Array(
      StructField("country", StringType, true),
      StructField("support", StringType, true),
      StructField("service", StringType, true),
      StructField("operator_brand", StringType, true),
      StructField("operator_name", StringType, true),
      StructField("offermedia", StringType, true),
      StructField("offer", StringType, true),
      StructField("frequency", IntegerType, true),
      StructField("customerid", StringType, true),
      StructField("customerid_type", StringType, true),
      StructField("customerid_ipaddress", StringType, true),
      StructField("ext_code", StringType, true),
      StructField("t2c", StringType, true),
      StructField("subscription_id", StringType, true),
      StructField("subscription_datesubscribed", StringType, true),
      StructField("cohort", StringType, true),
      StructField("payment_date", StringType, true),
      StructField("delay", IntegerType, true),
      StructField("amount", DoubleType, true)
    ))

    // csv file to transactions Dataframe
    val transactions = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("encapsulation", """""")
      .schema(customSchema)
      .load("/user/maria_dev/arpu")
      .toDF()

    // transactions.printSchema()
    // transactions.show(20)

    // transactions.select("country", "cohort", "service", "operator_name", "offermedia", "delay", "amount").show()

    val arpu_by_country = transactions
      .groupBy("country", "cohort")
      .agg(
        org.apache.spark.sql.functions.countDistinct("subscription_id") as "population",
        sum( when(col("delay") < 1, col("amount")).otherwise(0) ) as "amount_delay0",
        sum( when(col("delay") < 2, col("amount")).otherwise(0) ) as "amount_delay1",
        sum( when(col("delay") < 3, col("amount")).otherwise(0) ) as "amount_delay2",
        sum( when(col("delay") < 4, col("amount")).otherwise(0) ) as "amount_delay3",
        sum( when(col("delay") < 5, col("amount")).otherwise(0) ) as "amount_delay4",
        sum( when(col("delay") < 6, col("amount")).otherwise(0) ) as "amount_delay5",
        sum("amount") as "total_amount"
      )

    // arpu_by_country.write.mode("overwrite").saveAsTable("default.arpu_by_country")

    /*


    */

    // arpus.show(20)
  }
}
