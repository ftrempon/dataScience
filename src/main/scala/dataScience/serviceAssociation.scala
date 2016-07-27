package dataScience

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.fpm.FPGrowth

/* Test

 *
 *
 *

 */

object serviceAssociation {
  def main(args: Array[String]) = {

    // Parameters :
    // Boolean for printing values
    val debug           = true
    val printModel      = true
    val printResults    = true

    // Association rules
    val minSupport      = 0.001
    val minConfidence   = 0.001
    val partitions      = 10

    val sc = new SparkContext()
    val sqlContext: SQLContext = new HiveContext(sc)

    // csv file to subscriptions Dataframe
    val subscriptions = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("parserLib", "univocity")
      .load("/user/maria_dev/association/FR_abo.csv")
      .toDF()

    val servicesBySuscribers = subscriptions.groupBy("transaction_customerid").agg(org.apache.spark.sql.functions.collect_set("service_label"))

    servicesBySuscribers.map( p => p(1).toString() ).take(10).foreach(println)

    servicesBySuscribers.show(10)




    /*

    // subscriptions Dataframe to Subscriptions Temp Table
    subscriptions.registerTempTable("temp_subscriptions")

    // SQL statements can be run by using the sql methods provided by sqlContext
    val servicesBySuscribers = sqlContext.sql("SELECT SUBSTRING(CAST(COLLECT_SET(service_label) AS STRING), 2, LENGTH(CAST(COLLECT_SET(service_label) AS STRING)) - 2) AS services FROM temp_subscriptions GROUP BY transaction_customerid")

    // Export servicesBySuscribers result to csv file
    servicesBySuscribers.rdd.saveAsTextFile("/user/maria_dev/association/test.csv")

    // Algorithm FPGrowth for association rules
    val transactions = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("parserLib", "univocity")
      .load("/user/maria_dev/association/test.csv")
      .map( s => s.toString() ) // val data = sc.textFile("/user/maria_dev/association/test.csv")

    val transactionsRDD = transactions.map( t => t.trim.split(',') )
    val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(partitions)
    val model = fpg.run(transactionsRDD)

    val modelsList = model.freqItemsets
    val resultsList = model.generateAssociationRules(minConfidence)

    // Printing models list
    if (printModel) {
      println("Model list :")
      modelsList.collect().foreach {
        itemset => println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
      }
    }

    // Printing results list
    if (printResults) {
      println("Result list :")
      resultsList.collect().foreach {
        rule => println(rule.antecedent.mkString("[", ",", "]") + " => " + rule.consequent .mkString("[", ",", "]") + ", " + rule.confidence)
      }
    }

    */

  }
}
