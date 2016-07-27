package dataScience

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/*

 *
 *
 *

 */

object logApache {
  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val sqlContext = new SQLContext(sc)

    val logFile = sc.textFile("/user/maria_dev/etude_log/log apache.txt")

      /*
    val fields = [StructField("month",StringType(), True), StructField("day",StringType(), True)
    ,StructField("server_time",StringType(), True),StructField("http",StringType(), True)
    ,StructField("ip",StringType(), True)
    ,StructField("time",StringType(), True),StructField("path",StringType(), True)
    ,StructField("query_string",StringType(), True),StructField("status_code",StringType(), True)
    ,StructField("referer",StringType(), True),StructField("user_agent",StringType(), True)
    ,StructField("t2c",StringType(), True),StructField("dve_trk_id",StringType(), True),StructField("ptn",StringType(), True)
    ]
    schema = StructType(fields)
*/

    // sqlContext.cr


    println("test")




  }
}
