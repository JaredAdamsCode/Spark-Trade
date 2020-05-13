// WordCount.scala
// Testing how to use Scala

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row



object WordCount {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("WordCount").getOrCreate()
    val sc = SparkContext.getOrCreate()
    import spark.implicits._

    val textFile = sc.textFile("/TradeData/country_partner_sitcproduct2digit_year.tab")
    val counts = textFile.flatMap(line => line.split("\t")).map(word => (word, 1)).reduceByKey(_+_)
    
    counts.saveAsTextFile("/scalaWC-out7/")

  }

}