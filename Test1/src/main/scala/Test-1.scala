

import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
// import org.apache.spark.sql.Row

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.sql.Row

object Test1 {

  val conf = new SparkConf().setAppName("Test1")
  conf.setMaster("local")

  val sc = new SparkContext(conf)

  val sqlContext = sqlContext(sc)

  def main(args: Array[String]){
    // val spark = SparkSession.builder.appName("Test1").getOrCreate()
    // val sc = SparkContext.getOrCreate()
    // import spark.implicits._


    val textFile = sc.textFile("/TradeData/country_partner_sitcproduct2digit_year.tab")

    val dataHeader = "location_id	partner_id	product_id	year	export_value	import_value	sitc_eci	sitc_coi	location_code	partner_code	sitc_product_code"

    val schema = StructType(dataHeader.split("\t").map(fieldName => StructField(fieldName, StringType, true)))

    val rows = textFile.map(_.split("\t")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10)))

    val dataFrame = sqlContext.createDataFrame(rows, schema)

    // val entries = textFile.flatMap(line => line.split("\t")).map(word => (word, 1)).reduceByKey(_+_)
    // val fortyOne = entries.groupBy(identity)
    // val count41 = entries.map(word => if(word == "41") (word, 1)).reduceByKey(_+_)

    entries.saveAsTextFile("/test1-1/")

  }


}

val schema = StructType(Array(StructField("location_id", IntegerType, true), StructField("partner_id", IntegerType, true),
StructField("product_id", IntegerType, true), StructField("year", IntegerType, true), StructField("export_value", IntegerType, true),
StructField("import_value", IntegerType, true), StructField("sitc_eci", IntegerType, true), StructField("sitc_coi", IntegerType, true),
StructField("location_code", StringType, true), StructField("partner_code", StringType, true), StructField("sitc_product_code", StringType, true)))


val trade = spark.read.option("header", "false").option("delimiter", "\t").schema(schema).csv("/TradeData/country_partner_sitcproduct2digit_year.tab")


import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import org.apache.spark.util.IntParam
import org.apache.spark.sql.functions._