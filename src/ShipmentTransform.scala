package com.gautam.demo
import org.apache.spark.sql.{ SparkSession, SQLContext }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.{ Level, Logger }

object ShipmentTransform extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("JSON Flattening")
    .config("spark.sql.warehouse.dir", "c:\\tmp\\hive")
    .enableHiveSupport()
    .getOrCreate()
  import spark.implicits._
  val df = spark.read
    .format("json")
    .option("multiline", true)
    .load("shipment.json")

  df.printSchema()

  val custsuppDF = df.withColumn("CustomerName", $"customer.Name")
    .withColumn("CustomerCity", $"customer.City")
    .withColumn("CustomerState", $"customer.State")
    .withColumn("CustomerCountry", $"customer.Country")
    .withColumn("SupplierName", $"supplier.Name")
    .withColumn("SupplierCity", $"supplier.City")
    .withColumn("SupplierState", $"supplier.State")
    .withColumn("SupplierCountry", $"supplier.Country")
    .drop($"customer")
    .drop($"supplier")
  val booksDF = custsuppDF
    .withColumn("items", explode($"books"))

  val flattenDF = booksDF.withColumn("Title", $"items.title")
    .withColumn("Qty", $"items.qty")
    .drop($"items")
    .drop($"books")
  //.select(explode($"books")).toDF("books")
  //.select("books.title" , "books.qty")

  flattenDF.show(false)
  println(flattenDF.count())
}
