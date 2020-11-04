package com.steveyu.covid.learning

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}



object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    // sc
    val conf = new SparkConf()
      .setAppName("china province data")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    // spark
    val spark = SparkSession.builder
      .appName("china province data")
      .master("local[2]")
      .getOrCreate()
    val chinaProvinceDataSchema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("confirmedCount", IntegerType, true),
      StructField("confirmedIncr", IntegerType, true),
      StructField("curedCount", IntegerType, true),
      StructField("curedIncr", IntegerType, true),
      StructField("currentConfirmedCount", IntegerType, true),
      StructField("currentConfirmedIncr", IntegerType, true),
      StructField("dateId", IntegerType, true),
      StructField("deadCount", IntegerType, true),
      StructField("deadIncr", IntegerType, true),
      StructField("suspectedCount", IntegerType, true),
      StructField("suspectedCountIncr", IntegerType, true),
      StructField("provinceName", StringType, true),
      StructField("provinceShortName", StringType, true)
    ))
    val chinaProvinceDataFrame = spark.read.option("header", true).schema(chinaProvinceDataSchema).csv("src/main/resources/china_provincedata.csv")
    //    chinaProvinceDataFrame.printSchema()
    //    chinaProvinceDataFrame.columns.foreach(println)
    //    print(chinaProvinceDataFrame.describe("confirmedCount").show)
    //    println(chinaProvinceDataFrame.select("id", "confirmedCount", "provinceName", "provinceShortName").filter("id < 5").show)
    // spark sql
    //    chinaProvinceDataFrame.createOrReplaceTempView("china_province")
    //    val sqlContext = chinaProvinceDataFrame.sqlContext
    //    sqlContext.sql("select provinceShortName, count(provinceShortName) from china_province group by provinceShortName").show()

    //    println(chinaProvinceDataFrame.count())


    //    "","","","","","","","","","","",""
    //    "","","","","","","","","","","","","","countryShortCode","",""

  }
}
