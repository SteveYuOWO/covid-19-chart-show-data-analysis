package com.steveyu.covid

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CountryDataAnalysis {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","Root123..")
    properties.put("driver", "com.mysql.cj.jdbc.Driver")

    val spark = SparkSession.builder
      .appName("china province data")
      .master("local[2]")
      .getOrCreate()

    val countryDataScheme = StructType(Array(
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
      StructField("countryName", StringType, true),
      StructField("countryShortCode", StringType, true),
      StructField("continent", StringType, true),
      StructField("countryFullName", StringType, true)
    ))

    val cddf = spark
      .read
      .option("header", true)
      .schema(countryDataScheme)
      .csv("src/main/resources/countrydata.csv")

    cddf.createOrReplaceTempView("country")
    cddf
      .sqlContext
      .sql("select countryName, sum(confirmedCount) confirmedCount, " +
        "sum(confirmedIncr) confirmedIncr, " +
        "sum(curedCount) curedCount, " +
        "sum(curedIncr) curedIncr " +
        "from country group by countryName")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://120.78.206.78:3306/covid-analysis?serverTimezone=UTC&characterEncoding=UTF-8",
        "country_sum", properties)
  }
}
