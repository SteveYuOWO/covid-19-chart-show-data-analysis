package com.steveyu.covid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object ChinaDataAnalysis {
  def main(args: Array[String]): Unit = {
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

    val cpdf = spark
      .read
      .option("header", true)
      .schema(chinaProvinceDataSchema)
      .csv("src/main/resources/china_provincedata.csv")
    val cpdfContext = cpdf
      .sqlContext
    cpdf
      .createOrReplaceTempView("tmp")
    cpdfContext
      .sql("select provinceName, sum(confirmedCount) from tmp group by provinceName").show()


  }
}
