package com.steveyu.covid.learning

import org.apache.spark.sql._

case class Employee(name: String, age: Long)

object SparkSQLDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("employee")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val caseClassDS = Seq(Employee("Andrew", 55), Employee("Steveyu", 18), Employee("Calor", 18)).toDS()
    caseClassDS.show()


    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect().foreach(println)


    val employeeDF = caseClassDS.toDF
    employeeDF.createOrReplaceTempView("employee")
    employeeDF.sqlContext.sql("select * from employee where age = 18").show()
  }
}
