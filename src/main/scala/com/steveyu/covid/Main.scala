package com.steveyu.covid

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf
    conf.setAppName("covid-19-analysis")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val fileRDD:RDD[String] = sc.textFile("src/main/resources/testdata.txt")
    val words:RDD[String] = fileRDD.flatMap((x:String)=>{ x.split(" ") })
    val pairword:RDD[(String,Int)] = words.map((x:String)=>{new Tuple2(x,1)})
    val res:RDD[(String,Int)] = pairword.reduceByKey((x:Int,y:Int)=>{x+y})
    res.foreach(println)
  }
}
