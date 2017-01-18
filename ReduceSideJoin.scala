package com.devinline.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import java.io._
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

object ReduceSideJoin {
  def main(args: Array[String]) = {
   val conf = new SparkConf().setAppName("ReduceSideJoin").setMaster("local")
   val sc = new SparkContext(conf)
   def age(dob:String):Int={
  val age = 2016-dob.split("/")(2).toInt
  return age
  }
   val input = sc.textFile("networkdata.txt")
   val userData = sc.textFile("userdata.txt")

  val frndList   = input.map(x=>x.split("\t")).filter(x=>x.size==2)
  val userAgeMap = userData.map(y=>y.split(",")).filter(y=>y.size==10).map(y=>(y(0),age(y(9))))
  val userFndMap = frndList.map(x=>(x(0),x(1).split(","))).flatMap(x=>x._2.map(z=>(z,x._1)))
  val userFrndDetails = userFndMap.join(userAgeMap).map(x=>(x._2._1,x._2._2))
  val userSort = userFrndDetails.reduceByKey((a,b)=>if(a>b) a else b)
  val outputFormat = userData.map(z=>z.split(",")).filter(z=>z.size==10).map(z=>(z(0),z(3)+","+z(4)+","+z(5)))
  val output = userSort.join(outputFormat).map(z=>(z._2._1,(z._2._2,z._1)))
  val result = output.sortByKey(false).map(z=>z._2._2+", "+z._2._1+","+z._1).take(10).foreach(println)
  }
}