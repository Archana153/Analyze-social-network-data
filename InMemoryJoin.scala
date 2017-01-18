package com.devinline.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import java.io._
object InMemoryJoin {
  def main(args: Array[String]) = {
   
    val conf = new SparkConf()
      .setAppName("InMemoryJoin")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val userA = readLine("Enter UserA : ")
    val userB = readLine("Enter UserB : ")
    val input = sc.textFile("networkdata.txt")
    val frnd = input.map(li=>li.split("\\t")).filter(l1 => (l1.size == 2)).filter(li=>(userB==li(0))).flatMap(li=>li(1).split(","))
    val frnd1 = input.map(li=>li.split("\\t")).filter(l1 => (l1.size == 2)).filter(li=>(userA==li(0))).flatMap(li=>li(1).split(","))
    val Details = frnd1.intersection(frnd).collect()
    
    val input2 = sc.textFile("userdata.txt")
    val Details2 = input2.map(li=>li.split(",")).filter(li=>Details.contains(li(0))).map(li=>(li(1)+":"+li(9)))
    val answer=userA+" "+userB+"\t["+Details2.collect.mkString(",")+"]"
    System.out.println(answer)
    sc.stop
  }
  
}