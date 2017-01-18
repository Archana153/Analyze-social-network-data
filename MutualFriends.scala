package com.devinline.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import java.io._
object MutualFriends {
  def main(args: Array[String]) = {
   
  val conf = new SparkConf()
      .setAppName("MutualFriends")
      .setMaster("local")
  val sc = new SparkContext(conf)
  val writer = new PrintWriter(new File("MutualFriends_1_Output.txt"))
  val x=0
  val y=0
  val input = sc.textFile("networkdata.txt").cache();
  val length=input.count().toInt
  for(x<-0 to length){
    val a1=input.map(line=>line.split("\\t")).filter(line=>(x.toString()==line(0))).flatMap(li=>li(1).split(","))
    for(y<-x+1 to length){
      val a2 = input.map(line => line.split("\\t")).filter(line =>(y.toString()==line(0))).flatMap(li=>li(1).split(","))
      val output = a2.intersection(a1).filter(li =>li.length()>=1).collect()
      if(output.length>=1){
      val finalOp = (x +","+y+"\t"+output.mkString(",")+"\n")
      writer.write(finalOp)
      }
    }
  }
  writer.close()
  sc.stop;  
  }
  
}