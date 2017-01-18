package com.devinline.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import java.io._
object MutualFriendsUser {
  def main(args: Array[String]) = {
   
    val conf = new SparkConf()
      .setAppName("MutualFriendsUser")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val A = readLine("Enter first user: ")
    val B = readLine("Enter second user: ")
    val inputFile = sc.textFile("networkdata.txt")
    val friendListA = inputFile.map(line=>line.split("\\t")).filter(line1=>line1(0)==A).filter(line=>(line.size==2)).flatMap(line=>line(1).split(","))
    val friendListB = inputFile.map(line=>line.split("\\t")).filter(line1=>line1(0)==B).filter(line=>(line.size==2)).flatMap(line=>line(1).split(","))
    val mutualFriends = friendListA.intersection(friendListB).collect()
    val result = A +"," + B+"\t"+mutualFriends.mkString(",")
    System.out.println(result)
    sc.stop
  }
  
}