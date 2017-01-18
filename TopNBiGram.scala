package com.devinline.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.PairRDDFunctions
import java.io._
import scala.collection.mutable.ArrayBuffer
import java.util.Properties
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import scala.collection.JavaConversions._
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import scala.collection.JavaConversions._

//N is 2 here(declared in the program)
object TopNBiGram {
  def main(args: Array[String]) = {
   val conf = new SparkConf().setAppName("TopNBiGram").setMaster("local")
   val sc = new SparkContext(conf)
   def bigramsInString(s: String,stopWords: Set[String]): Array[((String, String), Int)] = { 

    s.split("""\.""")                        // split on .
     .map(_.split(" ")                       // split on space
           .filter(_.nonEmpty)               // remove empty string
           .map(_.replaceAll("""\W""", "")   // remove special chars
                 .toLowerCase)
           .filter(_.nonEmpty)
           .filter(!stopWords.contains(_))
           .sliding(2)                       // take continuous pairs
           .filter(_.size == 2)              // sliding can return partial
           .map{ case Array(a, b) => ((a, b), 1) })
     .flatMap(x => x)                         
}
     def plainTextToLemmas(text: String, stopWords: Set[String]): Seq[String] = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }

    val rdd = sc.parallelize(Array("Alice is testing spark application. Testing spark is fun"))
    val stopWords = sc.broadcast( Set("a","able","about","across","after","all","almost","also","am","among","an","and","any","are","as","at","be",
    "because","been","but","by","can","cannot","could","dear","did","do","does","either","else","ever","every","for","from","get","got","had",
    "has","have","he","her","hers","him","his","how","however","i","if","in","into","is","it","its","just","least","let","like","likely","may",
    "me","might","most","must","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","said","say",
    "says","she","should","since","so","some","than","that","the","their","them","then","there","these","they","this","tis","to","too","was","us",
    "wants","was","we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your"))
    val stopWords1 = Set("stopWord")
    val lemmatized = rdd.map(plainTextToLemmas(_, stopWords1))
    val op=lemmatized.collect().mkString
    val len=op.length
    val N=2
    val rdd1=sc.parallelize(Array(op.substring(12, len-1)))
  rdd1.map(bigramsInString(_,stopWords.value))
     .flatMap(x => x)             
     .countByKey                  // get result in driver memory as Map
     .foreach{ case ((x, y), z) => if(z>=N)println(s"${x} ${y}, ${z}") }
    }
}