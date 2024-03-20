import org.apache.spark.sql.SparkSession

import scala.io.StdIn.readLine

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("WordCount").getOrCreate()
    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    println("This program uses Spark will show the count of words in a text file, and the 5 most common words.")
    println("Please enter path to file to perform a word count: ")
    var filePath = readLine()
    val text = sc.textFile(filePath)
    val words = text.flatMap(_.split("\\s"))
      .map(_.replaceAll("[,.!?:;]","")
        .trim
        .toLowerCase)
      .filter(_.nonEmpty)
      .map((_,1))
    val totalWords = words.count()
    println(s"Total number of words in ${filePath} is ${totalWords}")
    val wordCount = words.reduceByKey(_+_).sortBy(_._2,ascending=false).take(5)
    println(s"5 most common words: ${wordCount.mkString(", ")}")
    spark.stop()
  }

}
