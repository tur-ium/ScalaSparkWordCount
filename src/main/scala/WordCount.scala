import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("WordCount").getOrCreate()
    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    val text = sc.textFile("data/gutenberg_pg73186.txt")
    val words = text.flatMap(_.split("\\s"))
      .map(_.replaceAll("[,.!?:;]","")
        .trim
        .toLowerCase)
      .filter(_.nonEmpty)
      .map((_,1))
    val wordCount = words.reduceByKey(_+_).sortBy(_._2,ascending=false).take(5)
    println(s"The counts of 5 most common words is ${wordCount.mkString(", ")}")
    spark.stop()
  }

}
