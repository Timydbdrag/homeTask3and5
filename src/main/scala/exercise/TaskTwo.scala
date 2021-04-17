package exercise


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import java.io.{File, PrintWriter}


object TaskTwo {

  val pathParquet = "src/main/resources/data/yellow_taxi_jan_25_2018"

  def main(args: Array[String]): Unit = {
    val spark = SessionBuilder.session()

    val taxiFactsRDD = getTaxiFacts(spark)
    val result = getResult(taxiFactsRDD)

    val file = new File("test.txt")
    val pwText = new PrintWriter(file)

    printer("start write....")
    try{
      writer(result, pwText)
    } finally {
      pwText.close()
      spark.sparkContext.stop()
      sys.ShutdownHookThread{spark.stop()}
    }
    printer("Finish")

  }

  def getTaxiFacts(spark: SparkSession): RDD[Row] = {
    spark
      .read
      .parquet(pathParquet)
      .rdd
  }

  def getTime(t: String) = {
    t.substring(11, 13)
  }

  def getResult(res: RDD[Row]) = {
    res
      .filter(el => el(1) != null)
      .groupBy(el => getTime(el(1).toString) + ":00")
      .mapValues(_.size)
      .sortBy(x => -x._2)
      .map(x => x._1 + " - " + x._2)
  }

  def writer(res: RDD[String], file: PrintWriter): Unit = {
    res.collect().foreach(el => {
      println(el)
      file.write(el + "\n")
    })
  }

  def printer(msg:String): Unit ={
    println("="*20)
    println(msg)
    println("="*20)
  }


}
