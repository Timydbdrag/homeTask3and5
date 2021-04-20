package exercise

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, desc_nulls_first}

object TaskOne {

  val pathParquet = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val pathCSV = "src/main/resources/data/taxi_zones.csv"

  def main(args: Array[String]): Unit = {

    val spark = SessionBuilder.session()

    val taxiZones = readDataFromCsv(spark)
    val taxiFacts = getRafiFacts(spark)
    val result = calculatePopularBorough(taxiFacts,taxiZones,spark)

    printLog("start write....")
    try{
      writer(result, "outInfo")
    } finally {
      result.show()

      spark.sparkContext.stop()
      sys.ShutdownHookThread{spark.stop()}
      spark.stop()
    }
    printLog("Finish")

  }

  def readDataFromCsv(spark:SparkSession):DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(pathCSV)
  }

  def getRafiFacts(spark:SparkSession):DataFrame = {
    spark.read
      .parquet(pathParquet)
  }

  def calculatePopularBorough(facts:DataFrame, zones:DataFrame, spark:SparkSession):DataFrame = {
    val columns = Seq("Borough", "count")
    import spark.implicits._

    val result = facts
      .join(broadcast(zones), $"DOLocationID" === $"LocationID", "left")
      .groupBy($"Borough")
      .count()
      .orderBy($"count".desc)
      .toDF(columns: _*)

    result
  }

  def printLog(msg:String): Unit ={
    println("="*20)
    println(msg)
    println("="*20)
  }

  def writer(result:DataFrame, path:String)={
    result
      .coalesce(1)
      .write
      .mode("append")
      .parquet(path)
  }


}
