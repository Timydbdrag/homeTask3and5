package exercise

import entity.{ResData, TaxiData, TaxiData2, TaxiZones}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.util.Properties
import scala.collection.mutable.ArrayBuffer

object TaskThree {

  val pathParquet = "src/main/resources/data/yellow_taxi_jan_25_2018"
  val pathCSV = "src/main/resources/data/taxi_zones.csv"

  val url = "jdbc:postgresql://localhost:5432/docker"
  val table = "test_data_mart"
  val prop = new Properties()
  prop.put("user", "docker")
  prop.put("password", "docker")

  def main(args: Array[String]): Unit = {
    val spark = SessionBuilder.session()

    val taxiFacts = getTaxiFactsDS(pathParquet, spark)
    val result = getResult(taxiFacts,spark)

    printer("start write....")
    try {
      saver(result)
    } finally {
      result.show(result.count().toInt, truncate = false)
      spark.sparkContext.stop()
      spark.stop()
    }
    printer("Finish")

  }

  def getTaxiFactsDS(pathParquet:String, spark:SparkSession): Dataset[TaxiData] ={
    import spark.implicits._
    spark
      .read
      .option("header","true")
      .option("inferSchema", "true")
      .parquet(pathParquet)
      .as[TaxiData]
  }

  def readCSV(path: String)(implicit spark: SparkSession):Dataset[TaxiZones] = {
    import spark.implicits._
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path).as[TaxiZones]
  }


  def getResult(taxiFactsDS: Dataset[TaxiData], spark: SparkSession):Dataset[ResData] = {
    val taxiZonesDF2 = readCSV("src/main/resources/data/taxi_zones.csv")(spark)

    import spark.implicits._

    taxiFactsDS
      .join(taxiZonesDF2, taxiZonesDF2("LocationID") === taxiFactsDS("DOLocationID"))
      .as[TaxiData2]
      .groupByKey(el => el.Borough)
      .flatMapGroups((_, dataForeach) => {
        val distance = ArrayBuffer[Double]()
        val outList = ArrayBuffer[ResData]()
        var average: Double = 0
        var district: String = ""
        var min: Double = Double.MaxValue
        var max: Double = 0

        dataForeach.foreach(elem => {
          distance += elem.trip_distance
          average += elem.trip_distance
          district = elem.Borough
          if (elem.trip_distance < min) min = elem.trip_distance
          if (elem.trip_distance > max) max = elem.trip_distance
        })

        average = average / distance.length
        var sumDeviationSqr: Double = 0

        distance.foreach(elem => {
          sumDeviationSqr += Math.pow(average - elem, 2)
        })

        val deviation = Math.sqrt(sumDeviationSqr / (distance.length - 1)) // Sqrt( (Sum((Xi-Xср)^2))/(n-1) )

        outList += ResData(district, distance.length, average, deviation, min, max)
        outList
      })

  }

  def printer(msg:String): Unit ={
    println("="*20)
    println(msg)
    println("="*20)
  }

  def saver(res: Dataset[ResData]): Unit ={
    res
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, table, prop)
  }

}
