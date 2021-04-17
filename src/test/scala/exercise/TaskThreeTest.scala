package exercise

import entity.{ResData, TaxiData}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.{Row, SparkSession}
import exercise.TaskThree.{getResult, getTaxiFactsDS}

import java.util.stream.Collectors
import scala.collection.mutable.ArrayBuffer

class TaskThreeTest extends SharedSparkSession {
  import testImplicits._

  test("check result") {
    val spark: SparkSession = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Test №1")
      .getOrCreate()

    import testImplicits._
    val taxiDF2 = getTaxiFactsDS("src/main/resources/data/yellow_taxi_jan_25_2018",spark)
      .as[TaxiData]

    val data = getResult(taxiDF2, spark)
      .toDF("district", "count", "avr", "deviation", "min_distance", "max_distance")

    checkAnswer(
      data,
      Row("Bronx",1589,9.052913782253006,5.417456008382843,0.0,31.18)  ::
        Row("Brooklyn",12672,6.88587436868688,4.77247255984397,0.0,44.8) ::
        Row("EWR",508,16.97218503937008,4.864870222043953,0.0,45.98) ::
        Row("Manhattan",296527,2.1818937229998903,2.6307171623375316,0.0,37.92) ::
        Row("Queens",13819,8.712971995079211,5.557884808639943,0.0,51.6) ::
        Row("Staten Island",64,19.485468749999995,7.644284290662419,0.0,33.78) ::
        Row("Unknown",6714,3.451726243669959,5.603374652105333,0.0,66.0) :: Nil
    )

  }


}
