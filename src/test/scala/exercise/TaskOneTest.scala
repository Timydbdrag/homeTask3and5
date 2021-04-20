package exercise

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import TaskOne.{getRafiFacts, calculatePopularBorough, readDataFromCsv}

class TaskOneTest extends AnyFlatSpec{
  implicit val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test №1")
    .getOrCreate()

  it should "upload and write data" in {
    val taxiZones = readDataFromCsv(spark)
    val taxiFacts = getRafiFacts(spark)

    val actualDistribution = calculatePopularBorough(taxiFacts,taxiZones,spark)
      .collectAsList()
      .get(0)

    assert(actualDistribution.get(0) == "Manhattan")
    assert(actualDistribution.get(1) == 296527)
    assert(taxiFacts.count() > 0)
  }

  it should "upload csv to df" in {
    val taxiZones = readDataFromCsv(spark)
    assert(taxiZones.isInstanceOf[DataFrame])
    assert(taxiZones.count() > 0)
    assert(taxiZones.collectAsList().get(0).get(1) === "EWR")
  }

  it should "check on null" in {
    def tZone = readDataFromCsv(null)
    assertThrows[NullPointerException]{
      tZone != null
    }
  }
}
