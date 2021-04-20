package exercise

import exercise.TaskTwo.{calculatePopularBorough, getTaxiFacts}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec


class TaskTwoTest extends AnyFlatSpec{
  implicit val spark: SparkSession = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test №1")
    .getOrCreate()

  it should "test result" in {
    val taxiFacts = getTaxiFacts(spark)

    def actualDistribution = calculatePopularBorough(taxiFacts)

    assert(actualDistribution.isInstanceOf[RDD[String]])
    assert(actualDistribution.count() == 24)
    assert(actualDistribution.take(1).last.equals("19:00 - 22121"))
  }

  it should "upload parquet to rdd" in {
    val taxiFacts = getTaxiFacts(spark)
    assert(taxiFacts.isInstanceOf[RDD[Row]])
    assert(taxiFacts.count() > 0)
    val one = taxiFacts.take(1)
    assert(one.last.get(0).equals(2))
  }

  it should "check on null" in {
    def tZone = calculatePopularBorough(null)
    assertThrows[NullPointerException]{
      tZone != null
    }
  }
}
